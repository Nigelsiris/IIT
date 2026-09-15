/**
 * Integration tests for processInvoiceFile: what actually reaches the log
 * sheet, and what gets held back for a human instead.
 * Run: node test/run-pipeline-tests.js
 */
const fs = require('fs');
const path = require('path');
const { buildSandbox } = require('./harness');

const S = buildSandbox();

let passed = 0;
const failures = [];

function check(name, actual, expected) {
  const a = JSON.stringify(actual);
  const e = JSON.stringify(expected);
  if (a === e) { passed++; return; }
  failures.push(`${name}\n    expected: ${e}\n    actual:   ${a}`);
}

function group(title, fn) {
  console.log('\n· ' + title);
  fn();
}

/**
 * Stand up the whole Apps Script side of processInvoiceFile: OCR returns the
 * supplied text, the log sheet collects rows in memory, review items and
 * processing state live in a plain object.
 */
function mountPipeline(ocrText, options) {
  const opts = options || {};
  const store = {};
  const appendedRows = [];
  const emails = [];

  S.PropertiesService = {
    getScriptProperties: () => ({
      getProperty: key => (key in store ? store[key] : null),
      setProperty: (key, value) => { store[key] = value; },
      deleteProperty: key => { delete store[key]; },
      getProperties: () => Object.assign({}, store)
    })
  };
  S.LockService = { getScriptLock: () => ({ waitLock() {}, releaseLock() {} }) };
  S.Drive = {
    Files: {
      create: () => ({ id: 'temp-doc-id', mimeType: 'application/vnd.google-apps.spreadsheet' }),
      get: () => ({ mimeType: 'application/vnd.google-apps.spreadsheet' })
    }
  };
  S.DriveApp = { getFileById: () => ({ setTrashed() {} }) };
  S.DocumentApp = { openById: () => ({ getBody: () => ({ getText: () => ocrText }) }) };
  S.MailApp = { sendEmail: message => emails.push(message) };

  const sheet = {
    getMaxColumns: () => 26,
    insertColumnsAfter() {},
    getRange: () => ({ getValues: () => [S.LOG_HEADERS.slice()], setValues() {} }),
    appendRow: row => appendedRows.push(row)
  };

  // Enough SpreadsheetApp surface for the sheets-based merge to run: it builds a
  // master spreadsheet, copies the converted original into it and exports once.
  const mergeRange = {
    setValues() { return mergeRange; },
    merge() { return mergeRange; },
    setBackground() { return mergeRange; },
    setFontColor() { return mergeRange; },
    setFontSize() { return mergeRange; },
    setFontWeight() { return mergeRange; },
    setHorizontalAlignment() { return mergeRange; },
    setWrap() { return mergeRange; },
    setBorder() { return mergeRange; },
    getValues: () => [S.LOG_HEADERS.slice()]
  };
  const mergeSheet = {
    setName() {},
    getName: () => 'Coded Summary',
    getRange: () => mergeRange,
    setColumnWidth() {},
    copyTo() {}
  };
  const mergeSpreadsheet = {
    getActiveSheet: () => mergeSheet,
    getSheets: () => [mergeSheet],
    getSheetByName: name => (name === opts.logSheetName ? sheet : null),
    deleteSheet() {}
  };
  S.SpreadsheetApp = {
    BorderStyle: { SOLID: 'SOLID' },
    flush() {},
    openById: id => (id === 'sheet-id'
      ? { getSheetByName: () => sheet, insertSheet: () => sheet }
      : mergeSpreadsheet)
  };
  S.ScriptApp = { getOAuthToken: () => 'token' };
  S.UrlFetchApp = {
    fetch: () => ({
      getResponseCode: () => (opts.breakMerge ? 500 : 200),
      getBlob: () => S.Utilities.newBlob('merged-pdf-bytes', 'application/pdf', 'merged.pdf')
    })
  };
  S.Session = { getEffectiveUser: () => ({ getEmail: () => 'owner@lidl.us' }), getScriptTimeZone: () => 'UTC' };

  const sentMail = [];
  S.GmailApp = {
    sendEmail: (to, subject, body, options) => sentMail.push({ to, subject, body, options }),
    getAliases: () => (opts.aliases || [])
  };
  S.clearSendAsAliasCache();

  // getInvoiceSheet memoizes on SHEET_ID::SHEET_NAME and that cache lives in
  // code.js's own scope, so give every mount a distinct name to force a fresh
  // lookup against this mount's SpreadsheetApp stub.
  const config = {
    SHEET_ID: 'sheet-id',
    SHEET_NAME: 'Invoice Logger ' + (opts.key || Math.random()),
    TARGET_EMAIL: 'ap@example.com',
    PROCESSED_FOLDER_ID: 'processed-id',
    SOURCE_FOLDERS: ['source-id'],
    SEND_AS_ALIAS: opts.sendAsAlias === undefined ? 'logistics.invoices@lidl.us' : opts.sendAsAlias,
    SEND_AS_NAME: 'Inbound Invoicing',
    // getConfig() stores this as a Set of lowercased names.
    CONFIRMED_CARRIERS: S.parseConfirmedCarriers((opts.confirmedCarriers || []).join('\n'))
  };
  opts.logSheetName = config.SHEET_NAME;

  const blob = S.Utilities.newBlob('pdf', 'application/pdf', opts.fileName || 'invoice.pdf');
  const result = S.processInvoiceFile(
    blob,
    opts.fileName || 'invoice.pdf',
    'Drive Folder: Inbound',
    'drive|test|' + (opts.key || Math.random()),
    config,
    'application/pdf',
    { sendEmail: opts.sendEmail !== false, fileId: 'drive-file-' + (opts.key || 'x') }
  );

  const reviewItems = Object.keys(store)
    .filter(key => key.indexOf(S.REVIEW_PREFIX) === 0)
    .map(key => JSON.parse(store[key]));

  return { result, appendedRows, emails: sentMail, reviewItems, store, config };
}

const fixture = name => fs.readFileSync(path.join(__dirname, 'fixtures', name), 'utf8');

/* ─── a clean invoice from a confirmed carrier goes straight through ──────── */
group('confirmed carrier, confident amount', () => {
  const run = mountPipeline(fixture('arrive-INV6737123.txt'), {
    fileName: 'invoice-INV6737123.pdf',
    confirmedCarriers: ['Arrive Logistics'],
    aliases: ['logistics.invoices@lidl.us'],
    key: 'clean'
  });

  check('processed', run.result.status, 'processed');
  check('carrier', run.result.carrierType, 'Arrive Logistics');
  check('one log row', run.appendedRows.length, 1);
  check('nothing held', run.reviewItems.length, 0);

  const row = run.appendedRows[0];
  check('row width', row.length, S.LOG_HEADERS.length);
  check('invoice number logged', row[3], 'INV6737123');
  check('po logged', row[4], '118819032601');
  check('ship date logged', row[5], '3/18/2026');
  check('delivery date logged', row[6], '3/19/2026');
  // The Amount column must be a real number so SUM works over the sheet.
  check('amount logged as number', row[7], 1150);
  check('coding logged', row[12], '360100, 50001, CAT 1');

  check('one email sent', run.emails.length, 1);
  const mail = run.emails[0];
  // Sent AS the delegated mailbox, TO the AP address — not owner-to-owner.
  check('sent to AP', mail.to, 'ap@example.com');
  check('sent as the delegated mailbox', mail.options.from, 'logistics.invoices@lidl.us');
  // One merged document, not a code sheet plus a loose original.
  check('single merged attachment', mail.options.attachments.length, 1);
  check('merged attachment name', mail.options.attachments[0].getName(), 'Coded_invoice-INV6737123.pdf');
  check('subject not flagged', mail.subject, 'Processed Invoice: invoice-INV6737123.pdf');

  // The pair still goes to Drive so the carrier merge job can find it.
  check('pair written to Drive', run.result.outputBlobs.length, 2);
});

/* ─── a shaky amount still ships, as long as the coding resolved ──────────── */
group('low-confidence amount still sends', () => {
  const vague = [
    'Speedy Freight LLC',
    '100 Depot Road',
    'Perryville, Maryland 21903',
    'Invoice # SF-88213',
    'Ship Date 3/18/2026',
    'Delivery Date 3/19/2026',
    'Thanks for your business',
    '$1,234.56'
  ].join('\n');

  const run = mountPipeline(vague, {
    fileName: 'SF-88213.pdf',
    confirmedCarriers: ['Speedy Freight LLC'],
    aliases: ['logistics.invoices@lidl.us'],
    key: 'vague'
  });

  // Perryville resolves to RDC 70001, so the coding is good and it ships.
  check('processed, not held', run.result.status, 'processed');
  check('logged', run.appendedRows.length, 1);
  check('coding resolved', run.appendedRows[0][12], '360100, 70001, CAT 4');
  check('amount still logged', run.appendedRows[0][7], 1234.56);
  check('nothing queued', run.reviewItems.length, 0);

  // ...but the uncertainty is impossible to miss.
  check('email sent', run.emails.length, 1);
  check('subject flags the amount', run.emails[0].subject.indexOf('[CHECK AMOUNT]'), 0);
  check('body explains', run.emails[0].body.indexOf('could not be tied to an') > -1, true);
});

/* ─── no RDC means a person has to decide ─────────────────────────────────── */
group('unresolved coding is held', () => {
  const noRdc = [
    'Speedy Freight LLC',
    'Invoice # SF-99001',
    'Ship Date 3/18/2026',
    'Amount Due $2,000.00'
  ].join('\n');

  const run = mountPipeline(noRdc, {
    fileName: 'SF-99001.pdf',
    confirmedCarriers: ['Speedy Freight LLC'],
    key: 'nordc'
  });

  check('held for review', run.result.status, 'held_for_review');
  check('reason is the coding', run.result.heldReason, 'unresolved_coding');
  check('nothing written to the sheet', run.appendedRows.length, 0);
  check('no email sent', run.emails.length, 0);
  check('queued once', run.reviewItems.length, 1);
  // The held item must remember the Drive file, or approving it later loses
  // the original and emails a lone code sheet.
  check('source file remembered', run.reviewItems[0].fileId, 'drive-file-nordc');
});

/* ─── the merge degrades gracefully ───────────────────────────────────────── */
group('merge failure falls back to both files', () => {
  const run = mountPipeline(fixture('arrive-INV6744248.txt'), {
    fileName: 'invoice-INV6744248.pdf',
    confirmedCarriers: ['Arrive Logistics'],
    aliases: ['logistics.invoices@lidl.us'],
    key: 'mergefail',
    breakMerge: true
  });

  check('still processed', run.result.status, 'processed');
  check('email still sent', run.emails.length, 1);
  // Two attachments rather than nothing at all.
  check('fell back to the pair', run.emails[0].options.attachments.length, 2);
});

/* ─── an unusable alias is reported, not silently ignored ─────────────────── */
group('send-as alias validation', () => {
  const run = mountPipeline(fixture('arrive-INV6737123.txt'), {
    fileName: 'invoice-INV6737123.pdf',
    confirmedCarriers: ['Arrive Logistics'],
    aliases: [],
    key: 'noalias'
  });

  check('still sent', run.emails.length, 1);
  // No verified alias: Gmail would ignore `from`, so it is left off entirely.
  check('from omitted', run.emails[0].options.from, undefined);

  const warned = S.getProcessingFeed(50).some(function(entry) {
    return entry.type === 'warning' && entry.message.indexOf('Sending as the account owner') >= 0;
  });
  check('warned about the alias', warned, true);
});

/* ─── an unconfirmed carrier is held before anything else ─────────────────── */
group('unconfirmed carrier is held', () => {
  const run = mountPipeline(fixture('arrive-INV6744248.txt'), {
    fileName: 'invoice-INV6744248.pdf',
    confirmedCarriers: ['Some Other Carrier'],
    key: 'unconfirmed'
  });

  check('held for review', run.result.status, 'held_for_review');
  check('reason is the carrier', run.result.heldReason, 'unconfirmed_carrier');
  check('nothing written to the sheet', run.appendedRows.length, 0);
});

/* ─── the same file twice is processed once ───────────────────────────────── */
group('idempotency', () => {
  const text = fixture('arrive-INV6737123.txt');
  const first = mountPipeline(text, {
    fileName: 'invoice-INV6737123.pdf',
    confirmedCarriers: ['Arrive Logistics'],
    key: 'dupe'
  });
  check('first run processes', first.result.status, 'processed');

  // Re-run against the state the first run left behind.
  const store = first.store;
  S.PropertiesService = {
    getScriptProperties: () => ({
      getProperty: key => (key in store ? store[key] : null),
      setProperty: (key, value) => { store[key] = value; },
      deleteProperty: key => { delete store[key]; },
      getProperties: () => Object.assign({}, store)
    })
  };
  const second = S.processInvoiceFile(
    S.Utilities.newBlob('pdf', 'application/pdf', 'invoice-INV6737123.pdf'),
    'invoice-INV6737123.pdf',
    'Drive Folder: Inbound',
    'drive|test|dupe',
    first.config,
    'application/pdf',
    { sendEmail: false }
  );
  check('second run is a no-op', second.status, 'already_processed');
  check('no extra log row', first.appendedRows.length, 1);
});

/* ─── report ──────────────────────────────────────────────────────────────── */
console.log('\n' + '─'.repeat(60));
if (failures.length === 0) {
  console.log(`✓ all ${passed} assertions passed`);
  process.exit(0);
}
console.log(`✗ ${failures.length} failed, ${passed} passed\n`);
failures.forEach(f => console.log('  ✗ ' + f));
process.exit(1);
