/**
 * Spreadsheet-invoice and archive tests.
 * Run: node test/run-sheet-tests.js
 */
const fs = require('fs');
const path = require('path');
const { buildSandbox } = require('./harness');
const { makeSpreadsheet, makeGridWorkbook } = require('./fake-spreadsheet');

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

/** Run the full in-workbook pipeline without touching Drive. */
function parseWorkbook(workbook, fileName) {
  const spreadsheet = makeSpreadsheet(workbook);
  const meta = S.extractSpreadsheetInvoiceMeta(spreadsheet, fileName);
  const table = S.findInvoiceTable(spreadsheet);
  if (!table) return { meta, table: null };
  const lineRows = S.extractSpreadsheetLineRows(table);
  const invoices = S.buildSpreadsheetInvoices(lineRows.rows, meta, fileName);
  const reconciliation = S.reconcileSpreadsheetTotals(invoices, meta, lineRows);
  return { meta, table, lineRows, invoices, reconciliation };
}

/* ─── the real Lidl / EmergeTech workbook ─────────────────────────────────── */
group('real workbook (Lidl Invoice 04.08.26.xlsx)', () => {
  const workbook = JSON.parse(fs.readFileSync(path.join(__dirname, 'fixtures', 'lidl-workbook.json'), 'utf8'));
  const out = parseWorkbook(workbook, 'Lidl Invoice 04.08.26.xlsx');

  check('found the table', out.table && out.table.sheetName, 'Shifts');
  check('carrier from Invoice tab', out.meta.carrierType, 'EmergeTech INC');
  // Used to fall back to the filename and produce "See Below".
  check('invoice number from Invoice tab', out.meta.invoiceNumber, 'Emerge 04.08.26');
  check('declared total read', out.meta.declaredTotal, 5186.68);

  check('row count', out.lineRows.rows.length, 5);
  check('single invoice', out.invoices.length, 1);
  check('total matches declared', Number(out.invoices[0].totalAmount.toFixed(2)), 5186.68);
  check('reconciled', out.reconciliation.matches, true);
  check('no issues', out.reconciliation.issues, []);

  // Three RDCs are represented: Perryville (70001), Mebane (60001), FRG (50001).
  const codings = out.invoices[0].groups.map(g => g.coding).sort();
  check('RDC split', codings, ['360100, 50001, CAT 4', '360100, 60001, CAT 4', '360100, 70001, CAT 4']);

  const logRows = S.buildSpreadsheetLogRows(out.invoices[0], 'Lidl Invoice 04.08.26.xlsx', 'Drive');
  check('one log row per coding group', logRows.length, 3);
  check('amount logged as a number', typeof logRows[0][7], 'number');
  const logged = logRows.reduce((sum, row) => sum + row[7], 0);
  check('log rows sum to the invoice', Number(logged.toFixed(2)), 5186.68);
});

/* ─── a register workbook: many invoices, one per line ────────────────────── */
group('multi-invoice register', () => {
  const workbook = makeGridWorkbook('Sheet1', [
    ['Invoice No', 'Date', 'Carrier', 'PO Number', 'Origin', 'Destination', 'Amount'],
    ['INV-1001', '03/24/2026', 'Acme Freight', '118824032662', 'Worcester, MA', 'Perryville, MD', 932.03],
    ['INV-1002', '03/25/2026', 'Acme Freight', '153727032630', 'Kennesaw, GA', 'Mebane, NC', 1320],
    ['INV-1002', '03/25/2026', 'Acme Freight', '153727032631', 'Kennesaw, GA', 'Mebane, NC', 680],
    ['INV-1003', '03/27/2026', 'Acme Freight', '105923032602', 'Bridgeton, NJ', 'Fredericksburg, VA', 1070.59],
    ['', '', '', '', '', '', ''],
    ['Total', '', '', '', '', '', 4002.62]
  ]);
  const out = parseWorkbook(workbook, 'March Register.xlsx');

  check('three invoices', out.invoices.length, 3);
  check('invoice numbers', out.invoices.map(i => i.invoiceNumber), ['INV-1001', 'INV-1002', 'INV-1003']);
  // INV-1002 has two lines that must be added together, not treated as two invoices.
  check('multi-line invoice summed', out.invoices[1].totalAmount, 2000);
  check('footer Total row ignored', out.reconciliation.skippedTotalRows, 1);
  check('grand total', Number(out.reconciliation.computedTotal.toFixed(2)), 4002.62);
  check('carrier from the rows', S.resolveSpreadsheetCarrier(out.lineRows.rows, out.meta), 'Acme Freight');

  // Each invoice codes independently off its own destination.
  check('INV-1001 coding', out.invoices[0].groups[0].coding, '360100, 70001, CAT 4');
  check('INV-1003 coding', out.invoices[2].groups[0].coding, '360100, 50001, CAT 4');

  const allLogRows = [];
  out.invoices.forEach(invoice => {
    S.buildSpreadsheetLogRows(invoice, 'March Register.xlsx', 'Drive').forEach(row => allLogRows.push(row));
  });
  check('one log row per invoice', allLogRows.length, 3);
});

/* ─── header-shape tolerance ──────────────────────────────────────────────── */
group('header detection', () => {
  // Header not on row 1, different column spellings, a title block above it.
  const workbook = makeGridWorkbook('Billing', [
    ['CARRIER BILLING SUMMARY', '', '', ''],
    ['Prepared 04/08/2026', '', '', ''],
    ['', '', '', ''],
    ['Ship Date', 'Purchase Order', 'Ship To', 'Total Charge'],
    ['03/24/2026', '118824032662', 'Perryville, MD', '$932.03'],
    ['03/25/2026', '153727032630', 'Mebane, NC', '$1,320.00']
  ]);
  const out = parseWorkbook(workbook, 'summary.xlsx');
  check('header found below a title block', out.table && out.table.headerIndex, 3);
  check('rows parsed', out.lineRows.rows.length, 2);
  check('currency strings parsed', Number(out.reconciliation.computedTotal.toFixed(2)), 2252.03);

  // Transport cost + accessorials when there is no explicit total column.
  const split = makeGridWorkbook('Shifts', [
    ['Delivery Date', 'PO #', 'Dropoff Location', 'Transport Cost', 'Tolls'],
    ['03/24/2026', '118824032662', 'Perryville, MD', 900, 32.03]
  ]);
  const splitOut = parseWorkbook(split, 'split.xlsx');
  check('transport + tolls', splitOut.lineRows.rows[0].totalAmount, 932.03);

  check('column synonyms are disjoint', (() => {
    const seen = {};
    let clash = null;
    Object.keys(S.SPREADSHEET_COLUMN_SYNONYMS).forEach(field => {
      S.SPREADSHEET_COLUMN_SYNONYMS[field].forEach(word => {
        if (seen[word]) clash = `${word} in ${seen[word]} and ${field}`;
        seen[word] = field;
      });
    });
    return clash;
  })(), null);

  // A sheet with no amount column is not an invoice table.
  const noAmount = makeGridWorkbook('Directions', [
    ['Origin', 'Destination', 'Notes'],
    ['Worcester, MA', 'Perryville, MD', 'Use dock 4']
  ]);
  check('no amount column rejected', S.findInvoiceTable(makeSpreadsheet(noAmount)), null);
});

/* ─── reconciliation and broken cells ─────────────────────────────────────── */
group('reconciliation', () => {
  // Declared total disagrees with the line items: must be flagged, not logged.
  const workbook = {
    Invoice: {
      raw: [['Carrier:', 'Acme Freight'], ['Invoice #:', 'INV-9001'], ['Total Amount Due:', 2000]],
      display: [['Carrier:', 'Acme Freight'], ['Invoice #:', 'INV-9001'], ['Total Amount Due:', '2000']]
    },
    Shifts: {
      raw: [
        ['Delivery Date', 'PO #', 'Dropoff Location', 'Total'],
        ['03/24/2026', '118824032662', 'Perryville, MD', 900],
        ['03/25/2026', '153727032630', 'Mebane, NC', 600]
      ],
      display: [
        ['Delivery Date', 'PO #', 'Dropoff Location', 'Total'],
        ['03/24/2026', '118824032662', 'Perryville, MD', '900'],
        ['03/25/2026', '153727032630', 'Mebane, NC', '600']
      ]
    }
  };
  const out = parseWorkbook(workbook, 'mismatch.xlsx');
  check('mismatch detected', out.reconciliation.matches, false);
  check('difference reported', out.reconciliation.difference, -500);
  check('flagged for review', out.reconciliation.needsReview, true);

  // A #REF! cell must be surfaced, not silently counted as zero.
  const broken = makeGridWorkbook('Shifts', [
    ['Delivery Date', 'PO #', 'Dropoff Location', 'Total'],
    ['03/24/2026', '118824032662', 'Perryville, MD', 900],
    ['03/25/2026', '153727032630', 'Mebane, NC', '#REF!']
  ]);
  const brokenOut = parseWorkbook(broken, 'broken.xlsx');
  check('error cell recorded', brokenOut.reconciliation.errorCells.length, 1);
  check('error flags review', brokenOut.reconciliation.needsReview, true);
  check('error invoice marked low confidence', brokenOut.invoices[0].invoiceData.amountConfidence, 'low');
});

/* ─── end to end, with Drive mocked out ───────────────────────────────────── */
group('extractSpreadsheetInvoiceData wiring', () => {
  const workbook = JSON.parse(fs.readFileSync(path.join(__dirname, 'fixtures', 'lidl-workbook.json'), 'utf8'));

  S.Drive = {
    Files: {
      create: () => ({ id: 'temp-sheet-id' }),
      get: () => ({ mimeType: 'application/vnd.google-apps.spreadsheet' })
    }
  };
  S.DriveApp = { getFileById: () => ({ setTrashed() {} }) };
  S.SpreadsheetApp = { openById: () => makeSpreadsheet(workbook) };
  S.ScriptApp = { getOAuthToken: () => 'token' };
  S.UrlFetchApp = {
    fetch: () => ({
      getResponseCode: () => 200,
      getBlob: () => S.Utilities.newBlob('pdf-bytes', 'application/pdf', 'original.pdf')
    })
  };

  const result = S.extractSpreadsheetInvoiceData(
    S.Utilities.newBlob('xlsx-bytes', S.XLSX_MIME_TYPE, 'Lidl Invoice 04.08.26.xlsx'),
    'Lidl Invoice 04.08.26.xlsx',
    'Drive Folder: Inbound'
  );

  check('carrier returned', result.carrierType, 'EmergeTech INC');
  check('sheet reported', result.sheetName, 'Shifts');
  check('one invoice', result.invoices.length, 1);
  check('log rows built', result.logRows.length, 3);
  check('coding summary built', result.codingSummary.indexOf('70001') >= 0, true);
  check('code sheet produced', !!result.codeSheetBlob, true);
  check('original exported', !!result.originalPdfBlob, true);
  check('reconciled end to end', result.reconciliation.matches, true);

  const logged = result.logRows.reduce((sum, row) => sum + row[7], 0);
  check('logged total', Number(logged.toFixed(2)), 5186.68);
  check('log row width matches headers', result.logRows[0].length, S.LOG_HEADERS.length);
});

/* ─── archive helpers ─────────────────────────────────────────────────────── */
group('archives', () => {
  check('zip by extension', S.isArchiveMimeType('application/octet-stream', 'week12.zip'), true);
  check('zip by mime', S.isArchiveMimeType('application/x-zip-compressed', 'week12'), true);
  // A bare octet-stream is not assumed to be an archive.
  check('unlabelled binary is not a zip', S.isArchiveMimeType('application/octet-stream', 'scan'), false);
  check('pdf is not a zip', S.isArchiveMimeType('application/pdf', 'invoice.pdf'), false);

  check('pdf mime from name', S.mimeTypeForFileName('a/b/invoice.PDF'), 'application/pdf');
  check('xlsx mime from name', S.mimeTypeForFileName('register.xlsx'), S.XLSX_MIME_TYPE);
  check('csv mime from name', S.mimeTypeForFileName('rows.csv'), 'text/csv');

  check('macOS junk ignored', S.isIgnorableArchiveEntry('__MACOSX/._invoice.pdf'), true);
  check('AppleDouble ignored', S.isIgnorableArchiveEntry('folder/._invoice.pdf'), true);
  check('DS_Store ignored', S.isIgnorableArchiveEntry('.DS_Store'), true);
  check('directory entry ignored', S.isIgnorableArchiveEntry('invoices/'), true);
  check('real entry kept', S.isIgnorableArchiveEntry('invoices/invoice-1.pdf'), false);

  check('entry file name', S.buildArchiveEntryFileName('Week 12', 'north/invoice-1001.pdf'), 'Week 12 - north - invoice-1001.pdf');
  check('unsafe characters stripped', S.buildArchiveEntryFileName('Week 12', 'a:b*c.pdf'), 'Week 12 - a b c.pdf');

  check('xls recognised', S.isSpreadsheetInvoiceMimeType('application/vnd.ms-excel', 'old.xls'), true);
  check('csv recognised', S.isSpreadsheetInvoiceMimeType('text/csv', 'rows.csv'), true);
  check('xlsm recognised', S.isSpreadsheetInvoiceMimeType('', 'macro.xlsm'), true);
  check('pdf is not a spreadsheet', S.isSpreadsheetInvoiceMimeType('application/pdf', 'invoice.pdf'), false);

  check('archives are ingestible', S.isIngestibleInvoiceMimeType('application/zip', 'week.zip'), true);
  check('archives are not invoices', S.isSupportedInvoiceMimeType('application/zip', 'week.zip'), false);
  check('zip extension stripped', S.stripInvoiceExtension('Week 12.zip'), 'Week 12');
  check('csv extension stripped', S.stripInvoiceExtension('rows.csv'), 'rows');

  // Expansion, with Utilities.unzip stubbed to return a nested archive.
  const blob = (name, type) => ({
    _name: name, _type: type,
    getName() { return this._name; },
    setName(n) { this._name = n; return this; },
    setContentType(t) { this._type = t; return this; },
    copyBlob() { return blob(this._name, this._type); }
  });
  S.Utilities.unzip = b => {
    if (b.getName() === 'outer.zip') {
      return [blob('__MACOSX/._x.pdf'), blob('readme.txt'), blob('invoice-1.pdf'), blob('inner.zip')];
    }
    return [blob('deep/invoice-2.pdf')];
  };
  const expansion = S.expandInvoiceArchive(blob('outer.zip', 'application/zip'), 'outer.zip', 0);
  check('entries extracted', expansion.entries.map(e => e.path), ['invoice-1.pdf', 'inner.zip/deep/invoice-2.pdf']);
  check('non-invoice skipped', expansion.skipped, ['readme.txt']);
  check('entry mime retyped', expansion.entries[0].mimeType, 'application/pdf');
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
