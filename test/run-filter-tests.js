/**
 * Email filtering tests: forwarded-message unwrapping, rule evaluation,
 * Gmail query building, and migration from the legacy SEARCH_QUERY.
 * Run: node test/run-filter-tests.js
 */
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

/** A stand-in for a GmailMessage. */
function makeMessage(fields) {
  const f = fields || {};
  const headers = f.headers || {};
  return {
    getFrom: () => f.from || '',
    getTo: () => f.to || '',
    getCc: () => f.cc || '',
    getReplyTo: () => f.replyTo || '',
    getSubject: () => f.subject || '',
    getPlainBody: () => f.body || '',
    getHeader: name => headers[name] || '',
    getId: () => f.id || 'msg-1',
    isUnread: () => f.unread !== false,
    markRead() { f.markedRead = true; },
    getAttachments: () => (f.attachmentNames || []).map(name => ({
      getName: () => name,
      getContentType: () => 'application/pdf',
      getSize: () => 1024,
      copyBlob: () => S.Utilities.newBlob('x', 'application/pdf', name)
    }))
  };
}

function contextFor(fields, filters) {
  const message = makeMessage(fields);
  return S.buildMessageFilterContext(message, message.getAttachments(), filters || S.DEFAULT_EMAIL_FILTERS);
}

/* ─── the actual problem: a forward from the delegated mailbox ────────────── */
group('delegated forward unwrapping', () => {
  const filters = S.normalizeEmailFilters({
    delegatedMailboxes: ['logistics.invoices@lidl.us']
  });

  // Gmail-style forward.
  const gmailForward = contextFor({
    from: 'Logistics Invoices <logistics.invoices@lidl.us>',
    to: 'Nigel <nigel@lidl.us>',
    subject: 'Fwd: Invoice INV6744248',
    headers: { 'Delivered-To': 'nigel@lidl.us' },
    body: [
      'FYI',
      '',
      '---------- Forwarded message ---------',
      'From: Arrive Billing <billing@arrivelogistics.com>',
      'Date: Mon, Apr 6, 2026 at 9:15 AM',
      'Subject: Invoice INV6744248 - Inbound Load 8523482',
      'To: <logistics.invoices@lidl.us>',
      '',
      'Please find attached.'
    ].join('\n'),
    attachmentNames: ['invoice-INV6744248.pdf']
  }, filters);

  check('original sender recovered', gmailForward.originalFrom, 'Arrive Billing <billing@arrivelogistics.com>');
  check('effectiveFrom looks through the forward', gmailForward.effectiveFromAddress, 'billing@arrivelogistics.com');
  // Without unwrapping this would be the delegated mailbox — the whole bug.
  check('raw from is still the delegated box', gmailForward.fromAddress, 'logistics.invoices@lidl.us');
  check('original subject recovered', gmailForward.originalSubject, 'Invoice INV6744248 - Inbound Load 8523482');
  check('recognised as forwarded', gmailForward.isForwarded, true);
  check('delegated mailbox identified', gmailForward.forwardedFrom, 'logistics.invoices@lidl.us');
  check('flagged as a delegated forward', gmailForward.isDelegatedForward, true);

  // Outlook-style forward: no marker line, "Sent:" instead of "Date:".
  const outlookForward = contextFor({
    from: 'Logistics Invoices <logistics.invoices@lidl.us>',
    subject: 'FW: Outbound shipment 4471 paperwork',
    body: [
      'See below.',
      '',
      'From: Dispatch <dispatch@carrier.com>',
      'Sent: Monday, April 6, 2026 9:15 AM',
      'To: Logistics Invoices <logistics.invoices@lidl.us>',
      'Subject: Outbound shipment 4471 paperwork',
      '',
      'Attached.'
    ].join('\n'),
    attachmentNames: ['BOL-4471.pdf']
  }, filters);

  check('outlook sender recovered', outlookForward.effectiveFromAddress, 'dispatch@carrier.com');
  check('outlook subject recovered', outlookForward.originalSubject, 'Outbound shipment 4471 paperwork');
  check('outlook date captured', outlookForward.originalDate, 'Monday, April 6, 2026 9:15 AM');

  // A direct (non-forwarded) message falls back to its own sender cleanly.
  const direct = contextFor({
    from: 'Arrive Billing <billing@arrivelogistics.com>',
    subject: 'Invoice INV6737123',
    body: 'Invoice attached. Thanks.',
    attachmentNames: ['invoice-INV6737123.pdf']
  }, filters);
  check('direct message effectiveFrom', direct.effectiveFromAddress, 'billing@arrivelogistics.com');
  check('direct message not forwarded', direct.isForwarded, false);
  check('direct message no delegated box', direct.forwardedFrom, '');

  // Prose that merely mentions "From:" must not read as a forward.
  const prose = contextFor({
    from: 'Someone <someone@example.com>',
    subject: 'Question',
    body: 'From: the warehouse team we heard the load is late.',
    attachmentNames: []
  }, filters);
  check('prose is not a forward envelope', prose.isForwarded, false);
});

/* ─── subject helpers ─────────────────────────────────────────────────────── */
group('subject and address helpers', () => {
  check('strip single prefix', S.stripForwardPrefixes('Fwd: Invoice 1'), 'Invoice 1');
  check('strip stacked prefixes', S.stripForwardPrefixes('RE: FW: Fwd: Invoice 1'), 'Invoice 1');
  check('strip numbered reply', S.stripForwardPrefixes('Re[2]: Invoice 1'), 'Invoice 1');
  check('leave a clean subject alone', S.stripForwardPrefixes('Invoice 1'), 'Invoice 1');

  check('address from angle brackets', S.extractEmailAddress('Jane <jane@carrier.com>'), 'jane@carrier.com');
  check('bare address', S.extractEmailAddress('jane@carrier.com'), 'jane@carrier.com');
  check('address inside prose', S.extractEmailAddress('write to jane@carrier.com, thanks'), 'jane@carrier.com');
  check('no address', S.extractEmailAddress('Jane'), '');
  check('domain', S.extractEmailDomain('Jane <jane@mail.carrier.com>'), 'mail.carrier.com');
});

/* ─── rules ───────────────────────────────────────────────────────────────── */
group('rule evaluation', () => {
  const inboundContext = contextFor({
    from: 'Logistics Invoices <logistics.invoices@lidl.us>',
    subject: 'Fwd: Invoice INV6744248',
    body: [
      '---------- Forwarded message ---------',
      'From: Arrive Billing <billing@arrivelogistics.com>',
      'Subject: Inbound delivery invoice INV6744248',
      'To: <logistics.invoices@lidl.us>'
    ].join('\n'),
    attachmentNames: ['invoice-INV6744248.pdf']
  });

  const outboundContext = contextFor({
    from: 'Logistics Invoices <logistics.invoices@lidl.us>',
    subject: 'Fwd: Outbound load 4471',
    body: [
      '---------- Forwarded message ---------',
      'From: Dispatch <dispatch@carrier.com>',
      'Subject: Outbound load 4471 - customer delivery',
      'To: <logistics.invoices@lidl.us>'
    ].join('\n'),
    attachmentNames: ['BOL-4471.pdf']
  });

  // Ordered, first-match-wins: the outbound reject sits above the inbound accept.
  const filters = S.normalizeEmailFilters({
    delegatedMailboxes: ['logistics.invoices@lidl.us'],
    defaultAction: 'review',
    rules: [
      { id: 'r1', name: 'Skip outbound', action: 'reject', field: 'effectiveSubject', operator: 'contains', value: 'outbound' },
      { id: 'r2', name: 'Process inbound', action: 'accept', field: 'effectiveSubject', operator: 'contains', value: 'inbound' }
    ]
  });

  const inboundDecision = S.evaluateEmailFilters(inboundContext, filters);
  check('inbound accepted', inboundDecision.action, 'accept');
  check('inbound names the rule', inboundDecision.rule.name, 'Process inbound');

  const outboundDecision = S.evaluateEmailFilters(outboundContext, filters);
  check('outbound skipped', outboundDecision.action, 'skip');
  check('outbound names the rule', outboundDecision.rule.name, 'Skip outbound');

  // Nothing matches -> configured default.
  const neither = contextFor({
    from: 'Someone <someone@example.com>',
    subject: 'Paperwork',
    body: 'No keywords here.',
    attachmentNames: ['doc.pdf']
  });
  check('unmatched falls back to review', S.evaluateEmailFilters(neither, filters).action, 'review');
  check('unmatched has no rule', S.evaluateEmailFilters(neither, filters).rule, null);

  const skipDefault = S.normalizeEmailFilters({ defaultAction: 'skip', rules: [] });
  check('skip default honoured', S.evaluateEmailFilters(neither, skipDefault).action, 'skip');

  const off = S.normalizeEmailFilters({ enabled: false, defaultAction: 'skip', rules: [] });
  check('filtering off accepts everything', S.evaluateEmailFilters(neither, off).action, 'accept');

  // Order decides: put the accept first and the same mail is processed.
  const flipped = S.normalizeEmailFilters({
    rules: [
      { id: 'r2', name: 'Process inbound', action: 'accept', field: 'body', operator: 'contains', value: 'load 4471' },
      { id: 'r1', name: 'Skip outbound', action: 'reject', field: 'effectiveSubject', operator: 'contains', value: 'outbound' }
    ]
  });
  check('first match wins', S.evaluateEmailFilters(outboundContext, flipped).action, 'accept');

  // A disabled rule is skipped entirely.
  const disabled = S.normalizeEmailFilters({
    defaultAction: 'accept',
    rules: [{ id: 'r1', name: 'Skip outbound', enabled: false, action: 'reject', field: 'effectiveSubject', operator: 'contains', value: 'outbound' }]
  });
  check('disabled rule ignored', S.evaluateEmailFilters(outboundContext, disabled).action, 'accept');
});

/* ─── operators ───────────────────────────────────────────────────────────── */
group('operators', () => {
  const context = contextFor({
    from: 'Arrive Billing <billing@arrivelogistics.com>',
    to: 'ap@lidl.us',
    subject: 'Invoice INV6744248',
    body: 'Inbound delivery, net 30.',
    attachmentNames: ['invoice-INV6744248.pdf', 'terms.pdf']
  });

  const decide = rule => S.evaluateEmailFilterRule(S.normalizeEmailFilterRule(rule, 0), context);

  check('contains', decide({ field: 'subject', operator: 'contains', value: 'inv6744248' }), true);
  check('contains is case-insensitive by default', decide({ field: 'subject', operator: 'contains', value: 'INVOICE' }), true);
  check('caseSensitive respected', decide({ field: 'subject', operator: 'contains', value: 'invoice', caseSensitive: true }), false);
  check('notContains', decide({ field: 'subject', operator: 'notContains', value: 'outbound' }), true);
  check('equals', decide({ field: 'subject', operator: 'equals', value: 'Invoice INV6744248' }), true);
  check('startsWith', decide({ field: 'subject', operator: 'startsWith', value: 'invoice' }), true);
  check('endsWith', decide({ field: 'subject', operator: 'endsWith', value: '6744248' }), true);
  check('regex', decide({ field: 'subject', operator: 'regex', value: 'INV\\d{7}' }), true);
  check('domainIs', decide({ field: 'effectiveFrom', operator: 'domainIs', value: 'arrivelogistics.com' }), true);
  check('domainIs tolerates a leading @', decide({ field: 'effectiveFrom', operator: 'domainIs', value: '@arrivelogistics.com' }), true);
  check('domainIs rejects another domain', decide({ field: 'effectiveFrom', operator: 'domainIs', value: 'lidl.us' }), false);
  check('domainIs matches subdomains', S.evaluateEmailFilterRule(
    S.normalizeEmailFilterRule({ field: 'from', operator: 'domainIs', value: 'carrier.com' }, 0),
    contextFor({ from: 'a <a@mail.carrier.com>' })
  ), true);
  check('isEmpty', decide({ field: 'cc', operator: 'isEmpty', value: '' }), true);
  check('isNotEmpty', decide({ field: 'subject', operator: 'isNotEmpty', value: '' }), true);

  // attachmentName matches if ANY attachment matches.
  check('any attachment matches', decide({ field: 'attachmentName', operator: 'contains', value: 'terms' }), true);
  check('no attachment matches', decide({ field: 'attachmentName', operator: 'contains', value: 'packing' }), false);
  // 'any' searches everything at once.
  check('any field', decide({ field: 'any', operator: 'contains', value: 'net 30' }), true);
});

/* ─── rule normalization ──────────────────────────────────────────────────── */
group('rule normalization', () => {
  check('unknown field falls back to any', S.normalizeEmailFilterRule({ field: 'nope', operator: 'contains', value: 'x' }, 0).field, 'any');
  check('unknown operator falls back to contains', S.normalizeEmailFilterRule({ field: 'subject', operator: 'nope', value: 'x' }, 0).operator, 'contains');
  check('unknown action falls back to reject', S.normalizeEmailFilterRule({ field: 'subject', operator: 'contains', value: 'x', action: 'nope' }, 0).action, 'reject');
  // A rule with no value would match everything — drop it rather than ship a trap.
  check('empty value dropped', S.normalizeEmailFilterRule({ field: 'subject', operator: 'contains', value: '  ' }, 0), null);
  check('empty value kept for isEmpty', !!S.normalizeEmailFilterRule({ field: 'cc', operator: 'isEmpty', value: '' }, 0), true);
  // An unparseable regex would throw on every message.
  check('bad regex dropped', S.normalizeEmailFilterRule({ field: 'subject', operator: 'regex', value: '([unclosed' }, 0), null);
  check('name auto-generated', S.normalizeEmailFilterRule({ field: 'subject', operator: 'contains', value: 'x', action: 'reject' }, 0).name, 'Skip when subject contains "x"');
  check('shipped defaults are all valid', S.normalizeEmailFilters(S.DEFAULT_EMAIL_FILTERS).rules.length, S.DEFAULT_EMAIL_FILTERS.rules.length);
});

/* ─── gmail query building ────────────────────────────────────────────────── */
group('gmail query', () => {
  check('default builder query', S.buildGmailSearchQuery({
    mode: 'builder', hasAttachment: true, unreadOnly: true, newerThanDays: 30
  }), 'has:attachment is:unread newer_than:30d');

  check('single label', S.buildGmailSearchQuery({
    mode: 'builder', hasAttachment: true, unreadOnly: true, labels: ['Automation-Emails/ITBP'], newerThanDays: 0
  }), 'has:attachment is:unread label:Automation-Emails/ITBP');

  check('labels with spaces are quoted', S.buildGmailSearchQuery({
    mode: 'builder', hasAttachment: false, unreadOnly: false, labels: ['Carrier Invoices'], newerThanDays: 0
  }), 'label:"Carrier Invoices"');

  check('multiple senders become an OR group', S.buildGmailSearchQuery({
    mode: 'builder', hasAttachment: false, unreadOnly: false, newerThanDays: 0,
    fromAnyOf: ['a@x.com', 'b@y.com']
  }), '(from:a@x.com OR from:b@y.com)');

  // Catching a delegated forward at the fetch layer.
  check('deliveredto for the delegated box', S.buildGmailSearchQuery({
    mode: 'builder', hasAttachment: true, unreadOnly: true, newerThanDays: 0,
    deliveredToAnyOf: ['logistics.invoices@lidl.us']
  }), 'has:attachment is:unread deliveredto:logistics.invoices@lidl.us');

  check('raw mode passes through', S.buildGmailSearchQuery({
    mode: 'raw', rawQuery: 'label:Foo has:attachment'
  }), 'label:Foo has:attachment');

  // Never return an empty query — that would match the whole mailbox.
  check('empty builder is not an empty query', S.buildGmailSearchQuery({
    mode: 'builder', hasAttachment: false, unreadOnly: false, newerThanDays: 0
  }), 'has:attachment is:unread');
  check('blank raw query falls back', S.buildGmailSearchQuery({ mode: 'raw', rawQuery: '   ' }), S.DEFAULTS.SEARCH_QUERY);
});

/* ─── migration from the legacy query ─────────────────────────────────────── */
group('migration', () => {
  const parsed = S.parseGmailQueryToFetchSettings('has:attachment is:unread label:Automation-Emails/ITBP');
  check('label carried over', parsed.labels, ['Automation-Emails/ITBP']);
  check('unread carried over', parsed.unreadOnly, true);
  check('attachment carried over', parsed.hasAttachment, true);
  check('nothing left over', parsed.extraTerms, '');
  // Round-trips to the same query, so the migration cannot change behaviour.
  check('round trips', S.buildGmailSearchQuery(parsed), 'has:attachment is:unread label:Automation-Emails/ITBP');

  const grouped = S.parseGmailQueryToFetchSettings('has:attachment (from:"a@x.com" OR from:b@y.com) newer_than:14d');
  check('OR group unpacked', grouped.fromAnyOf, ['a@x.com', 'b@y.com']);
  check('newer_than unpacked', grouped.newerThanDays, 14);

  const unknown = S.parseGmailQueryToFetchSettings('has:attachment is:starred category:primary');
  check('unmodelled terms preserved', unknown.extraTerms, 'is:starred category:primary');

  // Migration must not start rejecting mail that used to be processed.
  const migrated = S.migrateEmailFiltersFromSearchQuery('has:attachment is:unread label:Automation-Emails/ITBP');
  check('migrated default is permissive', migrated.defaultAction, 'accept');
  check('migrated accept rules start off', migrated.rules.filter(r => r.action === 'accept' && r.enabled).length, 0);
});

/* ─── end to end through processIncomingPDFs ──────────────────────────────── */
group('gmail ingestion wiring', () => {
  const inbound = makeMessage({
    id: 'm-inbound',
    from: 'Logistics Invoices <logistics.invoices@lidl.us>',
    subject: 'Fwd: Invoice INV6744248',
    body: [
      '---------- Forwarded message ---------',
      'From: Arrive Billing <billing@arrivelogistics.com>',
      'Subject: Inbound invoice INV6744248',
      'To: <logistics.invoices@lidl.us>'
    ].join('\n'),
    attachmentNames: ['invoice-INV6744248.pdf']
  });

  const outbound = makeMessage({
    id: 'm-outbound',
    from: 'Logistics Invoices <logistics.invoices@lidl.us>',
    subject: 'Fwd: Load 4471',
    body: [
      '---------- Forwarded message ---------',
      'From: Dispatch <dispatch@carrier.com>',
      'Subject: Outbound load 4471 paperwork',
      'To: <logistics.invoices@lidl.us>'
    ].join('\n'),
    attachmentNames: ['BOL-4471.pdf']
  });

  const store = {
    EMAIL_FILTERS: JSON.stringify(S.normalizeEmailFilters({
      delegatedMailboxes: ['logistics.invoices@lidl.us'],
      defaultAction: 'skip',
      rules: [
        { id: 'r1', name: 'Skip outbound', action: 'reject', field: 'effectiveSubject', operator: 'contains', value: 'outbound' },
        { id: 'r2', name: 'Process inbound', action: 'accept', field: 'effectiveSubject', operator: 'contains', value: 'inbound' }
      ]
    })),
    SHEET_ID: 'sheet-id',
    TARGET_EMAIL: 'ap@example.com',
    PROCESSED_FOLDER_ID: 'processed-id',
    SOURCE_FOLDER_IDS: 'source-id'
  };

  S.PropertiesService = {
    getScriptProperties: () => ({
      getProperty: key => (key in store ? store[key] : null),
      setProperty: (key, value) => { store[key] = value; },
      deleteProperty: key => { delete store[key]; },
      getProperties: () => Object.assign({}, store)
    })
  };
  S.LockService = { getScriptLock: () => ({ waitLock() {}, releaseLock() {} }) };
  S.DriveApp = { getFolderById: () => ({ getName: () => 'Processed', createFile: () => ({}) }) };
  S.Drive = { Files: { create: () => ({ id: 'temp-doc-id' }) } };
  S.DocumentApp = { openById: () => ({ getBody: () => ({ getText: () => 'Invoice # INV6744248\nAmount Due $2,700.00' }) }) };
  S.MailApp = { sendEmail() {} };

  const appendedRows = [];
  const sheet = {
    getMaxColumns: () => 26,
    insertColumnsAfter() {},
    getRange: () => ({ getValues: () => [S.LOG_HEADERS.slice()], setValues() {} }),
    appendRow: row => appendedRows.push(row)
  };
  S.SpreadsheetApp = { openById: () => ({ getSheetByName: () => sheet, insertSheet: () => sheet }) };

  let searched = null;
  S.GmailApp = {
    search: query => { searched = query; return [{ getMessages: () => [inbound, outbound] }]; },
    getUserLabels: () => []
  };

  // Filters are cached per execution; this test swapped the property store.
  S.clearEmailFiltersCache();
  const summary = S.processIncomingPDFs();

  check('query came from the filters', searched, S.buildGmailSearchQuery(S.getEmailFilters().fetch));
  check('both messages seen', summary.unreadMessages, 2);
  // The outbound forward is rejected on the ORIGINAL subject, which native
  // Gmail filters cannot see at all.
  check('outbound filtered out', summary.filteredOut, 1);
  check('inbound processed', summary.processed, 1);
  check('only the inbound invoice logged', appendedRows.length, 1);
  check('logged the inbound invoice', appendedRows[0][3], 'INV6744248');
  // Skipped mail stays unread by default so a bad rule is easy to spot.
  check('skipped message left unread', outbound.markedRead === undefined || outbound.markedRead === false, true);
});

/* ─── the batch limit must cover filtered messages too ────────────────────── */
group('run budget', () => {
  // Queueing for review costs an OCR pass per message, so the per-run limit has
  // to apply to messages the filters never accept — not just processed ones.
  const many = [];
  for (let i = 0; i < S.MAX_INVOICES_PER_RUN + 10; i++) {
    many.push(makeMessage({
      id: 'm-' + i,
      from: 'Someone <someone@example.com>',
      subject: 'Unclassifiable ' + i,
      body: 'no keywords',
      attachmentNames: ['doc-' + i + '.pdf']
    }));
  }

  const store = {
    EMAIL_FILTERS: JSON.stringify(S.normalizeEmailFilters({ defaultAction: 'review', rules: [] })),
    SHEET_ID: 'sheet-id',
    TARGET_EMAIL: 'ap@example.com',
    PROCESSED_FOLDER_ID: 'processed-id',
    SOURCE_FOLDER_IDS: 'source-id'
  };
  S.PropertiesService = {
    getScriptProperties: () => ({
      getProperty: key => (key in store ? store[key] : null),
      setProperty: (key, value) => { store[key] = value; },
      deleteProperty: key => { delete store[key]; },
      getProperties: () => Object.assign({}, store)
    })
  };

  let created = 0;
  S.DriveApp = {
    getFolderById: () => ({
      getName: () => 'Source',
      createFile: () => { created += 1; return { getId: () => 'file-' + created, setTrashed() {} }; }
    }),
    getFileById: () => ({ getName: () => 'doc.pdf', getMimeType: () => 'application/pdf', getBlob: () => S.Utilities.newBlob('x', 'application/pdf', 'doc.pdf'), setTrashed() {} })
  };
  S.GmailApp = { search: () => [{ getMessages: () => many }], getUserLabels: () => [] };

  S.clearEmailFiltersCache();
  const summary = S.processIncomingPDFs();

  check('stopped at the batch limit', summary.stoppedEarly, true);
  check('did not queue the whole mailbox', summary.queuedForReview <= S.MAX_INVOICES_PER_RUN, true);
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
