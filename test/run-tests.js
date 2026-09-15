/**
 * Regression tests for the pure extraction logic in code.js.
 * Run: node test/run-tests.js
 */
const fs = require('fs');
const path = require('path');
const { buildSandbox } = require('./harness');

const S = buildSandbox();
const fixture = name => fs.readFileSync(path.join(__dirname, 'fixtures', name), 'utf8');

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

/* ─── parseMoneyToken ─────────────────────────────────────────────────────── */
group('parseMoneyToken', () => {
  check('plain', S.parseMoneyToken('1150.00'), 1150);
  check('commas + dollar', S.parseMoneyToken('$1,150.00'), 1150);
  check('USD suffix', S.parseMoneyToken('2,700.00 USD'), 2700);
  check('parenthesised credit', S.parseMoneyToken('(1,150.00)'), -1150);
  check('trailing minus credit', S.parseMoneyToken('1,150.00-'), -1150);
  check('CR marker', S.parseMoneyToken('450.00 CR'), -450);
  check('leading minus', S.parseMoneyToken('-$450.00'), -450);
  check('no cents', S.parseMoneyToken('$1,320'), 1320);
  check('european grouping', S.parseMoneyToken('1.234,56'), 1234.56);
  check('decimal comma', S.parseMoneyToken('1234,56'), 1234.56);
  check('euro symbol', S.parseMoneyToken('€980,50'), 980.5);
  check('number passthrough', S.parseMoneyToken(932.03), 932.03);
  check('zero', S.parseMoneyToken('0.00'), 0);
  check('not money', S.parseMoneyToken('Review Required'), null);
  check('empty', S.parseMoneyToken(''), null);
  check('null', S.parseMoneyToken(null), null);
});

/* ─── selectInvoiceAmount on the real Arrive invoices ─────────────────────── */
group('selectInvoiceAmount — real invoices', () => {
  const a = S.selectInvoiceAmount(fixture('arrive-INV6737123.txt'));
  check('INV6737123 value', a && a.value, 1150);
  check('INV6737123 confidence', a && a.confidence, 'high');

  const b = S.selectInvoiceAmount(fixture('arrive-INV6744248.txt'));
  check('INV6744248 value', b && b.value, 2700);
  check('INV6744248 confidence', b && b.confidence, 'high');
});

/* ─── selectInvoiceAmount — the ways it used to go wrong ──────────────────── */
group('selectInvoiceAmount — regression cases', () => {
  // Used to return the first line item because of the bare-$ fallback regex.
  const multiLine = [
    'Item Quantity Amount',
    'Line Haul 1 $1,150.00',
    'Fuel Surcharge 1 $245.50',
    'Detention 2 $150.00',
    'Total $1,545.50',
    'Amount Due $1,545.50'
  ].join('\n');
  check('multi line item total', S.selectInvoiceAmount(multiLine).value, 1545.5);

  // Weights and quantities must never win.
  const weights = [
    'Equipment Type: Van Weight: 43,500.00',
    'Total Weight 52,300.00 lbs',
    'Pieces 1,240.00',
    'Amount Due $980.00'
  ].join('\n');
  check('weights rejected', S.selectInvoiceAmount(weights).value, 980);

  // Label on the line above the number (common after OCR of a table).
  const stacked = ['Subtotal', '1,000.00', 'Total Amount Due', '1,075.00'].join('\n');
  check('stacked labels', S.selectInvoiceAmount(stacked).value, 1075);

  // Credit note: total must come back negative.
  const credit = ['Line Haul -$500.00', 'Total Due (500.00)'].join('\n');
  check('credit note negative', S.selectInvoiceAmount(credit).value, -500);

  // Line items summing to the total rescue a weakly-labelled total.
  const sumCheck = ['Freight 400.00', 'Fuel 100.00', 'Accessorial 25.00', '525.00'].join('\n');
  check('line item sum cross-check', S.selectInvoiceAmount(sumCheck).value, 525);

  // Identifiers, dates and phone numbers are not amounts.
  const noMoney = [
    'Invoice # INV6744248',
    'Date 3/21/2026',
    'Phone 888-861-0650',
    'PO # 224817032660'
  ].join('\n');
  check('no amount found', S.selectInvoiceAmount(noMoney), null);

  // Ambiguity is reported rather than guessed.
  const ambiguous = ['Total $500.00', 'Total $750.00'].join('\n');
  const amb = S.selectInvoiceAmount(ambiguous);
  check('ambiguous confidence', amb.confidence, 'medium');

  // A bare dollar figure with no label at all is low confidence.
  const bare = ['Thanks for your business', '$1,234.56'].join('\n');
  check('bare amount confidence', S.selectInvoiceAmount(bare).confidence, 'low');
});

/* ─── extractInvoiceData end to end ───────────────────────────────────────── */
group('extractInvoiceData', () => {
  const d = S.extractInvoiceData(fixture('arrive-INV6737123.txt'), 'invoice-INV6737123.pdf');
  check('invoice number', d.invoiceNumber, 'INV6737123');
  check('amount', d.amount, '1150.00');
  check('amountValue', d.amountValue, 1150);
  check('amount confidence', d.amountConfidence, 'high');
  check('po', d.po, '118819032601');
  check('ship date', d.shipDate, '3/18/2026');
  // The payment "Due Date 4/19/2026" must not be read as the delivery date.
  check('delivery date', d.deliveryDate, '3/19/2026');
  check('origin', d.origin, 'Worcester, Massachusetts 01610');
  check('destination', d.destination, 'Fredericksburg, Virginia 22407-9321');
  check('product type', d.productType, 'Van');

  const d2 = S.extractInvoiceData(fixture('arrive-INV6744248.txt'), 'invoice-INV6744248.pdf');
  check('2nd invoice number', d2.invoiceNumber, 'INV6744248');
  check('2nd amount', d2.amount, '2700.00');
  check('2nd po', d2.po, '224817032660');
  check('2nd delivery date', d2.deliveryDate, '3/20/2026');
  check('2nd destination', d2.destination, 'Perryville, Maryland 21903');
});

/* ─── coding & carrier identification ─────────────────────────────────────── */
group('coding and carrier', () => {
  const t1 = fixture('arrive-INV6737123.txt');
  const d1 = S.extractInvoiceData(t1, 'invoice-INV6737123.pdf');
  const c1 = S.determineCoding(t1);
  check('FRG coding', c1, '360100, 50001, CAT 1');
  // "DM Trans, LLC d/b/a / Arrive Logistics" — the trading name is the carrier,
  // not the shipper ("Polar") and not the equipment type ("Van").
  check('carrier from d/b/a', S.determineCarrierType(t1, d1, c1, 'invoice-INV6737123.pdf'), 'Arrive Logistics');

  const t2 = fixture('arrive-INV6744248.txt');
  const d2 = S.extractInvoiceData(t2, 'invoice-INV6744248.pdf');
  const c2 = S.determineCoding(t2);
  check('Perryville coding', c2, '360100, 70001, CAT 4');
  check('2nd carrier', S.determineCarrierType(t2, d2, c2, 'invoice-INV6744248.pdf'), 'Arrive Logistics');

  check('equipment not a carrier', S.isEquipmentTypeValue('Dry Van'), true);
  check('company is a carrier', S.isEquipmentTypeValue('Arrive Logistics'), false);
  check('address line detected', S.isAddressLine('7701 Metropolis Drive'), true);
  check('name is not an address', S.isAddressLine('Arrive Logistics'), false);
});

/* ─── normalizeAmountText / sheetAmountValue ──────────────────────────────── */
group('amount normalization', () => {
  check('normalize $', S.normalizeAmountText('$1,150.00'), '1150.00');
  check('normalize prose', S.normalizeAmountText('Total Due: $2,700'), '2700.00');
  check('normalize junk', S.normalizeAmountText('See Below'), '');
  check('sheet numeric', S.sheetAmountValue({ amount: '1150.00', amountValue: 1150 }), 1150);
  check('sheet from string', S.sheetAmountValue({ amount: '1,150.00' }), 1150);
  // A reviewer edit or a mapping profile rewrites `amount`; the stale numeric
  // twin must not win.
  check('edited amount wins over stale value', S.sheetAmountValue({ amount: '2,000.00', amountValue: 1150 }), 2000);
  check('negative amount', S.sheetAmountValue({ amount: '-500.00' }), -500);
  check('sheet placeholder', S.sheetAmountValue({ amount: 'See Below' }), 'See Below');
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
