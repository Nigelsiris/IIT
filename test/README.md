# Tests

Apps Script cannot be run locally, but almost all of the risky logic in
`code.js` — amount selection, field extraction, spreadsheet parsing, archive
handling — is pure JavaScript. `harness.js` loads `code.js` into a Node VM with
the handful of Apps Script globals it touches stubbed out, so that logic can be
exercised directly.

```
npm test                        # all three suites
node test/run-tests.js          # amounts, PDF field extraction, coding, carrier
node test/run-sheet-tests.js    # spreadsheet invoices, archives
node test/run-pipeline-tests.js # processInvoiceFile end to end, with Apps Script mocked
```

## Fixtures

| File | What it is |
| --- | --- |
| `fixtures/arrive-*.txt` | Text of the two real Arrive Logistics PDFs in the repo root |
| `fixtures/lidl-workbook.json` | Cell values of `Lidl Invoice 04.08.26.xlsx`, as `{ sheet: { raw, display } }` |

`fake-spreadsheet.js` mounts that JSON (or an inline grid) behind the small
slice of the `SpreadsheetApp` API the parser uses, so the spreadsheet tests run
against the real workbook without touching Drive.

## Regenerating the fixtures

```bash
python3 -c "
import pdfplumber
with pdfplumber.open('invoice-INV6737123.pdf') as pdf:
    print('\n'.join(p.extract_text() or '' for p in pdf.pages))
" > test/fixtures/arrive-INV6737123.txt
```

The workbook fixture is a dump of every sheet's raw and displayed cell values;
dates serialize as `{"__date__": "<iso>"}` and are revived as `Date` objects by
`fake-spreadsheet.js`.

## Adding a case

Most bugs here are "this invoice read the wrong number". The cheapest
regression test is to paste the offending text into a fixture and assert on
`selectInvoiceAmount` or `extractInvoiceData`, which is what the existing
`selectInvoiceAmount — regression cases` group does.
