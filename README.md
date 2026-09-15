# IIT
Inbound Invoicing Tool

Google Apps Script automation for inbound freight invoices — PDFs, Excel workbooks and zip archives — with a custom web interface.

## What it does
- Reads PDF, Excel (`.xlsx`/`.xls`/`.csv`) and **`.zip`** invoices from configured Drive source folders
- Unpacks zip archives (including nested ones) into their individual invoices before processing
- OCRs and extracts invoice metadata
- Supports manual extraction mapping profiles for hard-to-parse invoice formats (like Arrive)
- Applies routing/coding rules
- Logs to a Google Sheet
- Sends an email with original PDF + generated coded summary PDF
- Reads spreadsheet invoices as data, including workbooks holding **many invoices, one per line**
- Reconciles spreadsheet line items against the total the workbook declares
- Holds invoices whose amount cannot be tied to an explicit label for manual review
- Prevents duplicate processing with idempotent processing state
- Uploads invoices directly from the web app into source folders
- Shows real-time processing activity feed in the web app
- Moves finalized invoices into carrier-type subfolders (auto-created)

Default folder IDs:
- Source: `1Ver7zumHu7ILaqUaTqiSi8rkd9XKDOy9`
- Processed root: `1qfE0XUL_qNA5_f7F8sJdt1pQGrt1nYP2`

## Setup
1. Open the Apps Script project and enable **Advanced Google Services** for `Drive API`.
2. In script editor, run `initializeScriptProperties()` once.
3. Open the spreadsheet bound to this script, then use menu: **Invoice Automation → Open Website**.
4. Fill in and save:
	- `SOURCE_FOLDER_IDS` (comma/newline-separated)
	- `PROCESSED_FOLDER_ID`
	- `TARGET_EMAIL`
	- `SHEET_ID`
	- `SHEET_NAME` (defaults to `Invoice Logger`)
	- `SEARCH_QUERY` (defaults to unread attachment inbox query)
	- `RUN_INTERVAL_MINUTES` (defaults to `15`)
5. Click **Create / Reset Triggers** from the web app.

## Web App tabs
- **Dashboard**: run processing actions, view last run summary, and monitor live feed.
- **Upload**: upload new invoices (`.pdf`, `.xlsx`, `.csv`, `.zip`) and optionally process immediately.
- **Mapping Studio**: create/edit/delete profile-based extraction rules, install one-click Arrive template, and preview rules against source files.
- **Configuration**: manage all runtime settings.

## UI actions
- Run Drive processing immediately
- Run both channels
- Create/reset time-based triggers
- Clean up old idempotency state records
- Upload PDFs, spreadsheets or zip archives directly
- Save and test manual extraction profiles

## Supported inputs

| Input | How it is handled |
| --- | --- |
| `.pdf` | Drive OCR to text, then field extraction and coding |
| `.xlsx` / `.xlsm` / `.xls` / `.csv` | Converted to a Google Sheet and read as data |
| `.zip` | Unpacked into the files above, each processed on its own |

### Archives

A zip dropped in a source folder (or attached to an email, or uploaded from the
web app) is expanded before anything else runs. Entries are renamed
`<archive> - <path inside archive>` so two archives containing `invoice.pdf`
stay distinguishable, and macOS `__MACOSX/` and `._*` sidecars are skipped.
Nested zips are followed three levels deep, up to 100 invoices per archive.
The archive is only trashed once every invoice inside it has been finalized —
if one entry is held for review, the archive stays put.

### Spreadsheet invoices

The line-item table is located by scoring each candidate header row against a
list of column synonyms, so the tab does not have to be called "Shifts" and the
headers do not have to be spelled a particular way. A workbook is read as:

- **one invoice** when there is no invoice-number column — every line is summed
- **many invoices** when there is one — lines are grouped by invoice number, and
  each invoice gets its own log rows and its own coding

Within each invoice, lines are still split by RDC coding, one log row per code.
Footer `Total` rows are ignored rather than double-counted, and `#REF!`/`#VALUE!`
cells are reported instead of quietly counting as zero. If the line items do not
match the total the workbook declares, the file is held for review with the
difference spelled out.

### Amounts

The invoice total is chosen by scoring every money-shaped number on the page
against the label in front of it — `Amount Due` and `Total Due` outrank a bare
`Total`, which outranks a line-item `Line Haul` or `Rate`, and numbers labelled
as weights, quantities, reference numbers or payment terms are rejected outright.
Credits (`(500.00)`, `500.00-`, `500.00 CR`) come through negative, and amounts
agreeing across several labels, or reconciling against the line items, raise
confidence. Anything that lands below `high` confidence is queued for review
rather than logged; set `HOLD_LOW_CONFIDENCE_AMOUNTS` to `false` in `code.js` to
log every amount as read instead.

Amounts are written to the log sheet as **numbers**, not text, so `SUM` and
pivot tables over the Amount column work.

## Tests

```bash
npm test
```

Runs the pure extraction logic against the sample invoices in this repo. See
[`test/README.md`](test/README.md).

## Notes
- Gmail ingestion is disabled by default in `ENABLE_GMAIL_INGESTION`.
- Drive files are moved to processed carrier subfolders only after successful (or already-complete) processing.
- Carrier subfolders are created automatically if they do not exist.
- Old processing state entries are cleaned automatically during runs.
- Mapping profiles are matched by `matchKeywords`; when matched, mapped values override regex-derived defaults.
- A payment `Due Date` is never read as a delivery date — it is driven by the payment terms, not the shipment.
- Carrier names are taken from the invoice letterhead (including `<Legal Entity> d/b/a` / `<Trading Name>`) before falling back to commodity keywords.
