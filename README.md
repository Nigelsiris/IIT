# IIT
Inbound Invoicing Tool

Google Apps Script automation for inbound freight invoice PDFs with a custom web interface.

## What it does
- Reads PDF invoices from configured Drive source folders
- OCRs and extracts invoice metadata
- Supports manual extraction mapping profiles for hard-to-parse invoice formats (like Arrive)
- Applies routing/coding rules
- Logs to a Google Sheet
- Sends an email with original PDF + generated coded summary PDF
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
	- `SEARCH_QUERY` (defaults to unread PDF inbox query)
	- `RUN_INTERVAL_MINUTES` (defaults to `15`)
5. Click **Create / Reset Triggers** from the web app.

## Web App tabs
- **Dashboard**: run processing actions, view last run summary, and monitor live feed.
- **Upload**: upload new invoices and optionally process immediately.
- **Mapping Studio**: create/edit/delete profile-based extraction rules, install one-click Arrive template, and preview rules against source PDFs.
- **Configuration**: manage all runtime settings.

## UI actions
- Run Drive processing immediately
- Run both channels
- Create/reset time-based triggers
- Clean up old idempotency state records
- Upload PDFs directly
- Save and test manual extraction profiles

## Notes
- Gmail ingestion is disabled by default in `ENABLE_GMAIL_INGESTION`.
- Drive files are moved to processed carrier subfolders only after successful (or already-complete) processing.
- Carrier subfolders are created automatically if they do not exist.
- Old processing state entries are cleaned automatically during runs.
- Mapping profiles are matched by `matchKeywords`; when matched, mapped values override regex-derived defaults.
