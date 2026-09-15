# IIT
Inbound Invoicing Tool

Google Apps Script automation for inbound freight invoices — PDFs, Excel workbooks and zip archives — with a custom web interface.

## What it does
- Reads PDF, Excel (`.xlsx`/`.xls`/`.csv`) and **`.zip`** invoices from configured Drive source folders
- Unpacks zip archives (including nested ones) into their individual invoices before processing
- OCRs and extracts invoice metadata
- Supports manual extraction mapping profiles for hard-to-parse invoice formats (like Arrive)
- Separates inbound from outbound mail forwarded by a delegated mailbox, by reading the original sender/subject out of the forward
- Applies routing/coding rules
- Logs to a Google Sheet
- Reads spreadsheet invoices as data, including workbooks holding **many invoices, one per line**
- Reconciles spreadsheet line items against the total the workbook declares
- Flags invoices whose amount cannot be tied to an explicit label, and holds ones whose RDC coding could not be determined
- Emails the coded summary merged with the original invoice as a single PDF, sent as the delegated mailbox
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
	- `TARGET_EMAIL` (AP address)
	- `SEND_AS_ALIAS` (delegated mailbox to send as)
	- `SHEET_ID`
	- `SHEET_NAME` (defaults to `Invoice Logger`)
	- `RUN_INTERVAL_MINUTES` (defaults to `15`)
5. Click **Create / Reset Triggers** from the web app.

## Web App tabs
- **Dashboard**: run processing actions, view last run summary, and monitor live feed.
- **Upload**: upload new invoices (`.pdf`, `.xlsx`, `.csv`, `.zip`) and optionally process immediately.
- **Email Filters**: unwrap forwarded mail, edit inbound/outbound rules, and test them against real mail.
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

### Outgoing mail

Processed invoices are sent **as** the delegated mailbox and **to** AP:

| Setting | Default |
| --- | --- |
| `SEND_AS_ALIAS` | `logistics.invoices@lidl.us` |
| `TARGET_EMAIL` | `invoice@lus.costs.invoice.schwarz` |

The send-as address must be a **verified alias** on the account running the
script (Gmail → Settings → Accounts → "Send mail as"). If it is not, Gmail
ignores the `from` and sends as the account owner, so the alias is checked
against `getAliases()` before every send and a mismatch is reported in the
Configuration tab and the activity feed rather than quietly going out from the
wrong address. **Send Test Email** proves the setup without waiting for a batch.

A configuration that addressed invoices back to the account owner (or had no
recipient at all) is corrected once, automatically, and the change is recorded
in the feed.

### The merged PDF

The email carries **one** document: the coded summary page followed by the
original invoice, named `Coded_<invoice>.pdf`. If the merge fails the code
sheet and the original are sent as two attachments instead, with the reason in
the feed — better than sending half the paperwork.

The separate code-sheet/original pair is still written to the processed folder,
because the scheduled carrier merge pairs them there by filename.

### Amounts

The invoice total is chosen by scoring every money-shaped number on the page
against the label in front of it — `Amount Due` and `Total Due` outrank a bare
`Total`, which outranks a line-item `Line Haul` or `Rate`, and numbers labelled
as weights, quantities, reference numbers or payment terms are rejected outright.
Credits (`(500.00)`, `500.00-`, `500.00 CR`) come through negative, and amounts
agreeing across several labels, or reconciling against the line items, raise
confidence. Anything that lands below `high` confidence is flagged (see below)
rather than held back.

Amounts are written to the log sheet as **numbers**, not text, so `SUM` and
pivot tables over the Amount column work.

### What actually gets held back

Only two things stop an invoice reaching the ledger:

- **The carrier is not on the confirmed list** — it would be filed in the wrong place.
- **No RDC could be determined** — AP can correct a number, but they cannot guess
  which RDC a load belonged to.

A shaky *amount* does **not** hold the invoice. It goes out with the coding and
is flagged instead: `[CHECK AMOUNT]` in the subject, a red banner on the code
sheet naming the figure and the label it came from, and a note in the body. Set
`HOLD_LOW_CONFIDENCE_AMOUNTS` to `true` in `code.js` to hold on the amount too.

## Email filters

Gmail's native filters cannot separate these two, because both arrive `From:`
the same delegated mailbox:

```
logistics.invoices@lidl.us  ──forwards──▶  you    inbound carrier invoice  → process
logistics.invoices@lidl.us  ──forwards──▶  you    outbound freight         → ignore
```

The original sender, subject and recipient only exist *inside* the forwarded
body, where no Gmail rule can reach them. The **Email Filters** tab handles this
in two stages.

### 1. Forwards are unwrapped

List your delegated mailboxes (there is a "Suggest from recent mail" button that
finds them for you). When mail arrives through one of them, the forwarded
envelope is parsed — both the Gmail `---------- Forwarded message ---------`
block and the Outlook `From:/Sent:/To:/Subject:` block — and these fields become
available to rules:

| Field | What it holds |
| --- | --- |
| `effectiveFrom` / `effectiveSubject` | The **original** sender/subject on a forward, the message's own on anything else. Usually what you want. |
| `originalFrom` / `originalSubject` | Only what was found inside the forward |
| `from` / `subject` | What Gmail sees — the delegated mailbox and `Fwd: …` |
| `deliveredTo`, `replyTo`, `cc`, `forwardedFrom` | Routing, including which delegated box relayed it |
| `body`, `attachmentName`, `any` | Full text, each attachment name, everything at once |

### 2. Rules decide, top to bottom

Rules are checked in order and **the first one that matches wins** — nothing
below it is considered. Each rule is an action (Process / Skip), a field, a test
(`contains`, `does not contain`, `is exactly`, `starts with`, `ends with`,
`matches regex`, `domain is`, `is empty`, `is not empty`) and a value. Reorder
them with the arrows; the tester always names the single rule that decided.

So the case above is one rule above another:

| # | Action | Field | Test | Value |
| --- | --- | --- | --- | --- |
| 1 | Skip | Subject (through forward) | contains | `outbound` |
| 2 | Process | Subject (through forward) | contains | `inbound` |

When **no** rule matches, the default action applies. It ships as **Queue for
review**: the attachments are saved to the source folder and put in the Review
tab, so an unclassified message is never silently dropped and never posted on a
guess. It can be set to Process or Skip instead.

Skipped messages are left **unread** by default, so a rule that is too broad is
easy to notice and undo.

### Choosing what Gmail returns

The tab also builds the Gmail search itself — attachments, unread, sender,
`deliveredto:` (which catches a delegated forward without needing any label at
all), subject, labels and an age limit — and shows the resulting query live.
Labels are optional. There is a raw-query mode if you would rather write it
yourself.

### Testing before you commit

**Test Current Search** runs the rules as they are on screen, saved or not,
against real mail and shows what each message would do and why. **Test All
Recent Mail** ignores your search settings and looks at everything with an
attachment, so you can see what is currently being *missed* rather than only
what already gets through.

Your existing `SEARCH_QUERY` is migrated into these settings the first time the
tab loads, with the default action set to Process and the content rules left
off, so upgrading does not change what gets processed until you choose to.

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
