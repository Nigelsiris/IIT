const PROPERTY_KEYS = {
  SEARCH_QUERY: 'SEARCH_QUERY',
  SOURCE_FOLDERS: 'SOURCE_FOLDER_IDS',
  PROCESSED_FOLDER_ID: 'PROCESSED_FOLDER_ID',
  CARRIER_TYPE_FIXES: 'CARRIER_TYPE_FIXES',
  TARGET_EMAIL: 'TARGET_EMAIL',
  SHEET_ID: 'SHEET_ID',
  SHEET_NAME: 'SHEET_NAME',
  RUN_INTERVAL_MINUTES: 'RUN_INTERVAL_MINUTES',
  LAST_RUN_SUMMARY: 'LAST_RUN_SUMMARY',
  EXTRACTION_MAPPINGS: 'EXTRACTION_MAPPINGS',
  PROCESSING_FEED: 'PROCESSING_FEED',
  MERGE_WATCH_STATE: 'MERGE_WATCH_STATE',
  CONFIRMED_CARRIERS: 'CONFIRMED_CARRIERS',
  EMAIL_FILTERS: 'EMAIL_FILTERS',
  SEND_AS_ALIAS: 'SEND_AS_ALIAS',
  SEND_AS_NAME: 'SEND_AS_NAME'
};

const REVIEW_PREFIX = 'REVIEW_';
const LEARN_KEY = 'LEARNING_LEDGER';
const LEARN_MAX_ENTRIES = 500;

const DEFAULT_FOLDER_IDS = {
  SOURCE_FOLDER_ID: '1YjOhlN1gautM0BIFPg0QAmTtc_DLViSt',
  PROCESSED_FOLDER_ID: '1tZe3fMH1s9cl3EMijdqhecXpgY6n-scr',
  MERGED_OUTPUT_PARENT_FOLDER_ID: '1suhe1rYmaAVtE0Ul4BxEn5DXySO7B4G8'
};

const DEFAULT_TRACKER_SHEET_ID = '1dCjBTgYL9yPzyZd1PNM4B-R6Lv2SnOcDSzrQizV8XAg';
const POST_PROCESS_MERGE_TRIGGER = 'runScheduledProcessedCarrierMerge';
const MERGE_WATCH_TRIGGER = 'monitorProcessedFolderForMerge';
const MERGE_WATCH_DEBOUNCE_MS = 2 * 60 * 1000;
const MERGE_RUN_INTERVAL_MINUTES = 5;

const ENABLE_GMAIL_INGESTION = true;

const DEFAULTS = {
  SEARCH_QUERY: 'has:attachment is:unread label:Automation-Emails/ITBP',
  SOURCE_FOLDERS: DEFAULT_FOLDER_IDS.SOURCE_FOLDER_ID,
  PROCESSED_FOLDER_ID: DEFAULT_FOLDER_IDS.PROCESSED_FOLDER_ID,
  CARRIER_TYPE_FIXES: '',
  // Processed invoices go to AP, sent as the delegated mailbox — not as
  // whoever owns the script.
  TARGET_EMAIL: 'invoice@lus.costs.invoice.schwarz',
  SEND_AS_ALIAS: 'logistics.invoices@lidl.us',
  SEND_AS_NAME: 'Inbound Invoicing',
  SHEET_ID: '',
  SHEET_NAME: 'Invoice Logger',
  RUN_INTERVAL_MINUTES: '15'
};

const MAPPING_FIELDS = [
  'invoiceNumber',
  'po',
  'shipDate',
  'deliveryDate',
  'amount',
  'origin',
  'destination',
  'productType',
  'remitInfo',
  'carrierType',
  'appliedCoding'
];

const FEED_LIMIT = 200;
const MAX_INVOICES_PER_RUN = 20;
const MAX_BATCH_RUN_MS = 5 * 60 * 1000;
const MISSING_VALUE_LABEL = 'See Below';
// An invoice is held only when its CODING could not be resolved — that is the
// part a person has to fix. A shaky amount is sent through with the coding and
// flagged loudly instead, because AP can correct a number but cannot guess an
// RDC. Set to true to go back to holding on a low-confidence amount as well.
const HOLD_LOW_CONFIDENCE_AMOUNTS = false;
// The placeholder determineCoding() emits when no RDC keyword matched.
const UNRESOLVED_RDC_CODE = 'RDC-UNKNOWN';
const XLSX_MIME_TYPE = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet';
const GOOGLE_SHEETS_MIME_TYPE = 'application/vnd.google-apps.spreadsheet';

const ROUTING_RULES = {
  INVOICE_TYPE: '360100',
  rdcRules: [
    { keyword: 'pye', code: '70001' },
    { keyword: 'perryville', code: '70001' },
    { keyword: 'grm', code: '60001' },
    { keyword: 'graham', code: '60001' },
    { keyword: 'mebane', code: '60001' },
    { keyword: 'frg', code: '50001' },
    { keyword: 'fredericksburg', code: '50001' }
  ],
  catRules: [
    { keyword: 'beverages', code: 'CAT 1' },
    { keyword: 'polar', code: 'CAT 1' },
    { keyword: 'cg roxane', code: 'CAT 1' },
    { keyword: 'frizgerald', code: 'CAT 1' },
    { keyword: 'lassonde', code: 'CAT 1' },
    { keyword: 'independent beverage', code: 'CAT 1' },
    { keyword: 'premium water', code: 'CAT 1' },
    { keyword: 'h&s bakery', code: 'CAT 3' }
  ]
};

const PROCESSING_STATE = {
  PREFIX: 'PROC_STATE_',
  DONE: 'DONE',
  PROCESSING: 'PROCESSING',
  STALE_MS: 6 * 60 * 60 * 1000,
  RETENTION_DAYS: 60
};

const LOG_HEADERS = [
  'Timestamp',
  'Source',
  'File Name',
  'Invoice Number',
  'PO Number',
  'Ship Date',
  'Delivery Date',
  'Amount',
  'Origin',
  'Product Type',
  'Destination',
  'Remittance Info',
  'Applied Coding'
];

let invoiceSheetCache = null;
let emailFiltersCache = null;
let sendAsAliasCache = null;
let scriptOwnerCache = null;
const MAIL_CONFIG_REPAIR_FLAG = 'MAIL_CONFIG_REPAIRED_AT';

function onOpen() {
  try {
    SpreadsheetApp.getUi()
      .createMenu('Invoice Automation')
      .addItem('Open Control Panel', 'showControlPanel')
      .addItem('Open Website', 'showWebAppInDialog')
      .addSeparator()
      .addItem('Initialize Script Properties', 'initializeScriptProperties')
      .addItem('Create / Reset Triggers', 'createTriggers')
      .addSeparator()
      .addItem('Run Drive Processing Now', 'processDriveFolders')
      .addItem('Run All Processing Now', 'runAllProcessing')
      .addToUi();
  } catch (error) {
    Logger.log(`onOpen skipped: ${error.message}`);
  }
}

function showControlPanel() {
  const html = HtmlService.createHtmlOutputFromFile('Sidebar')
    .setTitle('Inbound Invoicing Tool');
  SpreadsheetApp.getUi().showSidebar(html);
}

function showWebAppInDialog() {
  const html = HtmlService.createHtmlOutputFromFile('WebApp')
    .setWidth(1200)
    .setHeight(800);
  SpreadsheetApp.getUi().showModalDialog(html, 'Inbound Invoicing Tool');
}

function doGet() {
  return HtmlService.createHtmlOutputFromFile('WebApp')
    .setTitle('Inbound Invoicing Tool');
}

function getUiConfig() {
  const properties = PropertiesService.getScriptProperties();
  return {
    // Read-only here: the Email Filters tab owns this now.
    searchQuery: buildGmailSearchQuery(getEmailFilters().fetch),
    sourceFolders: properties.getProperty(PROPERTY_KEYS.SOURCE_FOLDERS) || DEFAULTS.SOURCE_FOLDERS,
    processedFolderId: properties.getProperty(PROPERTY_KEYS.PROCESSED_FOLDER_ID) || DEFAULTS.PROCESSED_FOLDER_ID,
    carrierTypeFixes: properties.getProperty(PROPERTY_KEYS.CARRIER_TYPE_FIXES) || DEFAULTS.CARRIER_TYPE_FIXES,
    confirmedCarriers: properties.getProperty(PROPERTY_KEYS.CONFIRMED_CARRIERS) || '',
    targetEmail: properties.getProperty(PROPERTY_KEYS.TARGET_EMAIL) || DEFAULTS.TARGET_EMAIL,
    sendAsAlias: properties.getProperty(PROPERTY_KEYS.SEND_AS_ALIAS) || DEFAULTS.SEND_AS_ALIAS,
    sendAsName: properties.getProperty(PROPERTY_KEYS.SEND_AS_NAME) || DEFAULTS.SEND_AS_NAME,
    sheetId: properties.getProperty(PROPERTY_KEYS.SHEET_ID) || '',
    sheetName: properties.getProperty(PROPERTY_KEYS.SHEET_NAME) || DEFAULTS.SHEET_NAME,
    runIntervalMinutes: properties.getProperty(PROPERTY_KEYS.RUN_INTERVAL_MINUTES) || DEFAULTS.RUN_INTERVAL_MINUTES,
    lastRun: getLastRunSummary()
  };
}

function saveUiConfig(payload) {
  const properties = PropertiesService.getScriptProperties();
  const runIntervalMinutes = String(Number(payload.runIntervalMinutes) || Number(DEFAULTS.RUN_INTERVAL_MINUTES));
  properties.setProperty(PROPERTY_KEYS.SOURCE_FOLDERS, (payload.sourceFolders || DEFAULTS.SOURCE_FOLDERS).trim());
  properties.setProperty(PROPERTY_KEYS.PROCESSED_FOLDER_ID, (payload.processedFolderId || DEFAULTS.PROCESSED_FOLDER_ID).trim());
  properties.setProperty(PROPERTY_KEYS.CARRIER_TYPE_FIXES, (payload.carrierTypeFixes || DEFAULTS.CARRIER_TYPE_FIXES).trim());
  properties.setProperty(PROPERTY_KEYS.CONFIRMED_CARRIERS, (payload.confirmedCarriers || '').trim());
  properties.setProperty(PROPERTY_KEYS.TARGET_EMAIL, (payload.targetEmail || DEFAULTS.TARGET_EMAIL).trim());
  properties.setProperty(PROPERTY_KEYS.SEND_AS_ALIAS, (payload.sendAsAlias || '').trim());
  properties.setProperty(PROPERTY_KEYS.SEND_AS_NAME, (payload.sendAsName || DEFAULTS.SEND_AS_NAME).trim());
  clearSendAsAliasCache();
  properties.setProperty(PROPERTY_KEYS.SHEET_ID, (payload.sheetId || '').trim());
  properties.setProperty(PROPERTY_KEYS.SHEET_NAME, (payload.sheetName || DEFAULTS.SHEET_NAME).trim());
  properties.setProperty(PROPERTY_KEYS.RUN_INTERVAL_MINUTES, runIntervalMinutes);
  invoiceSheetCache = null;
  return {
    ok: true,
    message: 'Configuration saved.',
    config: getUiConfig()
  };
}

function initializeScriptProperties() {
  clearEmailFiltersCache();
  const properties = PropertiesService.getScriptProperties();
  Object.keys(DEFAULTS).forEach(key => {
    const propertyKey = PROPERTY_KEYS[key];
    if (!properties.getProperty(propertyKey)) {
      properties.setProperty(propertyKey, DEFAULTS[key]);
    }
  });
  return {
    ok: true,
    message: 'Default script properties initialized.',
    config: getUiConfig()
  };
}

function uiRunDriveProcessing() {
  return runUiAction(() => queueDriveFoldersForReview());
}

function uiRunGmailProcessing() {
  return runUiAction(() => processIncomingPDFs());
}

function uiRunAllProcessing() {
  return runUiAction(() => runAllProcessing());
}

function uiCreateTriggers() {
  return runUiAction(() => createTriggers());
}

function uiCleanupProcessedState() {
  return runUiAction(() => {
    const deleted = cleanupProcessedState(PROCESSING_STATE.RETENTION_DAYS);
    return { deleted, message: `Removed ${deleted} old processing records.` };
  });
}

function getWebAppBootstrapData() {
  ensureMailConfiguration();
  return {
    config: getUiConfig(),
    mappings: getExtractionMappings(),
    sourceFiles: listSourcePdfFiles(100),
    lastRun: getLastRunSummary(),
    feed: getProcessingFeed(120),
    reviewQueue: getAllReviewItems(),
    emailFilters: webGetEmailFilters(),
    mailStatus: webGetMailStatus()
  };
}

function webSaveConfig(payload) {
  return saveUiConfig(payload);
}

function webInitializeDefaults() {
  return initializeScriptProperties();
}

function webCreateTriggers() {
  return uiCreateTriggers();
}

function webRunDriveProcessing() {
  return uiRunDriveProcessing();
}

function webRunAllProcessing() {
  return uiRunAllProcessing();
}

function webCleanupProcessedState() {
  return uiCleanupProcessedState();
}

function webGetLiveState() {
  return {
    feed: getProcessingFeed(120),
    lastRun: getLastRunSummary(),
    sourceFiles: listSourcePdfFiles(100),
    reviewQueue: getAllReviewItems()
  };
}

function webUploadInvoiceFile(payload) {
  try {
    const config = getConfig();
    assertRequiredConfig(config, ['SOURCE_FOLDERS']);
    const sourceFolder = DriveApp.getFolderById(config.SOURCE_FOLDERS[0]);
    const fileName = (payload.fileName || `invoice-${Date.now()}.pdf`).trim();
    const mimeType = payload.mimeType || mimeTypeForFileName(fileName);
    const bytes = Utilities.base64Decode(payload.base64Data || '');
    const blob = Utilities.newBlob(bytes, mimeType, fileName);
    const created = sourceFolder.createFile(blob);

    appendProcessingFeed('upload', `Uploaded ${fileName} to source folder.`, {
      fileId: created.getId(),
      fileName,
      sourceFolderId: sourceFolder.getId()
    });

    // A zip becomes the invoices it contains; everything after this point works
    // on that list, so uploading an archive behaves like uploading its files.
    let targetFiles = [created];
    let archiveInfo = null;
    if (isArchiveMimeType(mimeType, fileName)) {
      const expansion = expandArchiveFileInPlace(created, sourceFolder);
      archiveInfo = {
        expanded: expansion.created.length,
        skipped: expansion.skipped,
        error: expansion.error
      };
      if (expansion.created.length === 0) {
        return {
          ok: false,
          error: expansion.error
            ? `Could not expand ${fileName}: ${expansion.error}`
            : `${fileName} contained no PDF or spreadsheet invoices.`,
          archive: archiveInfo,
          sourceFiles: listSourcePdfFiles(100),
          feed: getProcessingFeed(120)
        };
      }
      targetFiles = expansion.created;
    }

    const processedResults = [];
    const reviewItems = [];
    targetFiles.forEach(function(targetFile) {
      if (payload.action === 'review') {
        const reviewResult = webExtractForReview(targetFile.getId());
        if (reviewResult.ok && reviewResult.reviewItem) {
          reviewItems.push(reviewResult.reviewItem);
        }
      } else if (payload.action === 'process' || payload.processNow) {
        processedResults.push(processSpecificDriveFile(targetFile.getId()));
      }
    });

    const primaryFile = targetFiles[0];
    return {
      ok: true,
      file: {
        id: primaryFile.getId(),
        name: primaryFile.getName(),
        url: primaryFile.getUrl(),
        size: primaryFile.getSize(),
        updatedAt: primaryFile.getLastUpdated().toISOString()
      },
      files: targetFiles.map(function(item) {
        return { id: item.getId(), name: item.getName(), url: item.getUrl(), size: item.getSize() };
      }),
      archive: archiveInfo,
      processed: processedResults.length === 1 ? processedResults[0] : (processedResults.length ? processedResults : null),
      reviewItem: reviewItems.length ? reviewItems[0] : null,
      reviewItems: reviewItems,
      sourceFiles: listSourcePdfFiles(100),
      feed: getProcessingFeed(120),
      reviewQueue: getAllReviewItems()
    };
  } catch (error) {
    appendProcessingFeed('error', `Upload failed: ${error.message}`, {});
    return { ok: false, error: error.message };
  }
}

function webSaveMappingProfile(profile) {
  const saved = saveExtractionMappingProfile(profile);
  return {
    ok: true,
    profile: saved,
    mappings: getExtractionMappings()
  };
}

function webDeleteMappingProfile(profileId) {
  deleteExtractionMappingProfile(profileId);
  return {
    ok: true,
    mappings: getExtractionMappings()
  };
}

function webPreviewMappingProfile(profile, fileId) {
  try {
    const text = extractTextFromDriveFile(fileId);
    const values = extractValuesByProfile(text, sanitizeMappingProfile(profile));
    return {
      ok: true,
      values,
      textSnippet: text.slice(0, 5000)
    };
  } catch (error) {
    return { ok: false, error: error.message };
  }
}

function webInstallArriveTemplateProfile() {
  const profile = createOrUpdateArriveTemplateProfile();
  return {
    ok: true,
    profile,
    mappings: getExtractionMappings()
  };
}

function webExtractForReview(fileId) {
  try {
    const config = getConfig();
    const file = DriveApp.getFileById(fileId);
    const fileName = file.getName();
    const pdfBlob = file.getBlob();

    appendProcessingFeed('info', 'Extracting for review: ' + fileName, { fileId: fileId });

    // Spreadsheets are parsed as data, not OCR'd as pictures of text.
    if (isSpreadsheetInvoiceMimeType(file.getMimeType(), fileName)) {
      return extractSpreadsheetForReview(file, fileName, config);
    }

    if (isArchiveMimeType(file.getMimeType(), fileName)) {
      return { ok: false, error: 'Archives are unpacked before review — run a Drive pass, or upload the .zip from the Upload tab.' };
    }

    const extractedText = extractTextFromPdfBlob(pdfBlob, fileName);
    const parsedInvoiceData = extractInvoiceData(extractedText, fileName);
    const mappingResult = applyMappedExtraction(extractedText, fileName);
    const preLearnData = mergeInvoiceData(parsedInvoiceData, mappingResult.values);
    const learnedCorrections = applyLearnedCorrections(preLearnData, extractedText);
    var invoiceData = Object.assign({}, preLearnData);
    var learnedFields = [];
    Object.keys(learnedCorrections).forEach(function(field) {
      if (learnedCorrections[field]) {
        invoiceData[field] = learnedCorrections[field];
        learnedFields.push(field);
      }
    });
    const appliedCoding = mappingResult.values.appliedCoding || determineCoding(extractedText);
    const carrierType = applyCarrierTypeAutoFix(mappingResult.values.carrierType
      ? normalizeCarrierType(mappingResult.values.carrierType)
      : determineCarrierType(extractedText, invoiceData, appliedCoding, fileName), config);

    const reviewId = 'rv-' + Date.now() + '-' + fileId.slice(0, 8);
    const reviewItem = {
      reviewId: reviewId,
      fileId: fileId,
      fileName: fileName,
      source: 'Manual Review',
      ocrText: extractedText.slice(0, 4000),
      extractedData: invoiceData,
      appliedCoding: appliedCoding,
      carrierType: carrierType,
      profileName: mappingResult.profileName || null,
      learnedFields: learnedFields,
      createdAt: new Date().toISOString()
    };

    saveReviewItem(reviewItem);
    appendProcessingFeed('info', 'Queued for review: ' + fileName, { reviewId: reviewId });

    return {
      ok: true,
      reviewItem: reviewItem,
      reviewQueue: getAllReviewItems()
    };
  } catch (error) {
    appendProcessingFeed('error', 'Review extraction failed: ' + error.message, { fileId: fileId });
    return { ok: false, error: error.message };
  }
}

/**
 * Queue a spreadsheet invoice for review without running it through OCR.
 *
 * A multi-invoice workbook is shown as a single review item summarising every
 * invoice it holds — approving it is a decision about the file, and the split
 * into per-invoice log rows happens at processing time.
 */
function extractSpreadsheetForReview(file, fileName, config) {
  const spreadsheetData = extractSpreadsheetInvoiceData(file.getBlob(), fileName, 'Manual Review');
  const invoices = spreadsheetData.invoices || [];
  const reconciliation = spreadsheetData.reconciliation || { issues: [], needsReview: false };
  const primary = invoices.length === 1 ? invoices[0].invoiceData : null;
  const grandTotal = invoices.reduce(function(sum, invoice) {
    return sum + Number(invoice.totalAmount || 0);
  }, 0);

  const extractedData = primary || {
    invoiceNumber: `${invoices.length} invoices in this workbook`,
    po: summarizeValues(invoices.map(function(invoice) { return invoice.invoiceNumber; }), 8),
    shipDate: MISSING_VALUE_LABEL,
    deliveryDate: MISSING_VALUE_LABEL,
    amount: formatAmountNumber(grandTotal),
    amountValue: grandTotal,
    amountConfidence: reconciliation.needsReview ? 'low' : 'high',
    origin: 'Review Required',
    destination: 'Review Required',
    productType: 'Spreadsheet Invoice',
    remitInfo: `Multi-invoice workbook — ${invoices.length} invoices, ${spreadsheetData.logRows.length} coding row(s)`
  };

  const reviewId = 'rv-' + Date.now() + '-' + file.getId().slice(0, 8);
  const reviewItem = {
    reviewId: reviewId,
    fileId: file.getId(),
    fileName: fileName,
    source: 'Manual Review',
    ocrText: [
      `Sheet: ${spreadsheetData.sheetName}`,
      `Invoices found: ${invoices.length}`,
      `Line total: ${formatAmountNumber(grandTotal)}`,
      reconciliation.declaredTotal === null ? '' : `Declared total: ${formatAmountNumber(reconciliation.declaredTotal)}`,
      reconciliation.issues.length ? `Issues: ${reconciliation.issues.join('; ')}` : 'Totals reconcile.'
    ].filter(Boolean).join('\n'),
    extractedData: extractedData,
    appliedCoding: spreadsheetData.codingSummary,
    carrierType: spreadsheetData.carrierType,
    profileName: 'spreadsheet',
    invoiceCount: invoices.length,
    reconciliation: reconciliation,
    heldReason: reconciliation.needsReview ? 'total_mismatch' : null,
    createdAt: new Date().toISOString()
  };

  saveReviewItem(reviewItem);
  appendProcessingFeed('info', `Queued spreadsheet for review: ${fileName} (${invoices.length} invoice(s))`, {
    reviewId: reviewId,
    invoiceCount: invoices.length,
    issues: reconciliation.issues
  });

  return { ok: true, reviewItem: reviewItem, reviewQueue: getAllReviewItems() };
}


function webGetReviewQueue() {
  return { ok: true, reviewQueue: getAllReviewItems() };
}

function webApproveReview(reviewId, editedData) {
  try {
    const item = getReviewItem(reviewId);
    if (!item) {
      return { ok: false, error: 'Review item not found.' };
    }

    const config = getConfig();
    assertRequiredConfig(config, ['PROCESSED_FOLDER_ID', 'SHEET_ID', 'TARGET_EMAIL']);

    const invoiceData = {
      invoiceNumber: editedData.invoiceNumber || item.extractedData.invoiceNumber || MISSING_VALUE_LABEL,
      po: editedData.po || item.extractedData.po || MISSING_VALUE_LABEL,
      shipDate: editedData.shipDate || item.extractedData.shipDate || MISSING_VALUE_LABEL,
      deliveryDate: editedData.deliveryDate || item.extractedData.deliveryDate || MISSING_VALUE_LABEL,
      amount: normalizeAmountText(editedData.amount) ||
        normalizeAmountText(item.extractedData.amount) ||
        MISSING_VALUE_LABEL,
      origin: editedData.origin || item.extractedData.origin || 'Review Required',
      destination: editedData.destination || item.extractedData.destination || 'Review Required',
      productType: editedData.productType || item.extractedData.productType || 'Review Required',
      remitInfo: editedData.remitInfo || item.extractedData.remitInfo || MISSING_VALUE_LABEL
    };

    // A reviewer-confirmed amount is authoritative from here on.
    invoiceData.amountValue = parseMoneyToken(invoiceData.amount);
    invoiceData.amountConfidence = 'confirmed';

    // --- Record corrections for the learning engine ---
    recordCorrections(item, invoiceData, editedData);
    const appliedCoding = editedData.appliedCoding || item.appliedCoding;
    const carrierType = applyCarrierTypeAutoFix(editedData.carrierType || item.carrierType, config);

    const sheet = getInvoiceSheet(config);
    sheet.appendRow([
      new Date(),
      item.source,
      item.fileName,
      invoiceData.invoiceNumber,
      invoiceData.po,
      invoiceData.shipDate,
      invoiceData.deliveryDate,
      sheetAmountValue(invoiceData),
      invoiceData.origin,
      invoiceData.productType,
      invoiceData.destination,
      invoiceData.remitInfo,
      appliedCoding
    ]);

    const generatedCodedPdfBlob = generateHtmlPdf(invoiceData, appliedCoding, item.fileName);

    // Items held from a Gmail attachment have no Drive file behind them; the
    // code sheet still goes out, there is just no original to re-attach.
    const file = item.fileId ? DriveApp.getFileById(item.fileId) : null;
    const pdfBlob = file ? file.getBlob() : null;
    const mergePairId = buildMergePairId(item.reviewId || item.fileId || item.fileName);
    const outputBlobs = pdfBlob
      ? buildOutputBlobs(carrierType, item.fileName, generatedCodedPdfBlob, pdfBlob, mergePairId)
      : buildOutputBlobs(carrierType, item.fileName, generatedCodedPdfBlob, null, mergePairId);

    const emailPackage = buildInvoiceEmailAttachments(
      invoiceData, appliedCoding, generatedCodedPdfBlob, pdfBlob, item.fileName, outputBlobs
    );

    sendInvoiceEmail(config, {
      fileName: item.fileName,
      subject: buildInvoiceEmailSubject(item.fileName, invoiceData),
      body: buildInvoiceEmailBody(invoiceData, appliedCoding, emailPackage),
      attachments: emailPackage.attachments
    });

    const processedFolder = DriveApp.getFolderById(config.PROCESSED_FOLDER_ID);
    outputBlobs.forEach(function(blob) {
      processedFolder.createFile(blob);
    });
    if (file) {
      file.setTrashed(true);
    }

    deleteReviewItem(reviewId);
    appendProcessingFeed('success', 'Approved & finalized: ' + item.fileName, {
      reviewId: reviewId,
      carrierType: carrierType,
      mergePairId: mergePairId,
      outputFiles: outputBlobs.map(function(blob) { return blob.getName(); })
    });

    return {
      ok: true,
      message: 'Invoice approved and finalized.',
      reviewQueue: getAllReviewItems(),
      feed: getProcessingFeed(120)
    };
  } catch (error) {
    appendProcessingFeed('error', 'Approve failed: ' + error.message, { reviewId: reviewId });
    return { ok: false, error: error.message };
  }
}

function webRejectReview(reviewId) {
  const item = getReviewItem(reviewId);
  deleteReviewItem(reviewId);
  appendProcessingFeed('info', 'Review rejected: ' + (item ? item.fileName : reviewId), { reviewId: reviewId });
  return { ok: true, reviewQueue: getAllReviewItems() };
}

function webOcrSourceFile(fileId) {
  try {
    const text = extractTextFromDriveFile(fileId);
    const file = DriveApp.getFileById(fileId);
    return {
      ok: true,
      text: text,
      fileId: fileId,
      fileName: file.getName()
    };
  } catch (error) {
    return { ok: false, error: error.message };
  }
}

function getAllReviewItems() {
  const properties = PropertiesService.getScriptProperties().getProperties();
  const items = [];
  Object.keys(properties).forEach(function(key) {
    if (!key.startsWith(REVIEW_PREFIX)) {
      return;
    }
    try {
      items.push(JSON.parse(properties[key]));
    } catch (error) {
      // skip corrupt entries
    }
  });
  return items.sort(function(a, b) {
    return new Date(b.createdAt).getTime() - new Date(a.createdAt).getTime();
  });
}

function saveReviewItem(item) {
  var key = REVIEW_PREFIX + item.reviewId;
  PropertiesService.getScriptProperties().setProperty(key, JSON.stringify(item));
}

function getReviewItem(reviewId) {
  var raw = PropertiesService.getScriptProperties().getProperty(REVIEW_PREFIX + reviewId);
  if (!raw) {
    return null;
  }
  try {
    return JSON.parse(raw);
  } catch (error) {
    return null;
  }
}

function deleteReviewItem(reviewId) {
  PropertiesService.getScriptProperties().deleteProperty(REVIEW_PREFIX + reviewId);
}

/* ═══════════════════════════════════════════════════════
   LEARNING ENGINE — learns from user corrections
   ═══════════════════════════════════════════════════════ */

/**
 * Load the full learning ledger from Script Properties.
 * Returns an array of correction records.
 */
function getLearningLedger() {
  var raw = PropertiesService.getScriptProperties().getProperty(LEARN_KEY);
  if (!raw) return [];
  try { return JSON.parse(raw); } catch (e) { return []; }
}

/**
 * Persist the learning ledger (capped at LEARN_MAX_ENTRIES).
 */
function saveLearningLedger(ledger) {
  var trimmed = ledger.slice(-LEARN_MAX_ENTRIES);
  PropertiesService.getScriptProperties().setProperty(LEARN_KEY, JSON.stringify(trimmed));
}

/**
 * Called when a review is approved. Compares extracted vs. user-edited values
 * and stores any corrections along with OCR context fingerprints.
 */
function recordCorrections(reviewItem, finalData, editedData) {
  var ledger = getLearningLedger();
  var ocrText = (reviewItem.ocrText || '').toLowerCase();
  var profileName = reviewItem.profileName || 'unknown';

  // Build a small fingerprint of the OCR (first 200 chars) for fuzzy matching
  var ocrFingerprint = ocrText.slice(0, 200).replace(/\s+/g, ' ').trim();

  MAPPING_FIELDS.forEach(function(field) {
    if (field === 'carrierType' || field === 'appliedCoding') return;

    var extracted = (reviewItem.extractedData && reviewItem.extractedData[field]) || '';
    var userValue = (editedData && editedData[field]) || '';
    var finalValue = finalData[field] || '';

    // Normalize for comparison
    var extTrimmed = String(extracted).trim();
    var finalTrimmed = String(finalValue).trim();

    // Only record if the user actually changed the value
    if (!finalTrimmed || finalTrimmed === extTrimmed) return;
    // Don't record placeholder values
    if (/^(Not Found|See Below|Review Required|--|N\/A)$/i.test(finalTrimmed)) return;

    // Look for a contextual anchor — the 60 chars surrounding where the value might appear in OCR
    var anchor = '';
    var idx = ocrText.indexOf(finalTrimmed.toLowerCase());
    if (idx >= 0) {
      anchor = ocrText.slice(Math.max(0, idx - 30), idx + finalTrimmed.length + 30).replace(/\s+/g, ' ').trim();
    }

    ledger.push({
      field: field,
      extractedValue: extTrimmed,
      correctedValue: finalTrimmed,
      profileName: profileName,
      ocrFingerprint: ocrFingerprint,
      ocrAnchor: anchor,
      fileName: reviewItem.fileName || '',
      ts: new Date().toISOString()
    });
  });

  saveLearningLedger(ledger);
}

/**
 * Given current extraction results and OCR text, check the learning ledger
 * for applicable corrections. Uses multiple matching strategies:
 *   1. Exact extracted-value match with same profile → strongest signal
 *   2. OCR anchor match → value appeared in similar context before
 *   3. OCR fingerprint similarity → same type of document
 * Returns an object of { field: correctedValue } for fields that should be overridden.
 */
function applyLearnedCorrections(extractedData, ocrText) {
  var ledger = getLearningLedger();
  if (ledger.length === 0) return {};

  var corrections = {};
  var ocrLower = (ocrText || '').toLowerCase();
  var ocrFP = ocrLower.slice(0, 200).replace(/\s+/g, ' ').trim();

  MAPPING_FIELDS.forEach(function(field) {
    if (field === 'carrierType' || field === 'appliedCoding') return;

    var currentValue = String((extractedData && extractedData[field]) || '').trim();
    // Only try to correct bad/missing values
    if (currentValue && !/^(Not Found|See Below|Review Required|--|N\/A)$/i.test(currentValue)) {
      // Already has a real value — check if we've seen this exact bad value corrected before
      var exactMatches = ledger.filter(function(entry) {
        return entry.field === field && entry.extractedValue === currentValue;
      });
      if (exactMatches.length >= 2) {
        // Same bad value was corrected to the same thing multiple times = confident correction
        var correctionCounts = {};
        exactMatches.forEach(function(m) {
          correctionCounts[m.correctedValue] = (correctionCounts[m.correctedValue] || 0) + 1;
        });
        var best = null;
        var bestCount = 0;
        Object.keys(correctionCounts).forEach(function(val) {
          if (correctionCounts[val] > bestCount) {
            bestCount = correctionCounts[val];
            best = val;
          }
        });
        if (best && bestCount >= 2) {
          corrections[field] = best;
        }
      }
      return;
    }

    // Value is missing/bad — try to fill from learned data
    // Strategy: find corrections for this field where the OCR anchor text appears in our OCR
    var anchorMatches = ledger.filter(function(entry) {
      return entry.field === field && entry.ocrAnchor && entry.ocrAnchor.length > 10 &&
        ocrLower.indexOf(entry.ocrAnchor) >= 0;
    });
    if (anchorMatches.length > 0) {
      // Use the most recent anchor match
      corrections[field] = anchorMatches[anchorMatches.length - 1].correctedValue;
      return;
    }

    // Fallback: fingerprint similarity — same document header pattern
    var fpMatches = ledger.filter(function(entry) {
      return entry.field === field && entry.ocrFingerprint &&
        ocrFPSimilarity(entry.ocrFingerprint, ocrFP) > 0.6;
    });
    if (fpMatches.length >= 2) {
      // If several fingerprint-similar docs had the same correction, apply it
      var fpCounts = {};
      fpMatches.forEach(function(m) {
        fpCounts[m.correctedValue] = (fpCounts[m.correctedValue] || 0) + 1;
      });
      var fpBest = null;
      var fpBestCount = 0;
      Object.keys(fpCounts).forEach(function(val) {
        if (fpCounts[val] > fpBestCount) {
          fpBestCount = fpCounts[val];
          fpBest = val;
        }
      });
      if (fpBest && fpBestCount >= 2) {
        corrections[field] = fpBest;
      }
    }
  });

  return corrections;
}

/**
 * Simple bigram-based similarity between two fingerprint strings.
 * Returns 0.0 – 1.0.
 */
function ocrFPSimilarity(a, b) {
  if (!a || !b) return 0;
  function bigrams(str) {
    var set = {};
    for (var i = 0; i < str.length - 1; i++) {
      set[str.slice(i, i + 2)] = true;
    }
    return set;
  }
  var ba = bigrams(a);
  var bb = bigrams(b);
  var keysA = Object.keys(ba);
  var keysB = Object.keys(bb);
  if (keysA.length === 0 || keysB.length === 0) return 0;
  var intersection = 0;
  keysA.forEach(function(k) { if (bb[k]) intersection++; });
  return (2 * intersection) / (keysA.length + keysB.length);
}

/**
 * Web endpoint to retrieve learning stats for the UI.
 */
function webGetLearningStats() {
  var ledger = getLearningLedger();
  var fieldCounts = {};
  var profileCounts = {};
  ledger.forEach(function(entry) {
    fieldCounts[entry.field] = (fieldCounts[entry.field] || 0) + 1;
    profileCounts[entry.profileName] = (profileCounts[entry.profileName] || 0) + 1;
  });
  return {
    ok: true,
    totalCorrections: ledger.length,
    fieldCounts: fieldCounts,
    profileCounts: profileCounts,
    recentCorrections: ledger.slice(-20).reverse()
  };
}

/**
 * Web endpoint to clear the learning ledger.
 */
function webClearLearningData() {
  PropertiesService.getScriptProperties().deleteProperty(LEARN_KEY);
  return { ok: true, message: 'Learning data cleared.' };
}

function runUiAction(action) {
  try {
    return { ok: true, result: action(), lastRun: getLastRunSummary() };
  } catch (error) {
    return { ok: false, error: error.message, lastRun: getLastRunSummary() };
  }
}

function runAllProcessing() {
  ensureMailConfiguration();
  appendProcessingFeed('info', 'Starting runAllProcessing.', {});
  const driveSummary = processDriveFolders();
  const gmailSummary = ENABLE_GMAIL_INGESTION ? processIncomingPDFs() : { disabled: true, reason: 'Gmail ingestion disabled (folder-only mode).' };
  appendProcessingFeed('info', 'Completed runAllProcessing.', { drive: driveSummary, gmail: gmailSummary });
  return { drive: driveSummary, gmail: gmailSummary };
}

function queueDriveFoldersForReview() {
  const config = getConfig();
  assertRequiredConfig(config, ['SOURCE_FOLDERS']);
  appendProcessingFeed('info', 'Drive review queue run started.', { sourceFolders: config.SOURCE_FOLDERS });
  const archiveExpansion = expandArchivesInSourceFolders(config);

  const startedAt = Date.now();
  let handledCount = 0;
  let stoppedEarly = false;
  const existingFileIds = {};
  getAllReviewItems().forEach(function(item) {
    if (item && item.fileId) existingFileIds[item.fileId] = true;
  });

  const summary = {
    channel: 'drive-review',
    totalFound: 0,
    queued: 0,
    alreadyQueued: 0,
    failed: 0,
    archivesExpanded: archiveExpansion.archives,
    archiveEntriesFound: archiveExpansion.filesCreated
  };

  outer:
  for (let fi = 0; fi < config.SOURCE_FOLDERS.length; fi++) {
    const folder = DriveApp.getFolderById(config.SOURCE_FOLDERS[fi]);
    const files = folder.getFiles();

    while (files.hasNext()) {
      if (handledCount >= MAX_INVOICES_PER_RUN || (Date.now() - startedAt) >= MAX_BATCH_RUN_MS) {
        stoppedEarly = true;
        break outer;
      }

      const file = files.next();
      // Archives were already expanded into this folder above; anything still
      // flagged as one failed to unpack and must not be queued as an invoice.
      if (!isSupportedInvoiceFile(file)) {
        continue;
      }
      handledCount += 1;
      summary.totalFound += 1;

      if (existingFileIds[file.getId()]) {
        summary.alreadyQueued += 1;
        continue;
      }

      const reviewResult = webExtractForReview(file.getId());
      if (reviewResult && reviewResult.ok) {
        summary.queued += 1;
        existingFileIds[file.getId()] = true;
      } else {
        summary.failed += 1;
      }
    }
  }

  if (stoppedEarly) {
    summary.stoppedEarly = true;
    summary.batchLimit = MAX_INVOICES_PER_RUN;
    summary.handledThisRun = handledCount;
  }

  setLastRunSummary('drive-review', summary);
  appendProcessingFeed('info', 'Drive review queue run completed.', summary);
  return summary;
}

/**
 * Unpack a zip that is sitting in a Drive folder into that same folder, then
 * trash the archive.
 *
 * Doing this before anything else means the rest of the tool — the review
 * queue, the source-file list, the processing loop — only ever deals with real
 * Drive files. The alternative (expanding in memory) leaves review items with
 * no file to open and no original to attach.
 *
 * Returns { created: [File], skipped: [String], error: String|null }.
 */
function expandArchiveFileInPlace(file, folder) {
  const archiveName = file.getName();
  let expansion;
  try {
    expansion = expandInvoiceArchive(file.getBlob(), archiveName, 0);
  } catch (error) {
    appendProcessingFeed('error', `Could not expand ${archiveName}`, { error: error.message });
    return { created: [], skipped: [], error: error.message };
  }

  if (expansion.entries.length === 0) {
    appendProcessingFeed('warning', `Archive ${archiveName} contained no invoice files.`, {
      archiveName: archiveName,
      skipped: expansion.skipped.slice(0, 20)
    });
    return { created: [], skipped: expansion.skipped, error: null };
  }

  const created = [];
  const archiveBase = stripInvoiceExtension(archiveName);

  expansion.entries.forEach(function(entry) {
    try {
      // Prefix with the archive name so two zips holding "invoice.pdf" do not
      // become indistinguishable once unpacked side by side.
      const targetName = buildArchiveEntryFileName(archiveBase, entry.path);
      const createdFile = folder.createFile(entry.blob.setName(targetName));
      created.push(createdFile);
    } catch (error) {
      appendProcessingFeed('error', `Could not save ${entry.path} from ${archiveName}`, { error: error.message });
    }
  });

  if (created.length !== expansion.entries.length) {
    // Something failed to land — keep the archive so nothing is lost.
    appendProcessingFeed('warning', `Archive ${archiveName} kept: only ${created.length}/${expansion.entries.length} entries were saved.`, {
      archiveName: archiveName
    });
    return { created: created, skipped: expansion.skipped, error: 'partial_extraction' };
  }

  file.setTrashed(true);
  appendProcessingFeed('success', `Expanded ${archiveName} into ${created.length} invoice file(s).`, {
    archiveName: archiveName,
    files: created.map(function(item) { return item.getName(); }).slice(0, 20),
    skipped: expansion.skipped.slice(0, 20)
  });

  return { created: created, skipped: expansion.skipped, error: null };
}

/**
 * "Week 12.zip" + "north/invoice-1001.pdf" -> "Week 12 - north - invoice-1001.pdf"
 */
function buildArchiveEntryFileName(archiveBase, entryPath) {
  const cleanedPath = String(entryPath || 'invoice')
    .split('/')
    .filter(Boolean)
    .join(' - ');
  const name = `${archiveBase} - ${cleanedPath}`.replace(/[\\/:*?"<>|]+/g, ' ').replace(/\s+/g, ' ').trim();
  return name.length > 240 ? name.slice(name.length - 240) : name;
}

/**
 * Expand every archive sitting in the configured source folders. Run before a
 * processing or review pass so archives never reach the rest of the pipeline.
 */
function expandArchivesInSourceFolders(config) {
  const runtimeConfig = config || getConfig();
  const folderIds = runtimeConfig.SOURCE_FOLDERS || [];
  const result = { archives: 0, filesCreated: 0, failed: 0, deferred: 0 };
  const startedAt = Date.now();
  // Leave most of the run's budget for actually processing what we unpack.
  const expansionBudgetMs = Math.round(MAX_BATCH_RUN_MS / 2);

  for (let i = 0; i < folderIds.length; i++) {
    const folder = DriveApp.getFolderById(folderIds[i]);
    // Collect first: creating files while iterating the same folder can other-
    // wise hand the newly written entries straight back to the iterator.
    const archives = [];
    const iterator = folder.getFiles();
    while (iterator.hasNext()) {
      const file = iterator.next();
      if (isArchiveFile(file)) {
        archives.push(file);
      }
    }

    for (let a = 0; a < archives.length; a++) {
      if ((Date.now() - startedAt) >= expansionBudgetMs) {
        result.deferred += archives.length - a;
        appendProcessingFeed('info', `Deferred ${result.deferred} archive(s) to the next run to stay inside the runtime limit.`, {
          folderId: folderIds[i]
        });
        return result;
      }

      result.archives += 1;
      const expansion = expandArchiveFileInPlace(archives[a], folder);
      result.filesCreated += expansion.created.length;
      if (expansion.error) {
        result.failed += 1;
      }
    }
  }

  return result;
}


function processDriveFolders() {
  const config = getConfig();
  assertRequiredConfig(config, ['SOURCE_FOLDERS', 'PROCESSED_FOLDER_ID', 'SHEET_ID', 'TARGET_EMAIL']);
  ensureMailConfiguration();
  appendProcessingFeed('info', 'Drive processing started.', { sourceFolders: config.SOURCE_FOLDERS });
  const archiveExpansion = expandArchivesInSourceFolders(config);
  const startedAt = Date.now();
  let handledCount = 0;
  let stoppedEarly = false;

  const summary = {
    channel: 'drive',
    totalFound: 0,
    processed: 0,
    alreadyProcessed: 0,
    failed: 0,
    inProgressSkipped: 0,
    finalized: 0,
    summaryFilesCreated: 0,
    carrierFoldersCreated: 0,
    archivesExpanded: archiveExpansion.archives,
    archiveEntriesFound: archiveExpansion.filesCreated
  };

  const processedFolder = DriveApp.getFolderById(config.PROCESSED_FOLDER_ID);
  const carrierFolderCache = {};
  outer:
  for (let fi = 0; fi < config.SOURCE_FOLDERS.length; fi++) {
    const folder = DriveApp.getFolderById(config.SOURCE_FOLDERS[fi]);
    const files = folder.getFiles();

    while (files.hasNext()) {
      if (handledCount >= MAX_INVOICES_PER_RUN || (Date.now() - startedAt) >= MAX_BATCH_RUN_MS) {
        stoppedEarly = true;
        break outer;
      }
      const file = files.next();
      if (!isIngestibleInvoiceFile(file)) {
        continue;
      }
      summary.totalFound += 1;
      processDriveFileCore(file, `Drive Folder: ${folder.getName()}`, processedFolder, carrierFolderCache, config, summary);
      handledCount += 1;
    }
  }

  if (stoppedEarly) {
    summary.stoppedEarly = true;
    summary.batchLimit = MAX_INVOICES_PER_RUN;
    summary.handledThisRun = handledCount;
    appendProcessingFeed('info', 'Drive processing stopped early to keep runtime fast.', {
      handledThisRun: handledCount,
      batchLimit: MAX_INVOICES_PER_RUN
    });
  }

  cleanupProcessedState(PROCESSING_STATE.RETENTION_DAYS);
  setLastRunSummary('drive', summary);
  appendProcessingFeed('info', 'Drive processing completed.', summary);
  Logger.log(`Drive processing summary: ${JSON.stringify(summary)}`);
  return summary;
}

function processSpecificDriveFile(fileId) {
  const config = getConfig();
  assertRequiredConfig(config, ['PROCESSED_FOLDER_ID', 'SHEET_ID', 'TARGET_EMAIL']);
  const file = DriveApp.getFileById(fileId);
  const processedFolder = DriveApp.getFolderById(config.PROCESSED_FOLDER_ID);
  const summary = {
    channel: 'drive-single',
    totalFound: 1,
    processed: 0,
    alreadyProcessed: 0,
    failed: 0,
    inProgressSkipped: 0,
    finalized: 0,
    summaryFilesCreated: 0,
    carrierFoldersCreated: 0
  };
  processDriveFileCore(file, 'Uploaded File', processedFolder, {}, config, summary);
  setLastRunSummary('drive-single', summary);
  appendProcessingFeed('info', 'Single file processing completed.', summary);
  return summary;
}

function processDriveFileCore(file, sourceLabel, processedFolder, carrierFolderCache, config, summary) {
  appendProcessingFeed('info', `Processing ${file.getName()}`, { fileId: file.getId(), sourceLabel });
  const processingKey = buildDriveProcessingKey(file);

  // A zip is a bag of invoices, not an invoice: expand it, process each file
  // inside, and only trash the archive once every entry has been accounted for.
  if (isArchiveFile(file)) {
    let archiveResult;
    try {
      archiveResult = processArchiveFile(
        file.getBlob(),
        file.getName(),
        sourceLabel,
        processingKey,
        config,
        { sendEmail: false }
      );
    } catch (archiveError) {
      summary.failed += 1;
      Logger.log(`Archive expansion failed for ${file.getName()}: ${archiveError.message}`);
      appendProcessingFeed('error', `Could not expand ${file.getName()}`, { error: archiveError.message });
      return;
    }

    summary.archivesExpanded = (summary.archivesExpanded || 0) + 1;
    summary.archiveEntriesFound = (summary.archiveEntriesFound || 0) + archiveResult.counts.total;

    let unfinalized = 0;
    archiveResult.entries.forEach(function(item) {
      const entryResult = item.result;
      if (entryResult.ok && entryResult.status === 'processed') {
        summary.processed += 1;
        try {
          finalizeProcessedOutputBlobs(entryResult, processedFolder, summary);
          summary.finalized += 1;
        } catch (moveError) {
          unfinalized += 1;
          summary.failed += 1;
          appendProcessingFeed('error', `Finalize failed for ${item.entry.path} in ${file.getName()}`, { error: moveError.message });
        }
      } else if (entryResult.ok && entryResult.status === 'already_processed') {
        summary.alreadyProcessed += 1;
      } else if (entryResult.ok && entryResult.status === 'held_for_review') {
        summary.heldForReview = (summary.heldForReview || 0) + 1;
        unfinalized += 1;
      } else if (entryResult.status === 'in_progress') {
        summary.inProgressSkipped += 1;
        unfinalized += 1;
      } else {
        summary.failed += 1;
        unfinalized += 1;
        appendProcessingFeed('error', `Failed ${item.entry.path} in ${file.getName()}`, {
          status: entryResult.status,
          error: entryResult.error || null
        });
      }
    });

    // Keep the archive in place while any entry still needs attention, so the
    // originals stay reachable for review or a retry.
    if (unfinalized === 0 && archiveResult.complete && archiveResult.counts.total > 0) {
      file.setTrashed(true);
      appendProcessingFeed('success', `Finalized archive ${file.getName()} (${archiveResult.counts.total} invoice(s))`, {
        fileId: file.getId(),
        counts: archiveResult.counts
      });
    } else {
      appendProcessingFeed('warning', `Archive ${file.getName()} left in place — ${unfinalized} entr${unfinalized === 1 ? 'y' : 'ies'} still need attention.`, {
        fileId: file.getId(),
        counts: archiveResult.counts
      });
    }
    return;
  }

  const result = processInvoiceFile(file.getBlob(), file.getName(), sourceLabel, processingKey, config, file.getMimeType(), {
    sendEmail: false,
    fileId: file.getId()
  });

  if (result.ok && result.status === 'processed') {
    summary.processed += 1;
  } else if (result.ok && result.status === 'already_processed') {
    summary.alreadyProcessed += 1;
  } else if (result.ok && result.status === 'held_for_review') {
    summary.heldForReview = (summary.heldForReview || 0) + 1;
    return; // leave the source file in place; it's queued for manual review
  } else if (result.status === 'in_progress') {
    summary.inProgressSkipped += 1;
  } else {
    summary.failed += 1;
  }

  if (!result.ok) {
    appendProcessingFeed('error', `Failed ${file.getName()}`, { status: result.status, error: result.error || null });
    return;
  }

  try {
    const outputBlobs = finalizeProcessedOutputBlobs(result, processedFolder, summary);
    file.setTrashed(true);
    summary.finalized += 1;
    appendProcessingFeed('success', `Finalized ${file.getName()} -> ${processedFolder.getName()}`, {
      fileId: file.getId(),
      carrierType: result.carrierType,
      outputFiles: outputBlobs.map(function(blob) { return blob.getName(); })
    });
  } catch (moveError) {
    summary.failed += 1;
    Logger.log(`Move failed for ${file.getName()}: ${moveError.message}`);
    appendProcessingFeed('error', `Finalize failed for ${file.getName()}`, { error: moveError.message });
  }
}

/**
 * Diagnostic: run this manually from the Apps Script editor to debug Gmail search issues.
 * Logs all Gmail labels (to verify exact name/path) and tests several query variants.
 */
/**
 * One-time fix for the shorthand `label:ITBP`, which is not the label's real
 * nested path. Goes through the saved filters — writing the raw property
 * directly would just be overwritten the next time the filters are saved.
 */
function fixGmailSearchQuery() {
  const filters = getEmailFilters();
  const before = buildGmailSearchQuery(filters.fetch);

  filters.fetch.labels = filters.fetch.labels.map(function(label) {
    return label === 'ITBP' ? 'Automation-Emails/ITBP' : label;
  });
  filters.fetch.rawQuery = String(filters.fetch.rawQuery || '')
    .replace(/label:ITBP\b/g, 'label:Automation-Emails/ITBP');

  const saved = saveEmailFilters(filters);
  const after = buildGmailSearchQuery(saved.fetch);
  Logger.log('Search query updated: "' + before + '" → "' + after + '"');
  appendProcessingFeed('info', 'Gmail search query fixed', { from: before, to: after });
  return { ok: true, from: before, to: after };
}

function debugGmailSearch() {
  const config = getConfig();
  const results = {};

  // 1. List all labels so we can see exact names (case & nesting)
  const labels = GmailApp.getUserLabels().map(function(l) { return l.getName(); });
  results.allLabels = labels;
  Logger.log('All Gmail labels: ' + JSON.stringify(labels));

  // 2. Try progressively broader queries
  const queries = [
    config.SEARCH_QUERY,
    'has:attachment is:unread label:ITBP',
    'has:attachment label:ITBP',
    'label:ITBP',
    'has:attachment is:unread',
    'has:attachment'
  ];

  queries.forEach(function(q) {
    try {
      const count = GmailApp.search(q, 0, 10).length;
      results[q] = count + ' thread(s)';
      Logger.log('Query "' + q + '" → ' + count + ' thread(s)');
    } catch (e) {
      results[q] = 'ERROR: ' + e.message;
      Logger.log('Query "' + q + '" → ERROR: ' + e.message);
    }
  });

  appendProcessingFeed('info', 'Gmail debug results', results);
  return results;
}

/* ═══════════════════════════════════════════════════════════════════════════
 * EMAIL FILTERING
 *
 * Gmail's own filters cannot tell these apart:
 *
 *   logistics.invoices@lidl.us  ──forwards──▶  you   (inbound carrier invoice)
 *   logistics.invoices@lidl.us  ──forwards──▶  you   (outbound freight, ignore)
 *
 * Both arrive From: the delegated mailbox, so every native rule that keys on
 * the sender matches both. The original sender, the original subject and the
 * original recipient only exist inside the forwarded body.
 *
 * So this module does two things:
 *
 *   1. UNWRAPS the forward — parses the "---------- Forwarded message ---------"
 *      / Outlook "From:/Sent:/To:/Subject:" envelope and the delegated-delivery
 *      headers, exposing originalFrom / originalSubject / deliveredTo as
 *      first-class fields.
 *
 *   2. Runs an ORDERED, UI-EDITABLE rule list over those fields. First rule
 *      that matches wins and decides accept or reject; if nothing matches, the
 *      configured default applies. First-match-wins is deliberate — it is the
 *      only evaluation order a non-programmer can reliably predict, and the
 *      filter tester can always name the single rule responsible.
 * ═══════════════════════════════════════════════════════════════════════════ */

const EMAIL_FILTER_FIELDS = [
  'effectiveFrom',
  'originalFrom',
  'from',
  'to',
  'cc',
  'deliveredTo',
  'replyTo',
  'forwardedFrom',
  'effectiveSubject',
  'originalSubject',
  'subject',
  'body',
  'attachmentName',
  'any'
];

const EMAIL_FILTER_OPERATORS = [
  'contains',
  'notContains',
  'equals',
  'startsWith',
  'endsWith',
  'regex',
  'domainIs',
  'isEmpty',
  'isNotEmpty'
];

const EMAIL_FILTER_ACTIONS = ['accept', 'reject'];
const EMAIL_FILTER_DEFAULT_ACTIONS = ['accept', 'skip', 'review'];
const EMAIL_FILTER_MAX_RULES = 100;
const EMAIL_FILTER_BODY_LIMIT = 20000;

/**
 * Shipped defaults. The rules are deliberately concrete rather than clever:
 * they encode the inbound/outbound split this tool was built for, and they are
 * meant to be edited in the Email Filters tab, not treated as fixed.
 */
const DEFAULT_CATCH_LABEL = 'Backup Catch';

const DEFAULT_EMAIL_FILTERS = {
  enabled: true,
  // An escape hatch for everything the rules cannot anticipate: drop the label
  // on a message by hand and it gets processed, no questions asked. Manual
  // forwards are the main reason it exists — they arrive from whoever pressed
  // Forward, under a "Fwd:" subject, often outside any search the rules use.
  catchLabel: DEFAULT_CATCH_LABEL,
  catchLabelBypassesRules: true,
  catchLabelRemoveAfterProcessing: true,
  fetch: {
    mode: 'builder',
    rawQuery: '',
    unreadOnly: true,
    hasAttachment: true,
    newerThanDays: 30,
    labels: [],
    fromAnyOf: [],
    deliveredToAnyOf: [],
    subjectAnyOf: [],
    extraTerms: ''
  },
  // Mailboxes that forward mail to you. Used to recognise a delegated forward
  // and to look through it at the original sender.
  delegatedMailboxes: [],
  defaultAction: 'review',
  markRejectedRead: false,
  rules: [
    {
      id: 'rule-outbound-subject',
      name: 'Skip outbound freight',
      enabled: true,
      action: 'reject',
      field: 'effectiveSubject',
      operator: 'regex',
      value: '\\b(out\\s*bound|outbound|ob)\\b',
      caseSensitive: false
    },
    {
      id: 'rule-outbound-body',
      name: 'Skip anything the body calls outbound',
      enabled: true,
      action: 'reject',
      field: 'body',
      operator: 'regex',
      value: '\\boutbound\\s+(?:freight|shipment|load|delivery)\\b',
      caseSensitive: false
    },
    {
      id: 'rule-noise',
      name: 'Skip auto-replies and bounces',
      enabled: true,
      action: 'reject',
      field: 'subject',
      operator: 'regex',
      value: '(out of office|automatic reply|undeliverable|delivery status notification|read receipt)',
      caseSensitive: false
    },
    {
      id: 'rule-inbound-subject',
      name: 'Process inbound invoices',
      enabled: true,
      action: 'accept',
      field: 'effectiveSubject',
      operator: 'regex',
      value: '\\b(inbound|in\\s*bound|invoice|inv\\b|billing|freight bill)\\b',
      caseSensitive: false
    }
  ]
};

/* ─── storage ─────────────────────────────────────────────────────────────── */

/**
 * Read the saved filters, migrating from the legacy raw SEARCH_QUERY the first
 * time so an upgrade does not silently widen what gets processed.
 */
function getEmailFilters() {
  // getConfig() derives SEARCH_QUERY from these, and getConfig() is called all
  // over the place — including once per carrier auto-fix — so parse them once
  // per execution rather than on every read.
  if (emailFiltersCache) {
    return emailFiltersCache;
  }

  const properties = PropertiesService.getScriptProperties();
  const raw = properties.getProperty(PROPERTY_KEYS.EMAIL_FILTERS);

  if (!raw) {
    const migrated = migrateEmailFiltersFromSearchQuery(
      properties.getProperty(PROPERTY_KEYS.SEARCH_QUERY) || DEFAULTS.SEARCH_QUERY
    );
    emailFiltersCache = normalizeEmailFilters(migrated);
    return emailFiltersCache;
  }

  let parsed = null;
  try {
    parsed = JSON.parse(raw);
  } catch (error) {
    Logger.log('Email filters were unreadable, falling back to defaults: ' + error.message);
    parsed = null;
  }
  emailFiltersCache = normalizeEmailFilters(parsed);
  return emailFiltersCache;
}

/**
 * Drop the per-execution filter cache. Call after anything writes the filter
 * property behind getEmailFilters()'s back.
 */
function clearEmailFiltersCache() {
  emailFiltersCache = null;
}

function saveEmailFilters(payload) {
  const normalized = normalizeEmailFilters(payload);
  emailFiltersCache = normalized;
  PropertiesService.getScriptProperties()
    .setProperty(PROPERTY_KEYS.EMAIL_FILTERS, JSON.stringify(normalized));
  // Keep the legacy property in step so anything still reading it agrees.
  PropertiesService.getScriptProperties()
    .setProperty(PROPERTY_KEYS.SEARCH_QUERY, buildGmailSearchQuery(normalized.fetch));
  return normalized;
}

/**
 * Turn the legacy `has:attachment is:unread label:Automation-Emails/ITBP`
 * string into builder settings, so the saved behaviour carries over and the
 * label simply becomes one editable field among several.
 */
function migrateEmailFiltersFromSearchQuery(query) {
  const migrated = JSON.parse(JSON.stringify(DEFAULT_EMAIL_FILTERS));
  const parsedFetch = parseGmailQueryToFetchSettings(query);
  migrated.fetch = parsedFetch;
  // The legacy query was the only filter in place, so nothing was being
  // rejected on content. Start permissive and let the rules be switched on
  // deliberately rather than surprising anyone mid-week.
  migrated.rules.forEach(function(rule) {
    rule.enabled = rule.action === 'reject';
  });
  migrated.defaultAction = 'accept';
  return migrated;
}

function normalizeEmailFilters(raw) {
  const source = raw && typeof raw === 'object' ? raw : {};
  const defaults = DEFAULT_EMAIL_FILTERS;
  const fetchSource = source.fetch && typeof source.fetch === 'object' ? source.fetch : {};

  const fetchSettings = {
    mode: fetchSource.mode === 'raw' ? 'raw' : 'builder',
    rawQuery: String(fetchSource.rawQuery || '').trim(),
    unreadOnly: fetchSource.unreadOnly !== false,
    hasAttachment: fetchSource.hasAttachment !== false,
    newerThanDays: normalizeNewerThanDays(fetchSource.newerThanDays),
    labels: normalizeStringList(fetchSource.labels),
    fromAnyOf: normalizeStringList(fetchSource.fromAnyOf),
    deliveredToAnyOf: normalizeStringList(fetchSource.deliveredToAnyOf),
    subjectAnyOf: normalizeStringList(fetchSource.subjectAnyOf),
    extraTerms: String(fetchSource.extraTerms || '').trim()
  };

  const rules = (Array.isArray(source.rules) ? source.rules : defaults.rules)
    .slice(0, EMAIL_FILTER_MAX_RULES)
    .map(normalizeEmailFilterRule)
    .filter(Boolean);

  return {
    enabled: source.enabled !== false,
    catchLabel: source.catchLabel === undefined
      ? defaults.catchLabel
      : String(source.catchLabel || '').trim(),
    catchLabelBypassesRules: source.catchLabelBypassesRules !== false,
    catchLabelRemoveAfterProcessing: source.catchLabelRemoveAfterProcessing !== false,
    fetch: fetchSettings,
    delegatedMailboxes: normalizeStringList(source.delegatedMailboxes).map(function(entry) {
      return entry.toLowerCase();
    }),
    defaultAction: EMAIL_FILTER_DEFAULT_ACTIONS.indexOf(source.defaultAction) >= 0
      ? source.defaultAction
      : defaults.defaultAction,
    markRejectedRead: source.markRejectedRead === true,
    rules: rules
  };
}

function normalizeEmailFilterRule(rule, index) {
  if (!rule || typeof rule !== 'object') return null;

  const field = EMAIL_FILTER_FIELDS.indexOf(rule.field) >= 0 ? rule.field : 'any';
  const operator = EMAIL_FILTER_OPERATORS.indexOf(rule.operator) >= 0 ? rule.operator : 'contains';
  const action = EMAIL_FILTER_ACTIONS.indexOf(rule.action) >= 0 ? rule.action : 'reject';
  const value = String(rule.value === undefined || rule.value === null ? '' : rule.value).trim();

  // Every operator except the emptiness checks needs something to match on.
  if (!value && operator !== 'isEmpty' && operator !== 'isNotEmpty') {
    return null;
  }
  if (operator === 'regex' && !isUsableRegexSource(value)) {
    return null;
  }

  return {
    id: String(rule.id || `rule-${Date.now()}-${index || 0}`),
    name: String(rule.name || '').trim() || describeEmailFilterRule({ action, field, operator, value }),
    enabled: rule.enabled !== false,
    action: action,
    field: field,
    operator: operator,
    value: value,
    caseSensitive: rule.caseSensitive === true
  };
}

function normalizeStringList(value) {
  if (Array.isArray(value)) {
    return value.map(function(item) { return String(item || '').trim(); }).filter(Boolean);
  }
  return String(value || '')
    .split(/[\n,]/)
    .map(function(item) { return item.trim(); })
    .filter(Boolean);
}

function normalizeNewerThanDays(value) {
  const days = Number(value);
  if (!isFinite(days) || days <= 0) return 0;
  return Math.min(Math.round(days), 3650);
}

function isUsableRegexSource(source) {
  try {
    new RegExp(source);
    return true;
  } catch (error) {
    return false;
  }
}

function describeEmailFilterRule(rule) {
  const verb = rule.action === 'accept' ? 'Process' : 'Skip';
  if (rule.operator === 'isEmpty') return `${verb} when ${rule.field} is empty`;
  if (rule.operator === 'isNotEmpty') return `${verb} when ${rule.field} is set`;
  return `${verb} when ${rule.field} ${rule.operator} "${rule.value}"`;
}

/* ─── fetch query ─────────────────────────────────────────────────────────── */

/**
 * Build the Gmail search string from the builder settings.
 *
 * This is only the coarse "what to pull out of the mailbox" pass — the rules
 * below do the real work. Deliberately never returns an empty query, which
 * would match the entire mailbox.
 */
function buildGmailSearchQuery(fetchSettings) {
  const settings = (fetchSettings && typeof fetchSettings === 'object')
    ? fetchSettings
    : DEFAULT_EMAIL_FILTERS.fetch;

  if (settings.mode === 'raw') {
    return String(settings.rawQuery || '').trim() || DEFAULTS.SEARCH_QUERY;
  }

  const parts = [];
  if (settings.hasAttachment !== false) parts.push('has:attachment');
  if (settings.unreadOnly !== false) parts.push('is:unread');

  const orGroup = function(prefix, values) {
    const list = normalizeStringList(values);
    if (list.length === 0) return;
    const terms = list.map(function(value) { return prefix + ':' + quoteGmailTerm(value); });
    parts.push(terms.length === 1 ? terms[0] : '(' + terms.join(' OR ') + ')');
  };

  orGroup('label', settings.labels);
  orGroup('from', settings.fromAnyOf);
  orGroup('deliveredto', settings.deliveredToAnyOf);
  orGroup('subject', settings.subjectAnyOf);

  const days = normalizeNewerThanDays(settings.newerThanDays);
  if (days > 0) parts.push('newer_than:' + days + 'd');

  const extra = String(settings.extraTerms || '').trim();
  if (extra) parts.push(extra);

  if (parts.length === 0) {
    return 'has:attachment is:unread';
  }
  return parts.join(' ');
}

function quoteGmailTerm(value) {
  const text = String(value || '').trim();
  return /[\s()"]/.test(text) ? '"' + text.replace(/"/g, '') + '"' : text;
}

/**
 * Best-effort reverse of buildGmailSearchQuery, used once to migrate whatever
 * query was already configured into editable fields.
 */
function parseGmailQueryToFetchSettings(query) {
  const settings = JSON.parse(JSON.stringify(DEFAULT_EMAIL_FILTERS.fetch));
  let remaining = String(query || '').trim();

  settings.labels = [];
  settings.fromAnyOf = [];
  settings.deliveredToAnyOf = [];
  settings.subjectAnyOf = [];
  settings.newerThanDays = 0;
  settings.hasAttachment = false;
  settings.unreadOnly = false;

  const takeOperator = function(name, sink) {
    const pattern = new RegExp(name + ':("[^"]*"|[^\\s()]+)', 'gi');
    remaining = remaining.replace(pattern, function(match, captured) {
      sink.push(String(captured).replace(/^"|"$/g, ''));
      return ' ';
    });
  };

  takeOperator('label', settings.labels);
  takeOperator('from', settings.fromAnyOf);
  takeOperator('deliveredto', settings.deliveredToAnyOf);
  takeOperator('subject', settings.subjectAnyOf);

  remaining = remaining.replace(/newer_than:(\d+)d/gi, function(match, days) {
    settings.newerThanDays = normalizeNewerThanDays(days);
    return ' ';
  });
  remaining = remaining.replace(/has:attachment/gi, function() {
    settings.hasAttachment = true;
    return ' ';
  });
  remaining = remaining.replace(/is:unread/gi, function() {
    settings.unreadOnly = true;
    return ' ';
  });

  // Whatever is left is either OR/parenthesis scaffolding from the groups we
  // already consumed, or terms we do not model — keep the latter verbatim.
  settings.extraTerms = remaining
    .replace(/\bOR\b/gi, ' ')
    .replace(/[()]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();

  return settings;
}

/* ═══════════════════════════════════════════════════════════════════════════
 * FORWARD-AS-ATTACHMENT (.eml)
 *
 * "Forward as attachment" in Gmail, and dragging a message into a new one in
 * Outlook, wrap the original in a message/rfc822 part rather than quoting it.
 * The invoice is then an attachment *inside* an attachment, and every
 * attachment check on the outer message comes up empty — so the mail looked to
 * this tool like a note with nothing in it.
 *
 * There is no MIME parser in Apps Script, so this walks the raw message text:
 * find the multipart boundary, split into parts, and decode any part that is a
 * supported invoice. Good enough for mail produced by real clients, and it
 * never throws — a message it cannot read simply yields no attachments.
 * ═══════════════════════════════════════════════════════════════════════════ */

const EML_MIME_TYPES = ['message/rfc822', 'application/octet-stream'];
const MAX_EML_DEPTH = 3;
const MAX_EML_BYTES = 25 * 1024 * 1024;

function isEmlAttachment(attachment) {
  const name = String(attachment.getName ? attachment.getName() : '');
  const type = String(attachment.getContentType ? attachment.getContentType() : '').toLowerCase();
  if (/\.eml$/i.test(name) || /\.msg$/i.test(name)) return true;
  return type === 'message/rfc822';
}

/**
 * Every invoice attachment on a message, including any buried inside a
 * forwarded-as-attachment .eml.
 *
 * Returns objects shaped like GmailAttachment (getName / getContentType /
 * getSize / copyBlob) so callers do not care where a file came from.
 */
function gatherInvoiceAttachments(message) {
  let attachments = [];
  try {
    attachments = message.getAttachments() || [];
  } catch (error) {
    return [];
  }

  const collected = [];

  for (let i = 0; i < attachments.length; i++) {
    const attachment = attachments[i];

    if (isIngestibleInvoiceAttachment(attachment)) {
      collected.push(attachment);
      continue;
    }

    if (!isEmlAttachment(attachment)) {
      continue;
    }

    const nested = extractAttachmentsFromEml(attachment, 0);
    if (nested.length > 0) {
      appendProcessingFeed('info', `Opened ${attachment.getName()} and found ${nested.length} invoice file(s) inside.`, {
        container: attachment.getName(),
        files: nested.map(function(item) { return item.getName(); })
      });
    }
    nested.forEach(function(item) { collected.push(item); });
  }

  return collected;
}

/**
 * Pull the invoice attachments out of one .eml blob.
 */
function extractAttachmentsFromEml(attachment, depth) {
  if (Number(depth || 0) >= MAX_EML_DEPTH) return [];

  let raw = '';
  try {
    const blob = attachment.copyBlob ? attachment.copyBlob() : attachment;
    const bytes = blob.getBytes();
    if (bytes.length > MAX_EML_BYTES) {
      appendProcessingFeed('warning', `${attachment.getName()} is too large to open (${bytes.length} bytes).`, {});
      return [];
    }
    raw = Utilities.newBlob(bytes).getDataAsString('UTF-8');
  } catch (error) {
    appendProcessingFeed('warning', `Could not read ${attachment.getName()}: ${error.message}`, {});
    return [];
  }

  return parseEmlAttachments(raw, depth);
}

/**
 * Walk a raw RFC-822 message and return its invoice attachments.
 * Pure string work, so this is exercised directly by the tests.
 */
function parseEmlAttachments(rawMessage, depth) {
  const results = [];
  const raw = String(rawMessage || '');
  if (!raw) return results;

  const boundary = findMimeBoundary(raw);
  if (!boundary) {
    return results;
  }

  const parts = splitMimeParts(raw, boundary);

  for (let i = 0; i < parts.length; i++) {
    const part = parts[i];
    const headers = parseMimeHeaders(part.headerText);
    const contentType = String(headers['content-type'] || '');
    const mediaType = contentType.split(';')[0].trim().toLowerCase();
    const fileName = mimeFileName(headers);

    // Nested multipart: recurse rather than trying to decode it as a file.
    if (mediaType.indexOf('multipart/') === 0) {
      parseEmlAttachments(part.headerText + '\r\n\r\n' + part.body, Number(depth || 0)).forEach(function(item) {
        results.push(item);
      });
      continue;
    }

    // A message forwarded inside a forward.
    if (mediaType === 'message/rfc822') {
      parseEmlAttachments(part.body, Number(depth || 0) + 1).forEach(function(item) {
        results.push(item);
      });
      continue;
    }

    if (!fileName) continue;

    const resolvedType = mediaType && mediaType !== 'application/octet-stream'
      ? mediaType
      : mimeTypeForFileName(fileName);

    if (!isSupportedInvoiceMimeType(resolvedType, fileName) && !isArchiveMimeType(resolvedType, fileName)) {
      continue;
    }

    const encoding = String(headers['content-transfer-encoding'] || '').trim().toLowerCase();
    const blob = decodeMimePartToBlob(part.body, encoding, resolvedType, fileName);
    if (blob) {
      results.push(wrapBlobAsAttachment(blob, resolvedType, fileName));
    }
  }

  return results;
}

/**
 * The boundary token from the outermost Content-Type header.
 */
function findMimeBoundary(raw) {
  const headerEnd = findHeaderBlockEnd(raw);
  const headerText = raw.slice(0, headerEnd);
  const match = headerText.match(/boundary\s*=\s*"([^"]+)"/i) ||
    headerText.match(/boundary\s*=\s*([^\s;"]+)/i);
  return match ? match[1] : '';
}

function findHeaderBlockEnd(raw) {
  const crlf = raw.indexOf('\r\n\r\n');
  const lf = raw.indexOf('\n\n');
  if (crlf >= 0 && (lf < 0 || crlf < lf)) return crlf;
  if (lf >= 0) return lf;
  return raw.length;
}

/**
 * Split a multipart body on its boundary, returning { headerText, body } for
 * each part.
 */
function splitMimeParts(raw, boundary) {
  const marker = '--' + boundary;
  const segments = raw.split(marker);
  const parts = [];

  // segments[0] is the preamble; the last is the epilogue after the closing
  // "--boundary--".
  for (let i = 1; i < segments.length; i++) {
    let segment = segments[i];
    if (segment.indexOf('--') === 0) break; // closing boundary
    segment = segment.replace(/^\r?\n/, '');

    const headerEnd = findHeaderBlockEnd(segment);
    const headerText = segment.slice(0, headerEnd);
    let body = segment.slice(headerEnd).replace(/^(\r\n\r\n|\n\n)/, '');
    body = body.replace(/\r?\n$/, '');

    parts.push({ headerText: headerText, body: body });
  }

  return parts;
}

/**
 * MIME headers, lowercased keys, with folded continuation lines joined.
 */
function parseMimeHeaders(headerText) {
  const headers = {};
  const lines = String(headerText || '').split(/\r?\n/);
  let currentKey = '';

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];
    if (!line.trim()) continue;

    if (/^\s/.test(line) && currentKey) {
      headers[currentKey] += ' ' + line.trim();
      continue;
    }

    const match = line.match(/^([A-Za-z0-9-]+)\s*:\s*(.*)$/);
    if (!match) continue;
    currentKey = match[1].toLowerCase();
    headers[currentKey] = match[2].trim();
  }

  return headers;
}

/**
 * The filename from Content-Disposition, falling back to the Content-Type
 * `name` parameter that older clients use.
 */
function mimeFileName(headers) {
  const sources = [headers['content-disposition'] || '', headers['content-type'] || ''];

  for (let i = 0; i < sources.length; i++) {
    const source = sources[i];
    const quoted = source.match(/(?:file)?name\s*=\s*"([^"]+)"/i);
    if (quoted) return decodeMimeWord(quoted[1]);
    const bare = source.match(/(?:file)?name\s*=\s*([^\s;]+)/i);
    if (bare) return decodeMimeWord(bare[1]);
    // RFC 2231 split/encoded form: filename*=UTF-8''invoice%20123.pdf
    const extended = source.match(/(?:file)?name\*\s*=\s*[^']*'[^']*'([^\s;]+)/i);
    if (extended) {
      try { return decodeURIComponent(extended[1]); } catch (error) { return extended[1]; }
    }
  }

  return '';
}

/**
 * Decode an RFC 2047 encoded-word ("=?UTF-8?B?...?=") filename.
 */
function decodeMimeWord(value) {
  const text = String(value || '').trim();
  const match = text.match(/^=\?([^?]+)\?([BbQq])\?(.*)\?=$/);
  if (!match) return text;

  try {
    if (match[2].toUpperCase() === 'B') {
      return Utilities.newBlob(Utilities.base64Decode(match[3])).getDataAsString('UTF-8');
    }
    return match[3]
      .replace(/_/g, ' ')
      .replace(/=([0-9A-Fa-f]{2})/g, function(all, hex) {
        return String.fromCharCode(parseInt(hex, 16));
      });
  } catch (error) {
    return text;
  }
}

/**
 * Turn one encoded MIME part body into a Blob.
 */
function decodeMimePartToBlob(body, encoding, contentType, fileName) {
  try {
    if (encoding === 'base64') {
      const cleaned = String(body || '').replace(/[^A-Za-z0-9+/=]/g, '');
      if (!cleaned) return null;
      return Utilities.newBlob(Utilities.base64Decode(cleaned), contentType, fileName);
    }

    if (encoding === 'quoted-printable') {
      return Utilities.newBlob(decodeQuotedPrintable(body), contentType, fileName);
    }

    return Utilities.newBlob(String(body || ''), contentType, fileName);
  } catch (error) {
    Logger.log('Could not decode MIME part ' + fileName + ': ' + error.message);
    return null;
  }
}

function decodeQuotedPrintable(body) {
  return String(body || '')
    .replace(/=\r?\n/g, '')
    .replace(/=([0-9A-Fa-f]{2})/g, function(all, hex) {
      return String.fromCharCode(parseInt(hex, 16));
    });
}

/**
 * Present a Blob with the slice of the GmailAttachment API the pipeline uses.
 */
function wrapBlobAsAttachment(blob, contentType, fileName) {
  return {
    getName: function() { return fileName; },
    getContentType: function() { return contentType; },
    getSize: function() {
      try { return blob.getBytes().length; } catch (error) { return 0; }
    },
    copyBlob: function() {
      return Utilities.newBlob(blob.getBytes(), contentType, fileName);
    },
    getDataAsString: function() {
      try { return blob.getDataAsString(); } catch (error) { return ''; }
    }
  };
}

/* ─── the Backup Catch lane ───────────────────────────────────────────────── */

/**
 * Search string for the catch label.
 *
 * Deliberately NOT folded into the main query: the whole point is that a
 * message wearing this label is fetched even when the normal search would
 * never have found it — wrong sender, wrong label, already read, older than the
 * age limit. Callers run it as a second search and merge the results.
 */
function buildCatchLabelQuery(filters) {
  const label = String((filters && filters.catchLabel) || '').trim();
  if (!label) return '';
  return 'label:' + quoteGmailTerm(label) + ' has:attachment';
}

/**
 * Does this message carry the catch label?
 * Gmail returns labels per thread, so a thread-level check is the honest one.
 */
function messageHasCatchLabel(message, filters) {
  const wanted = String((filters && filters.catchLabel) || '').trim().toLowerCase();
  if (!wanted) return false;

  try {
    const thread = message.getThread ? message.getThread() : null;
    if (!thread) return false;
    const labels = thread.getLabels() || [];
    for (let i = 0; i < labels.length; i++) {
      if (String(labels[i].getName() || '').toLowerCase() === wanted) {
        return true;
      }
    }
  } catch (error) {
    Logger.log('Could not read thread labels: ' + error.message);
  }
  return false;
}

/**
 * Take the catch label off once the message has been dealt with, so it does not
 * come back on every run. Best effort — failing to remove it is not a reason to
 * fail the invoice.
 */
function removeCatchLabel(message, filters) {
  if (!filters || !filters.catchLabelRemoveAfterProcessing) return false;
  const wanted = String(filters.catchLabel || '').trim();
  if (!wanted) return false;

  try {
    const label = GmailApp.getUserLabelByName(wanted);
    const thread = message.getThread ? message.getThread() : null;
    if (label && thread) {
      thread.removeLabel(label);
      return true;
    }
  } catch (error) {
    Logger.log('Could not remove the catch label: ' + error.message);
  }
  return false;
}

/**
 * Merge the results of several Gmail searches, keeping each message once.
 * Returns plain message objects rather than threads, because the catch-label
 * search and the main search routinely overlap.
 */
function collectUniqueMessages(queries, perQueryLimit) {
  const seen = {};
  const messages = [];

  for (let q = 0; q < queries.length; q++) {
    const query = String(queries[q] || '').trim();
    if (!query) continue;

    let threads = [];
    try {
      threads = perQueryLimit
        ? GmailApp.search(query, 0, perQueryLimit)
        : GmailApp.search(query);
    } catch (error) {
      appendProcessingFeed('error', `Gmail search failed for "${query}"`, { error: error.message });
      continue;
    }

    for (let t = 0; t < threads.length; t++) {
      const threadMessages = threads[t].getMessages();
      for (let m = 0; m < threadMessages.length; m++) {
        const message = threadMessages[m];
        const id = message.getId();
        if (seen[id]) continue;
        seen[id] = true;
        messages.push(message);
      }
    }
  }

  return messages;
}


/* ─── forwarded-message unwrapping ────────────────────────────────────────── */

const FORWARD_MARKER_PATTERN = /^\s*(?:-{2,}\s*(?:Forwarded message|Original Message)\s*-{2,}|Begin forwarded message:|_{10,})\s*$/i;
const MAX_FORWARD_HOPS = 8;
const FORWARD_SUBJECT_PREFIX_PATTERN = /^\s*(?:(?:re|fw|fwd|tr|aw|wg|vs)\s*(?:\[\d+\])?\s*:\s*)+/i;

/**
 * Pull the address out of `Name <a@b.com>` / `<a@b.com>` / `a@b.com`.
 */
function extractEmailAddress(value) {
  const text = String(value || '');
  const angled = text.match(/<([^<>@\s]+@[^<>@\s]+)>/);
  if (angled) return angled[1].toLowerCase();
  const bare = text.match(/([^\s<>,;:"']+@[^\s<>,;:"']+\.[A-Za-z]{2,})/);
  return bare ? bare[1].toLowerCase().replace(/[.,;:]+$/, '') : '';
}

function extractEmailDomain(value) {
  const address = extractEmailAddress(value);
  const at = address.lastIndexOf('@');
  return at >= 0 ? address.slice(at + 1) : '';
}

/**
 * "FW: Fwd: RE: Invoice 123" -> "Invoice 123"
 */
function stripForwardPrefixes(subject) {
  let text = String(subject || '').trim();
  let previous = null;
  while (text !== previous) {
    previous = text;
    text = text.replace(FORWARD_SUBJECT_PREFIX_PATTERN, '').trim();
  }
  return text;
}

/**
 * Read the forwarded envelope out of a message body.
 *
 * Handles the Gmail block ("---------- Forwarded message ---------" followed by
 * From/Date/Subject/To) and the Outlook block (From/Sent/To/Subject with no
 * marker at all). Returns the FIRST envelope found — for a delegated mailbox
 * forwarding a carrier's mail, that is the carrier.
 */
function parseForwardedEnvelope(body) {
  const chain = parseForwardedEnvelopeChain(body);
  return chain.length ? chain[0] : null;
}

/**
 * Read EVERY forwarded envelope in a message body, outermost first.
 *
 * A manually forwarded invoice usually has two hops — the carrier mailed the
 * delegated box, and a person then forwarded that on — which means the first
 * envelope names the delegated mailbox, not the carrier. Callers need the whole
 * chain to find the sender who actually issued the invoice.
 */
function parseForwardedEnvelopeChain(body) {
  const lines = String(body || '').split('\n');
  const limit = Math.min(lines.length, 1200);
  const chain = [];
  let index = 0;

  while (index < limit && chain.length < MAX_FORWARD_HOPS) {
    const isMarker = FORWARD_MARKER_PATTERN.test(lines[index]);
    const startsHeaderBlock = /^\s*From:\s*\S/i.test(lines[index]);
    if (!isMarker && !startsHeaderBlock) {
      index += 1;
      continue;
    }

    const envelope = readEnvelopeHeaders(lines, isMarker ? index + 1 : index, limit);
    if (!envelope) {
      index += 1;
      continue;
    }

    chain.push(envelope);
    // Skip past the block we just consumed so its own header lines are not
    // re-read as the start of the next hop.
    index = (envelope.endIndex || index) + 1;
  }

  return chain;
}

/**
 * Pick the envelope that names the party who actually sent the invoice.
 *
 * Walks the chain from the innermost hop outwards and takes the first sender
 * that is not one of our own addresses — a delegated mailbox, or the operator
 * who pressed Forward. Those are relays, not senders, and treating one as the
 * sender is what made manually forwarded invoices unroutable.
 */
function selectOriginatingEnvelope(chain, internalAddresses) {
  if (!chain || chain.length === 0) return null;

  const internal = (internalAddresses || [])
    .map(function(address) { return String(address || '').toLowerCase().trim(); })
    .filter(Boolean);

  const isInternal = function(value) {
    const address = extractEmailAddress(value);
    return !!address && internal.indexOf(address) >= 0;
  };

  for (let i = chain.length - 1; i >= 0; i--) {
    if (!isInternal(chain[i].from)) {
      return chain[i];
    }
  }

  // Every hop was one of ours (an internal relay chain with no outside sender
  // recoverable from the body) — the innermost is still the best guess.
  return chain[chain.length - 1];
}


/**
 * Read From/To/Cc/Subject/Date out of a contiguous header block.
 * Requires at least a From and one of Subject/To so a stray "From:" in prose
 * does not register as a forward.
 */
function readEnvelopeHeaders(lines, startIndex, limit) {
  const envelope = { from: '', to: '', cc: '', subject: '', date: '', endIndex: startIndex };
  let seen = 0;
  let blankRun = 0;

  for (let i = startIndex; i < Math.min(limit, startIndex + 16); i++) {
    const line = String(lines[i] || '');
    if (!line.trim()) {
      blankRun += 1;
      // A single blank line inside the block is tolerated (Outlook inserts one);
      // two means the block is over.
      if (blankRun >= 2 && seen > 0) break;
      continue;
    }
    blankRun = 0;

    const match = line.match(/^\s*(From|To|Cc|Subject|Date|Sent|Reply-To)\s*:\s*(.*)$/i);
    if (!match) {
      if (seen > 0) break;
      continue;
    }

    const key = match[1].toLowerCase();
    const value = match[2].trim();
    envelope.endIndex = i;
    if (key === 'from' && !envelope.from) { envelope.from = value; seen += 1; }
    else if (key === 'to' && !envelope.to) { envelope.to = value; seen += 1; }
    else if (key === 'cc' && !envelope.cc) { envelope.cc = value; seen += 1; }
    else if (key === 'subject' && !envelope.subject) { envelope.subject = value; seen += 1; }
    else if ((key === 'date' || key === 'sent') && !envelope.date) { envelope.date = value; seen += 1; }
  }

  if (!envelope.from) return null;
  if (!envelope.subject && !envelope.to) return null;
  if (!extractEmailAddress(envelope.from) && !/\S/.test(envelope.from)) return null;
  return envelope;
}

/**
 * Read a header without assuming getHeader() exists on this runtime.
 */
function getMessageHeaderSafe(message, name) {
  try {
    if (message && typeof message.getHeader === 'function') {
      return String(message.getHeader(name) || '');
    }
  } catch (error) {
    // Header not present, or not exposed — treated the same as absent.
  }
  return '';
}

/**
 * Everything the rules can match against, with the forward already unwrapped.
 *
 * `effectiveFrom` / `effectiveSubject` are the ones to reach for: on a normal
 * message they are the message's own sender and subject, and on a forward from
 * a delegated mailbox they are the ORIGINAL sender and subject — which is the
 * whole point.
 */
function buildMessageFilterContext(message, attachments, filters) {
  const delegated = (filters && filters.delegatedMailboxes) || [];
  const from = String(message.getFrom ? message.getFrom() : '');
  const to = String(message.getTo ? message.getTo() : '');
  const cc = (function() {
    try { return String(message.getCc ? message.getCc() : ''); } catch (error) { return ''; }
  })();
  const replyTo = (function() {
    try { return String(message.getReplyTo ? message.getReplyTo() : ''); } catch (error) { return ''; }
  })();
  const subject = String(message.getSubject ? message.getSubject() : '');

  let body = '';
  try {
    body = String(message.getPlainBody ? message.getPlainBody() : '');
  } catch (error) {
    body = '';
  }
  if (body.length > EMAIL_FILTER_BODY_LIMIT) {
    body = body.slice(0, EMAIL_FILTER_BODY_LIMIT);
  }

  const deliveredTo = getMessageHeaderSafe(message, 'Delivered-To');
  const forwardedForHeader = getMessageHeaderSafe(message, 'X-Forwarded-For') ||
    getMessageHeaderSafe(message, 'X-Forwarded-To');
  const originalSenderHeader = getMessageHeaderSafe(message, 'X-Original-Sender') ||
    getMessageHeaderSafe(message, 'X-Original-From');

  // Follow the whole chain, not just the outermost hop: a manually forwarded
  // invoice has the delegated mailbox at hop 1 and the carrier at hop 2.
  const chain = parseForwardedEnvelopeChain(body);

  // Which delegated mailbox, if any, this reached us through.
  const delegatedCandidates = [from, to, cc, deliveredTo, forwardedForHeader]
    .map(extractEmailAddress)
    .filter(Boolean);
  chain.forEach(function(hop) {
    const hopFrom = extractEmailAddress(hop.from);
    if (hopFrom) delegatedCandidates.push(hopFrom);
  });

  let forwardedFrom = '';
  for (let i = 0; i < delegated.length && !forwardedFrom; i++) {
    if (delegatedCandidates.indexOf(delegated[i]) >= 0) {
      forwardedFrom = delegated[i];
    }
  }

  // Addresses that only ever relay: the delegated boxes and whoever runs this
  // script (they are the one pressing Forward).
  const internalAddresses = delegated.slice();
  const owner = getScriptOwnerAddress();
  if (owner) internalAddresses.push(owner);

  const envelope = selectOriginatingEnvelope(chain, internalAddresses);

  const originalFrom = (envelope && envelope.from) || originalSenderHeader || '';
  const originalSubject = (envelope && envelope.subject) || stripForwardPrefixes(subject);
  const isForwarded = chain.length > 0 ||
    !!forwardedForHeader ||
    FORWARD_SUBJECT_PREFIX_PATTERN.test(subject);

  // Look through the forward only when we actually found someone behind it.
  const effectiveFrom = originalFrom || from;
  const effectiveSubject = originalSubject || subject;

  const attachmentNames = (attachments || []).map(function(attachment) {
    try { return String(attachment.getName() || ''); } catch (error) { return ''; }
  }).filter(Boolean);

  return {
    from: from,
    fromAddress: extractEmailAddress(from),
    to: to,
    cc: cc,
    replyTo: replyTo,
    deliveredTo: deliveredTo,
    forwardedFrom: forwardedFrom,
    subject: subject,
    body: body,
    originalFrom: originalFrom,
    originalSubject: originalSubject,
    originalTo: (envelope && envelope.to) || '',
    originalDate: (envelope && envelope.date) || '',
    effectiveFrom: effectiveFrom,
    effectiveFromAddress: extractEmailAddress(effectiveFrom),
    effectiveSubject: effectiveSubject,
    hasCatchLabel: messageHasCatchLabel(message, filters),
    isForwarded: isForwarded,
    isDelegatedForward: !!forwardedFrom && isForwarded,
    forwardHops: chain.length,
    // True when a person forwarded this on by hand rather than a mail rule
    // relaying it — the case the Backup Catch lane exists for.
    isManualForward: chain.length > 1 || (chain.length === 1 && !!forwardedFrom && !deliveredToDelegated(deliveredTo, delegated)),
    attachmentNames: attachmentNames
  };
}

/**
 * Was this delivered straight to a delegated mailbox (an automatic relay) as
 * opposed to reaching us some other way?
 */
function deliveredToDelegated(deliveredTo, delegated) {
  const address = extractEmailAddress(deliveredTo);
  return !!address && (delegated || []).indexOf(address) >= 0;
}

/**
 * The address running this script, cached per execution.
 */
function getScriptOwnerAddress() {
  if (scriptOwnerCache !== null) {
    return scriptOwnerCache;
  }
  try {
    scriptOwnerCache = String(Session.getEffectiveUser().getEmail() || '').toLowerCase();
  } catch (error) {
    scriptOwnerCache = '';
  }
  return scriptOwnerCache;
}

/* ─── rule evaluation ─────────────────────────────────────────────────────── */

/**
 * The text(s) a rule looks at. Returns an array because attachmentName has one
 * value per attachment and any of them may match.
 */
function emailFilterFieldValues(context, field) {
  switch (field) {
    case 'attachmentName':
      return context.attachmentNames.slice();
    case 'any':
      return [[
        context.from,
        context.to,
        context.cc,
        context.deliveredTo,
        context.replyTo,
        context.subject,
        context.originalFrom,
        context.originalSubject,
        context.attachmentNames.join(' '),
        context.body
      ].filter(Boolean).join('\n')];
    default:
      return [String(context[field] === undefined || context[field] === null ? '' : context[field])];
  }
}

function emailFilterValueMatches(haystack, rule) {
  const text = String(haystack || '');
  const needle = String(rule.value || '');

  switch (rule.operator) {
    case 'isEmpty':
      return text.trim() === '';
    case 'isNotEmpty':
      return text.trim() !== '';
    case 'domainIs': {
      const wanted = needle.toLowerCase().replace(/^@/, '').trim();
      if (!wanted) return false;
      const domain = extractEmailDomain(text);
      return !!domain && (domain === wanted || domain.endsWith('.' + wanted));
    }
    case 'regex': {
      try {
        return new RegExp(needle, rule.caseSensitive ? '' : 'i').test(text);
      } catch (error) {
        return false;
      }
    }
    default: {
      const subject = rule.caseSensitive ? text : text.toLowerCase();
      const term = rule.caseSensitive ? needle : needle.toLowerCase();
      if (rule.operator === 'equals') return subject.trim() === term.trim();
      if (rule.operator === 'startsWith') return subject.trim().indexOf(term) === 0;
      if (rule.operator === 'endsWith') return subject.trim().lastIndexOf(term) === subject.trim().length - term.length && term.length > 0;
      if (rule.operator === 'notContains') return subject.indexOf(term) === -1;
      return subject.indexOf(term) >= 0;
    }
  }
}

function evaluateEmailFilterRule(rule, context) {
  const values = emailFilterFieldValues(context, rule.field);
  if (values.length === 0) {
    // No attachments to test against: emptiness checks still have an answer.
    return rule.operator === 'isEmpty';
  }
  for (let i = 0; i < values.length; i++) {
    if (emailFilterValueMatches(values[i], rule)) {
      return true;
    }
  }
  return false;
}

/**
 * Walk the rules in order; the first match decides.
 *
 * Returns { action, reason, rule } where action is 'accept', 'skip' or
 * 'review'. The rule is carried back so the UI and the activity feed can name
 * exactly what made the decision.
 */
function evaluateEmailFilters(context, filters) {
  const settings = filters || getEmailFilters();

  if (settings.enabled === false) {
    return { action: 'accept', reason: 'Filtering is turned off', rule: null };
  }

  // A person put the label there on purpose. That outranks every rule — the
  // rules exist to guess, and this is someone who already knows.
  if (context && context.hasCatchLabel && settings.catchLabelBypassesRules !== false) {
    return {
      action: 'accept',
      reason: `Carries the "${settings.catchLabel}" label`,
      rule: null,
      viaCatchLabel: true
    };
  }

  const rules = settings.rules || [];
  for (let i = 0; i < rules.length; i++) {
    const rule = rules[i];
    if (!rule || rule.enabled === false) continue;
    if (!evaluateEmailFilterRule(rule, context)) continue;

    return {
      action: rule.action === 'accept' ? 'accept' : 'skip',
      reason: `Rule ${i + 1} — ${rule.name}`,
      rule: rule
    };
  }

  const fallback = settings.defaultAction || 'review';
  return {
    action: fallback,
    reason: fallback === 'accept'
      ? 'No rule matched; default is to process'
      : (fallback === 'skip'
        ? 'No rule matched; default is to skip'
        : 'No rule matched; default is to queue for review'),
    rule: null
  };
}

/**
 * Save an undecided message's attachments into the source folder and queue
 * them for review.
 *
 * This is what `defaultAction: 'review'` does. The point is that a message the
 * rules cannot classify is never silently dropped and never silently posted —
 * it lands in the review queue with the sender and subject the rules saw, so
 * the next filter rule can be written from a real example.
 */
function queueGmailAttachmentsForReview(message, attachments, filterContext, decision, config) {
  const runtimeConfig = config || getConfig();
  const result = { ok: true, queued: 0 };

  if (!runtimeConfig.SOURCE_FOLDERS || runtimeConfig.SOURCE_FOLDERS.length === 0) {
    appendProcessingFeed('warning', `Cannot queue "${filterContext.subject}" for review — no source folder configured.`, {
      subject: filterContext.subject
    });
    return { ok: false, queued: 0 };
  }

  const sourceFolder = DriveApp.getFolderById(runtimeConfig.SOURCE_FOLDERS[0]);

  for (let i = 0; i < attachments.length; i++) {
    const attachment = attachments[i];
    try {
      const created = sourceFolder.createFile(attachment.copyBlob().setName(attachment.getName()));

      if (isArchiveMimeType(attachment.getContentType(), attachment.getName())) {
        const expansion = expandArchiveFileInPlace(created, sourceFolder);
        expansion.created.forEach(function(entryFile) {
          webExtractForReview(entryFile.getId());
          result.queued += 1;
        });
      } else {
        webExtractForReview(created.getId());
        result.queued += 1;
      }
    } catch (error) {
      result.ok = false;
      appendProcessingFeed('error', `Could not queue ${attachment.getName()} for review`, { error: error.message });
    }
  }

  appendProcessingFeed('warning', `Queued for review — could not classify "${filterContext.subject}" (${decision.reason})`, {
    from: filterContext.from,
    effectiveFrom: filterContext.effectiveFrom,
    effectiveSubject: filterContext.effectiveSubject,
    isDelegatedForward: filterContext.isDelegatedForward,
    queued: result.queued
  });

  return result;
}

/* ─── web endpoints ───────────────────────────────────────────────────────── */

function webGetEmailFilters() {
  const filters = getEmailFilters();
  return {
    ok: true,
    filters: filters,
    previewQuery: buildGmailSearchQuery(filters.fetch),
    fields: EMAIL_FILTER_FIELDS,
    operators: EMAIL_FILTER_OPERATORS,
    defaultActions: EMAIL_FILTER_DEFAULT_ACTIONS
  };
}

function webSaveEmailFilters(payload) {
  try {
    const saved = saveEmailFilters(payload);
    appendProcessingFeed('info', 'Email filters updated.', {
      ruleCount: saved.rules.length,
      defaultAction: saved.defaultAction,
      query: buildGmailSearchQuery(saved.fetch)
    });
    return {
      ok: true,
      filters: saved,
      previewQuery: buildGmailSearchQuery(saved.fetch),
      message: 'Filters saved.'
    };
  } catch (error) {
    return { ok: false, error: error.message };
  }
}

function webResetEmailFilters() {
  const saved = saveEmailFilters(DEFAULT_EMAIL_FILTERS);
  appendProcessingFeed('info', 'Email filters reset to defaults.', {});
  return {
    ok: true,
    filters: saved,
    previewQuery: buildGmailSearchQuery(saved.fetch),
    message: 'Filters reset to defaults.'
  };
}

/**
 * Preview the Gmail query a set of unsaved settings would produce, so the UI
 * can show it live while someone is editing.
 */
function webPreviewEmailFilterQuery(fetchSettings) {
  try {
    return { ok: true, query: buildGmailSearchQuery(normalizeEmailFilters({ fetch: fetchSettings }).fetch) };
  } catch (error) {
    return { ok: false, error: error.message };
  }
}

/**
 * Run a set of filters (saved or unsaved) against real mail and report what
 * each message would do, including which rule decided.
 *
 * `broad` widens the search past the fetch settings so the messages currently
 * being MISSED are visible too — the fetch query alone can only ever show what
 * already gets through.
 */
function webTestEmailFilters(options) {
  try {
    const opts = options || {};
    const filters = opts.filters ? normalizeEmailFilters(opts.filters) : getEmailFilters();
    const limit = Math.max(1, Math.min(Number(opts.limit) || 25, 100));

    const fetchQuery = buildGmailSearchQuery(filters.fetch);
    const catchQuery = buildCatchLabelQuery(filters);
    const days = normalizeNewerThanDays(filters.fetch.newerThanDays) || 30;
    const query = opts.broad ? `has:attachment newer_than:${days}d` : fetchQuery;

    // Always include the catch lane, so a labelled message shows up here even
    // when the main search would never have found it.
    const messages = collectUniqueMessages([query, catchQuery], limit);
    const results = [];
    const counts = { accept: 0, skip: 0, review: 0, noAttachments: 0 };

    {
      for (let mi = 0; mi < messages.length && results.length < limit; mi++) {
        const message = messages[mi];
        const attachments = gatherInvoiceAttachments(message);

        if (attachments.length === 0) {
          counts.noAttachments += 1;
          continue;
        }

        const context = buildMessageFilterContext(message, attachments, filters);
        const decision = evaluateEmailFilters(context, filters);
        counts[decision.action] = (counts[decision.action] || 0) + 1;

        results.push({
          messageId: message.getId(),
          date: message.getDate ? message.getDate().toISOString() : null,
          unread: message.isUnread ? message.isUnread() : null,
          from: context.from,
          effectiveFrom: context.effectiveFrom,
          subject: context.subject,
          effectiveSubject: context.effectiveSubject,
          deliveredTo: context.deliveredTo,
          forwardedFrom: context.forwardedFrom,
          isForwarded: context.isForwarded,
          isDelegatedForward: context.isDelegatedForward,
          isManualForward: context.isManualForward,
          forwardHops: context.forwardHops,
          hasCatchLabel: context.hasCatchLabel,
          attachmentNames: context.attachmentNames,
          action: decision.action,
          reason: decision.reason,
          viaCatchLabel: !!decision.viaCatchLabel,
          ruleId: decision.rule ? decision.rule.id : null,
          ruleName: decision.rule ? decision.rule.name : null
        });
      }
    }

    return {
      ok: true,
      query: query,
      fetchQuery: fetchQuery,
      catchQuery: catchQuery,
      broad: !!opts.broad,
      threadsScanned: messages.length,
      counts: counts,
      results: results
    };
  } catch (error) {
    return { ok: false, error: error.message };
  }
}

/**
 * The account's Gmail labels, so the label field can be a picker rather than a
 * string anyone has to spell exactly right.
 */
function webListGmailLabels() {
  try {
    return {
      ok: true,
      labels: GmailApp.getUserLabels().map(function(label) { return label.getName(); }).sort()
    };
  } catch (error) {
    return { ok: false, error: error.message, labels: [] };
  }
}

/**
 * Suggest delegated mailboxes by looking at what recent mail was delivered to
 * or forwarded through. Saves the user hunting for the exact address.
 */
function webSuggestDelegatedMailboxes() {
  try {
    const threads = GmailApp.search('has:attachment newer_than:30d', 0, 40);
    const counts = {};
    const myAddress = extractEmailAddress(Session.getActiveUser().getEmail());

    threads.forEach(function(thread) {
      thread.getMessages().forEach(function(message) {
        const candidates = [
          getMessageHeaderSafe(message, 'X-Forwarded-For'),
          getMessageHeaderSafe(message, 'X-Forwarded-To'),
          getMessageHeaderSafe(message, 'Delivered-To'),
          message.getTo ? message.getTo() : ''
        ];
        const envelope = parseForwardedEnvelope(message.getPlainBody ? message.getPlainBody() : '');
        if (envelope && envelope.to) candidates.push(envelope.to);

        candidates.forEach(function(candidate) {
          String(candidate || '').split(/[,;]/).forEach(function(part) {
            const address = extractEmailAddress(part);
            if (!address || address === myAddress) return;
            counts[address] = (counts[address] || 0) + 1;
          });
        });
      });
    });

    const suggestions = Object.keys(counts)
      .map(function(address) { return { address: address, seen: counts[address] }; })
      .sort(function(a, b) { return b.seen - a.seen; })
      .slice(0, 10);

    return { ok: true, suggestions: suggestions };
  } catch (error) {
    return { ok: false, error: error.message, suggestions: [] };
  }
}

function processIncomingPDFs() {
  if (!ENABLE_GMAIL_INGESTION) {
    const summary = {
      channel: 'gmail',
      disabled: true,
      reason: 'Gmail ingestion disabled (folder-only mode).'
    };
    setLastRunSummary('gmail', summary);
    return summary;
  }

  const config = getConfig();
  assertRequiredConfig(config, ['SEARCH_QUERY', 'PROCESSED_FOLDER_ID', 'SHEET_ID', 'TARGET_EMAIL']);
  const processedFolder = DriveApp.getFolderById(config.PROCESSED_FOLDER_ID);
  const filters = getEmailFilters();
  const searchQuery = buildGmailSearchQuery(filters.fetch);

  const summary = {
    channel: 'gmail',
    threads: 0,
    unreadMessages: 0,
    invoiceAttachmentsFound: 0,
    pdfAttachmentsFound: 0,
    processed: 0,
    alreadyProcessed: 0,
    failed: 0,
    inProgressSkipped: 0,
    finalized: 0,
    summaryFilesCreated: 0,
    messagesMarkedRead: 0,
    filteredOut: 0,
    queuedForReview: 0,
    caughtByLabel: 0
  };
  const startedAt = Date.now();
  let handledCount = 0;
  let stoppedEarly = false;

  // Two lanes. The normal search finds what the rules expect; the catch-label
  // search finds everything else a person flagged by hand, which is how a
  // manual forward gets in — it is usually from the wrong sender, under a
  // "Fwd:" subject, and may well already have been read.
  const catchQuery = buildCatchLabelQuery(filters);
  const messages = collectUniqueMessages([searchQuery, catchQuery]);

  summary.searchQuery = searchQuery;
  summary.catchQuery = catchQuery;
  summary.threads = messages.length;
  appendProcessingFeed('info', `Gmail search: "${searchQuery}"${catchQuery ? ` + "${catchQuery}"` : ''} → ${messages.length} message(s) found`, {
    searchQuery: searchQuery,
    catchQuery: catchQuery,
    messageCount: messages.length
  });

  {
    for (let mi = 0; mi < messages.length; mi++) {
      if (stoppedEarly) break;
      const message = messages[mi];
      const carriesCatchLabel = messageHasCatchLabel(message, filters);

      // Unread is the normal signal that a message still needs work, but a
      // hand-applied catch label overrides it — people label things they have
      // already opened.
      if (!message.isUnread() && !carriesCatchLabel) {
        continue;
      }

      // Checked per message, not just per attachment: the filter branches below
      // return early, and queueing for review costs an OCR pass each.
      if (handledCount >= MAX_INVOICES_PER_RUN || (Date.now() - startedAt) >= MAX_BATCH_RUN_MS) {
        stoppedEarly = true;
        break;
      }

      summary.unreadMessages += 1;
      if (carriesCatchLabel) {
        summary.caughtByLabel += 1;
      }

      const invoiceAttachments = gatherInvoiceAttachments(message);
      if (invoiceAttachments.length === 0) {
        if (carriesCatchLabel) {
          // Someone labelled this expecting it to be picked up; saying nothing
          // would look exactly like the tool ignoring them.
          appendProcessingFeed('warning', `"${message.getSubject()}" carries the ${filters.catchLabel} label but has no invoice attachment.`, {
            from: message.getFrom(),
            subject: message.getSubject()
          });
          removeCatchLabel(message, filters);
        }
        message.markRead();
        summary.messagesMarkedRead += 1;
        continue;
      }

      // Decide on the MESSAGE before opening its attachments. The context has
      // the forward already unwrapped, so a rule can key on the carrier that
      // actually sent the invoice rather than the delegated mailbox that
      // relayed it — which is the only way to tell inbound from outbound here.
      const filterContext = buildMessageFilterContext(message, invoiceAttachments, filters);
      const decision = evaluateEmailFilters(filterContext, filters);

      if (decision.viaCatchLabel) {
        appendProcessingFeed('info', `Caught by the ${filters.catchLabel} label: "${filterContext.effectiveSubject}"`, {
          from: filterContext.from,
          effectiveFrom: filterContext.effectiveFrom,
          forwardHops: filterContext.forwardHops,
          isManualForward: filterContext.isManualForward
        });
      }

      if (decision.action === 'skip') {
        summary.filteredOut += 1;
        appendProcessingFeed('info', `Filtered out: "${filterContext.subject}" — ${decision.reason}`, {
          from: filterContext.from,
          effectiveFrom: filterContext.effectiveFrom,
          subject: filterContext.subject,
          effectiveSubject: filterContext.effectiveSubject,
          isDelegatedForward: filterContext.isDelegatedForward,
          reason: decision.reason,
          ruleId: decision.rule ? decision.rule.id : null
        });
        if (filters.markRejectedRead) {
          message.markRead();
          summary.messagesMarkedRead += 1;
        }
        continue;
      }

      if (decision.action === 'review') {
        const queued = queueGmailAttachmentsForReview(message, invoiceAttachments, filterContext, decision, config);
        summary.queuedForReview += queued.queued;
        handledCount += queued.queued;
        if (queued.ok) {
          message.markRead();
          summary.messagesMarkedRead += 1;
          if (carriesCatchLabel) {
            removeCatchLabel(message, filters);
          }
        }
        continue;
      }

      let canMarkRead = true;

      for (let ai = 0; ai < invoiceAttachments.length; ai++) {
        if (handledCount >= MAX_INVOICES_PER_RUN || (Date.now() - startedAt) >= MAX_BATCH_RUN_MS) {
          stoppedEarly = true;
          canMarkRead = false;
          break;
        }
        const attachment = invoiceAttachments[ai];
        summary.invoiceAttachmentsFound += 1;
        summary.pdfAttachmentsFound += 1;
        const processingKey = buildGmailProcessingKey(message, attachment);

        // Zipped attachments expand into their invoices; each is finalized on
        // its own so one bad file inside cannot lose the rest.
        if (isArchiveMimeType(attachment.getContentType(), attachment.getName())) {
          handledCount += 1;
          let archiveResult;
          try {
            archiveResult = processArchiveFile(
              attachment.copyBlob(),
              attachment.getName(),
              `Gmail: ${message.getFrom()}`,
              processingKey,
              config,
              { sendEmail: false }
            );
          } catch (archiveError) {
            summary.failed += 1;
            canMarkRead = false;
            appendProcessingFeed('error', `Could not expand ${attachment.getName()}`, { error: archiveError.message });
            continue;
          }

          summary.archivesExpanded = (summary.archivesExpanded || 0) + 1;
          summary.archiveEntriesFound = (summary.archiveEntriesFound || 0) + archiveResult.counts.total;

          archiveResult.entries.forEach(function(item) {
            const entryResult = item.result;
            if (entryResult.ok && entryResult.status === 'processed') {
              summary.processed += 1;
              try {
                finalizeProcessedOutputBlobs(entryResult, processedFolder, summary);
                summary.finalized += 1;
              } catch (finalizeError) {
                summary.failed += 1;
                canMarkRead = false;
                appendProcessingFeed('error', `Finalize failed for ${item.entry.path}`, { error: finalizeError.message });
              }
            } else if (entryResult.ok && entryResult.status === 'already_processed') {
              summary.alreadyProcessed += 1;
            } else if (entryResult.ok && entryResult.status === 'held_for_review') {
              summary.heldForReview = (summary.heldForReview || 0) + 1;
              canMarkRead = false;
            } else if (entryResult.status === 'in_progress') {
              summary.inProgressSkipped += 1;
              canMarkRead = false;
            } else {
              summary.failed += 1;
              canMarkRead = false;
            }
          });
          continue;
        }

        const result = processInvoiceFile(
          attachment.copyBlob(),
          attachment.getName(),
          `Gmail: ${message.getFrom()}`,
          processingKey,
          config,
          attachment.getContentType(),
          { sendEmail: false }
        );
        handledCount += 1;

        if (result.ok && result.status === 'processed') {
          summary.processed += 1;
          try {
            finalizeProcessedOutputBlobs(result, processedFolder, summary);
            summary.finalized += 1;
            appendProcessingFeed('success', `Finalized Gmail attachment ${attachment.getName()} -> ${processedFolder.getName()}`, {
              source: message.getFrom(),
              carrierType: result.carrierType,
              outputFiles: (result.outputBlobs || []).map(function(blob) { return blob.getName(); })
            });
          } catch (finalizeError) {
            summary.failed += 1;
            canMarkRead = false;
            clearProcessingState({ stateKey: getProcessingStateKey(processingKey) });
            appendProcessingFeed('error', `Finalize failed for Gmail attachment ${attachment.getName()}`, { error: finalizeError.message });
            continue;
          }
        } else if (result.ok && result.status === 'already_processed') {
          summary.alreadyProcessed += 1;
        } else if (result.ok && result.status === 'held_for_review') {
          summary.heldForReview = (summary.heldForReview || 0) + 1;
          canMarkRead = false; // leave unread so it isn't lost if later approved
        } else if (result.status === 'in_progress') {
          summary.inProgressSkipped += 1;
          canMarkRead = false;
        } else {
          summary.failed += 1;
          canMarkRead = false;
        }
      }

      if (canMarkRead) {
        message.markRead();
        summary.messagesMarkedRead += 1;
        // Clear the label so the next run does not pick this up again: the
        // catch-label search deliberately ignores read/unread.
        if (carriesCatchLabel) {
          removeCatchLabel(message, filters);
        }
      }
    }
  }

  if (stoppedEarly) {
    summary.stoppedEarly = true;
    summary.batchLimit = MAX_INVOICES_PER_RUN;
    summary.handledThisRun = handledCount;
  }

  cleanupProcessedState(PROCESSING_STATE.RETENTION_DAYS);
  setLastRunSummary('gmail', summary);
  Logger.log(`Gmail processing summary: ${JSON.stringify(summary)}`);
  return summary;
}

/* ═══════════════════════════════════════════════════════════════════════════
 * ARCHIVE (.ZIP) INGESTION
 *
 * Carriers routinely send a week of invoices as a single zip. Previously those
 * files were not recognised as invoices at all: they sat in the source folder
 * untouched and silently never got processed. Now a zip is expanded and each
 * invoice inside is processed as if it had arrived on its own, with its own
 * idempotency key so a re-sent archive cannot double-post.
 * ═══════════════════════════════════════════════════════════════════════════ */

const ZIP_MIME_TYPES = [
  'application/zip',
  'application/x-zip-compressed',
  'application/x-zip',
  'multipart/x-zip',
  'application/octet-stream' // what some mail clients label a .zip attachment
];
const MAX_ARCHIVE_DEPTH = 3;
const MAX_ARCHIVE_ENTRIES = 100;

function isArchiveMimeType(mimeType, fileName) {
  const normalized = String(mimeType || '').toLowerCase();
  const name = String(fileName || '');
  if (/\.zip$/i.test(name)) return true;
  // octet-stream only counts when the name also says zip, otherwise every
  // unlabelled attachment would be treated as an archive.
  if (normalized === 'application/octet-stream') return false;
  return ZIP_MIME_TYPES.indexOf(normalized) >= 0;
}

function isArchiveFile(file) {
  return isArchiveMimeType(file.getMimeType(), file.getName());
}

/**
 * Best-effort MIME type for a file pulled out of a zip. Utilities.unzip() hands
 * back blobs typed application/octet-stream, which every downstream type check
 * would reject, so re-derive the type from the entry name.
 */
function mimeTypeForFileName(fileName) {
  const name = String(fileName || '').toLowerCase();
  if (/\.pdf$/.test(name)) return 'application/pdf';
  if (/\.xlsx$/.test(name)) return XLSX_MIME_TYPE;
  if (/\.xlsm$/.test(name)) return 'application/vnd.ms-excel.sheet.macroEnabled.12';
  if (/\.xls$/.test(name)) return 'application/vnd.ms-excel';
  if (/\.csv$/.test(name)) return 'text/csv';
  if (/\.zip$/.test(name)) return 'application/zip';
  return 'application/octet-stream';
}

/**
 * Entries that are packaging noise rather than invoices.
 */
function isIgnorableArchiveEntry(entryName) {
  const name = String(entryName || '');
  if (!name) return true;
  if (/(^|\/)__MACOSX\//.test(name)) return true;   // macOS resource forks
  if (/(^|\/)\._/.test(name)) return true;          // AppleDouble sidecars
  if (/(^|\/)\.DS_Store$/i.test(name)) return true;
  if (/(^|\/)Thumbs\.db$/i.test(name)) return true;
  if (/\/$/.test(name)) return true;                // directory entry
  return false;
}

/**
 * Expand a zip blob into the invoice files it contains.
 *
 * Nested archives are followed up to MAX_ARCHIVE_DEPTH. Each returned entry is
 * `{ blob, name, path, mimeType }`, where `path` keeps the position inside the
 * archive so it can be folded into the idempotency key and shown in the feed.
 */
function expandInvoiceArchive(archiveBlob, archiveName, depth) {
  const currentDepth = Number(depth || 0);
  const results = [];
  const skipped = [];

  if (currentDepth >= MAX_ARCHIVE_DEPTH) {
    return { entries: results, skipped: [`${archiveName} (nested deeper than ${MAX_ARCHIVE_DEPTH} levels)`] };
  }

  let unzipped;
  try {
    // Utilities.unzip needs the blob explicitly typed as a zip.
    unzipped = Utilities.unzip(archiveBlob.copyBlob
      ? archiveBlob.copyBlob().setContentType('application/zip')
      : archiveBlob.setContentType('application/zip'));
  } catch (error) {
    throw new Error(`Could not open archive ${archiveName}: ${error.message}`);
  }

  for (let i = 0; i < unzipped.length; i++) {
    if (results.length >= MAX_ARCHIVE_ENTRIES) {
      skipped.push(`${archiveName} (more than ${MAX_ARCHIVE_ENTRIES} entries)`);
      break;
    }

    const entryBlob = unzipped[i];
    const entryPath = entryBlob.getName();
    if (isIgnorableArchiveEntry(entryPath)) {
      continue;
    }

    const baseName = entryPath.split('/').pop();
    const entryMimeType = mimeTypeForFileName(baseName);

    if (isArchiveMimeType(entryMimeType, baseName)) {
      const nested = expandInvoiceArchive(entryBlob, `${archiveName}/${baseName}`, currentDepth + 1);
      nested.entries.forEach(function(nestedEntry) {
        results.push({
          blob: nestedEntry.blob,
          name: nestedEntry.name,
          path: `${baseName}/${nestedEntry.path}`,
          mimeType: nestedEntry.mimeType
        });
      });
      nested.skipped.forEach(function(item) { skipped.push(item); });
      continue;
    }

    if (!isSupportedInvoiceMimeType(entryMimeType, baseName)) {
      skipped.push(entryPath);
      continue;
    }

    results.push({
      blob: entryBlob.setContentType(entryMimeType).setName(baseName),
      name: baseName,
      path: entryPath,
      mimeType: entryMimeType
    });
  }

  return { entries: results, skipped: skipped };
}

/**
 * Process every invoice inside an archive.
 *
 * Returns { ok, status: 'archive', entries: [...], counts } so callers can roll
 * the per-entry outcomes into their own run summary. Each entry carries its own
 * processing key derived from the parent key plus the entry path, so re-sending
 * the same archive is a no-op while a *new* file inside it still gets picked up.
 */
function processArchiveFile(archiveBlob, archiveName, source, processingKey, config, options) {
  const expansion = expandInvoiceArchive(archiveBlob, archiveName, 0);

  if (expansion.skipped.length > 0) {
    appendProcessingFeed('info', `Skipped ${expansion.skipped.length} non-invoice entr${expansion.skipped.length === 1 ? 'y' : 'ies'} in ${archiveName}`, {
      archiveName: archiveName,
      skipped: expansion.skipped.slice(0, 20)
    });
  }

  if (expansion.entries.length === 0) {
    appendProcessingFeed('warning', `Archive ${archiveName} contained no invoice files.`, { archiveName: archiveName });
    return {
      ok: true,
      status: 'archive',
      entries: [],
      counts: { total: 0, processed: 0, alreadyProcessed: 0, heldForReview: 0, failed: 0, inProgress: 0 }
    };
  }

  appendProcessingFeed('info', `Expanding ${archiveName}: ${expansion.entries.length} invoice file(s).`, {
    archiveName: archiveName,
    entries: expansion.entries.map(function(entry) { return entry.path; }).slice(0, 20)
  });

  const counts = { total: 0, processed: 0, alreadyProcessed: 0, heldForReview: 0, failed: 0, inProgress: 0, notReached: 0 };
  const entryResults = [];
  const startedAt = Date.now();

  for (let i = 0; i < expansion.entries.length; i++) {
    // Apps Script kills a run at six minutes. Stop early and leave the rest for
    // the next pass — their idempotency keys mean nothing is reprocessed.
    if ((Date.now() - startedAt) >= MAX_BATCH_RUN_MS) {
      counts.notReached = expansion.entries.length - i;
      appendProcessingFeed('info', `Paused ${archiveName} after ${i} invoice(s) to stay inside the runtime limit; ${counts.notReached} left for the next run.`, {
        archiveName: archiveName
      });
      break;
    }

    const entry = expansion.entries[i];
    const entryKey = `${processingKey}|zip:${entry.path}`;
    counts.total += 1;

    let result;
    try {
      result = processInvoiceFile(
        entry.blob,
        entry.name,
        `${source} (in ${archiveName})`,
        entryKey,
        config,
        entry.mimeType,
        options
      );
    } catch (error) {
      result = { ok: false, status: 'failed', error: error.message };
    }

    if (result.ok && result.status === 'processed') counts.processed += 1;
    else if (result.ok && result.status === 'already_processed') counts.alreadyProcessed += 1;
    else if (result.ok && result.status === 'held_for_review') counts.heldForReview += 1;
    else if (result.status === 'in_progress') counts.inProgress += 1;
    else counts.failed += 1;

    entryResults.push({ entry: entry, result: result });
  }

  return {
    ok: counts.failed === 0,
    status: 'archive',
    complete: counts.notReached === 0,
    entries: entryResults,
    counts: counts
  };
}


function processPdfFile(pdfBlob, fileName, source, processingKey, config, mimeType) {
  return processInvoiceFile(pdfBlob, fileName, source, processingKey, config, mimeType || MimeType.PDF, {});
}

function processInvoiceFile(fileBlob, fileName, source, processingKey, config, mimeType, options) {
  const runtimeConfig = config || getConfig();
  const processingOptions = options || {};
  assertRequiredConfig(runtimeConfig, ['SHEET_ID', 'SHEET_NAME', 'TARGET_EMAIL']);

  const claim = tryBeginProcessing(processingKey, { fileName, source });
  if (claim.alreadyProcessed) {
    return {
      ok: true,
      status: 'already_processed',
      carrierType: normalizeCarrierType((claim.details && claim.details.carrierType) || '')
    };
  }
  if (claim.inProgress) {
    return { ok: false, status: 'in_progress' };
  }

  try {
    if (isSpreadsheetInvoiceMimeType(mimeType, fileName)) {
      return processSpreadsheetInvoiceFile(fileBlob, fileName, source, claim, runtimeConfig, processingOptions);
    }

    const extractedText = extractTextFromPdfBlob(fileBlob, fileName);

    const parsedInvoiceData = extractInvoiceData(extractedText, fileName);
    const mappingResult = applyMappedExtraction(extractedText, fileName);
    const invoiceData = mergeInvoiceData(parsedInvoiceData, mappingResult.values, extractedText);
    const appliedCoding = mappingResult.values.appliedCoding || determineCoding(extractedText);
    const carrierType = applyCarrierTypeAutoFix(mappingResult.values.carrierType
      ? normalizeCarrierType(mappingResult.values.carrierType)
      : determineCarrierType(extractedText, invoiceData, appliedCoding, fileName), runtimeConfig);

    // Hold anything we are not sure about rather than logging it as fact: an
    // unknown carrier files the invoice in the wrong place, and a shaky amount
    // is worse — it reconciles to the wrong number and nobody notices.
    const holdReason = !isCarrierConfirmed(carrierType, runtimeConfig)
      ? 'unconfirmed_carrier'
      : (shouldHoldForAmountReview(invoiceData, appliedCoding)
        ? (isCodingResolved(appliedCoding) ? 'low_confidence_amount' : 'unresolved_coding')
        : null);

    if (holdReason) {
      const reviewId = 'rv-' + Date.now() + '-' + (claim.stateKey || fileName).slice(0, 8);
      saveReviewItem({
        reviewId: reviewId,
        fileId: processingOptions.fileId || (claim.details && claim.details.fileId) || null,
        fileName: fileName,
        source: source,
        ocrText: extractedText.slice(0, 4000),
        extractedData: invoiceData,
        appliedCoding: appliedCoding,
        carrierType: carrierType,
        profileName: mappingResult.profileName || null,
        heldReason: holdReason,
        createdAt: new Date().toISOString()
      });
      clearProcessingState(claim);
      const holdMessage = holdReason === 'unconfirmed_carrier'
        ? `Held for review — unconfirmed carrier: "${carrierType}" (${fileName})`
        : (holdReason === 'unresolved_coding'
          ? `Held for review — no RDC could be determined (${fileName})`
          : `Held for review — amount needs checking (${invoiceData.amount}) (${fileName})`);
      appendProcessingFeed('warning', holdMessage, {
        reviewId,
        carrierType,
        fileName,
        heldReason: holdReason,
        amount: invoiceData.amount,
        amountConfidence: invoiceData.amountConfidence || null
      });
      return { ok: true, status: 'held_for_review', carrierType, reviewId, heldReason: holdReason };
    }

    const sheet = getInvoiceSheet(runtimeConfig);
    sheet.appendRow([
      new Date(),
      source,
      fileName,
      invoiceData.invoiceNumber,
      invoiceData.po,
      invoiceData.shipDate,
      invoiceData.deliveryDate,
      sheetAmountValue(invoiceData),
      invoiceData.origin,
      invoiceData.productType,
      invoiceData.destination,
      invoiceData.remitInfo,
      appliedCoding
    ]);

    const generatedCodedPdfBlob = generateHtmlPdf(invoiceData, appliedCoding, fileName);
    const mergePairId = buildMergePairId(claim.stateKey || processingKey || fileName);
    const outputBlobs = buildOutputBlobs(carrierType, fileName, generatedCodedPdfBlob, fileBlob, mergePairId);
    const emailPackage = buildInvoiceEmailAttachments(
      invoiceData, appliedCoding, generatedCodedPdfBlob, fileBlob, fileName, outputBlobs
    );

    if (processingOptions.sendEmail !== false) {
      sendInvoiceEmail(runtimeConfig, {
        fileName: fileName,
        subject: buildInvoiceEmailSubject(fileName, invoiceData),
        body: buildInvoiceEmailBody(invoiceData, appliedCoding, emailPackage),
        attachments: emailPackage.attachments
      });
    }

    markProcessingDone(claim, {
      fileName,
      source,
      coding: appliedCoding,
      carrierType,
      mergePairId,
      mappingProfile: mappingResult.profileName || null
    });
    Logger.log(`Successfully processed: ${fileName}`);
    appendProcessingFeed('success', `Processed ${fileName}`, {
      fileName,
      source,
      carrierType,
      mergePairId,
      mappingProfile: mappingResult.profileName || null
    });
    return {
      ok: true,
      status: 'processed',
      carrierType,
      mergePairId,
      outputBlobs: outputBlobs
    };
  } catch (error) {
    clearProcessingState(claim);
    Logger.log(`Error processing ${fileName}: ${error.message}`);
    appendProcessingFeed('error', `Error processing ${fileName}`, { error: error.message });
    return { ok: false, status: 'failed', error: error.message };
  }
}

function processSpreadsheetInvoiceFile(fileBlob, fileName, source, claim, runtimeConfig, options) {
  try {
    const spreadsheetData = extractSpreadsheetInvoiceData(fileBlob, fileName, source);

    const reconciliation = spreadsheetData.reconciliation || { issues: [], needsReview: false };
    const invoices = spreadsheetData.invoices || [];

    // Either the carrier is unknown, or the workbook does not add up. Both mean
    // a person should look before any of this reaches the ledger.
    // Same rule as the PDF path: unresolved coding is the thing a person has to
    // fix. If not one line could be routed to an RDC, hold the whole file.
    const anyCodingResolved = invoices.some(function(invoice) {
      return (invoice.groups || []).some(function(group) {
        return isCodingResolved(group.coding);
      });
    });

    const holdReason = !isCarrierConfirmed(spreadsheetData.carrierType, runtimeConfig)
      ? 'unconfirmed_carrier'
      : (!anyCodingResolved
        ? 'unresolved_coding'
        : (reconciliation.needsReview ? 'total_mismatch' : null));

    if (holdReason) {
      const reviewId = 'rv-' + Date.now() + '-' + (claim.stateKey || fileName).slice(0, 8);
      saveReviewItem({
        reviewId: reviewId,
        fileId: (options && options.fileId) || (claim.details && claim.details.fileId) || null,
        fileName: fileName,
        source: source,
        ocrText: reconciliation.issues.join('\n'),
        extractedData: invoices.length === 1 ? invoices[0].invoiceData : {},
        appliedCoding: spreadsheetData.codingSummary || '',
        carrierType: spreadsheetData.carrierType,
        profileName: 'spreadsheet',
        heldReason: holdReason,
        invoiceCount: invoices.length,
        reconciliation: reconciliation,
        createdAt: new Date().toISOString()
      });
      clearProcessingState(claim);
      const holdMessage = holdReason === 'unconfirmed_carrier'
        ? `Held for review — unconfirmed carrier: "${spreadsheetData.carrierType}" (${fileName})`
        : (holdReason === 'unresolved_coding'
          ? `Held for review — no RDC could be determined for any line (${fileName})`
          : `Held for review — spreadsheet totals do not reconcile (${fileName}): ${reconciliation.issues.join('; ')}`);
      appendProcessingFeed('warning', holdMessage, {
        reviewId,
        carrierType: spreadsheetData.carrierType,
        fileName,
        heldReason: holdReason,
        issues: reconciliation.issues
      });
      return { ok: true, status: 'held_for_review', carrierType: spreadsheetData.carrierType, reviewId, heldReason: holdReason };
    }

    const sheet = getInvoiceSheet(runtimeConfig);

    spreadsheetData.logRows.forEach(function(row) {
      sheet.appendRow(row);
    });

    const mergePairId = buildMergePairId(claim.stateKey || fileName);
    const outputBlobs = buildOutputBlobs(
      spreadsheetData.carrierType,
      fileName,
      spreadsheetData.codeSheetBlob,
      spreadsheetData.originalPdfBlob,
      mergePairId
    );

    const emailPackage = buildInvoiceEmailAttachments(
      invoices.length === 1 ? invoices[0].invoiceData : null,
      spreadsheetData.codingSummary,
      spreadsheetData.codeSheetBlob,
      spreadsheetData.originalPdfBlob,
      fileName,
      outputBlobs
    );

    if (!options || options.sendEmail !== false) {
      sendInvoiceEmail(runtimeConfig, {
        fileName: fileName,
        subject: `Processed Invoice: ${fileName}`,
        body: `${emailPackage.merged ? 'Attached is the coded summary merged with the original invoice.' : 'Attached are the code sheet and the original invoice.'}\n\n` +
          `Invoices found in this file: ${invoices.length}\n` +
          `Split coding summary: ${spreadsheetData.codingSummary}`,
        attachments: emailPackage.attachments
      });
    }

    markProcessingDone(claim, {
      fileName,
      source,
      coding: spreadsheetData.codingSummary,
      carrierType: spreadsheetData.carrierType,
      mergePairId,
      mappingProfile: 'spreadsheet',
      invoiceCount: invoices.length
    });
    appendProcessingFeed('success', `Processed ${fileName}`, {
      fileName,
      source,
      carrierType: spreadsheetData.carrierType,
      mergePairId,
      mappingProfile: 'spreadsheet',
      invoiceCount: invoices.length,
      groupCount: spreadsheetData.logRows.length
    });

    return {
      ok: true,
      status: 'processed',
      carrierType: spreadsheetData.carrierType,
      mergePairId,
      outputBlobs: outputBlobs
    };
  } catch (error) {
    clearProcessingState(claim);
    Logger.log(`Error processing ${fileName}: ${error.message}`);
    appendProcessingFeed('error', `Error processing ${fileName}`, { error: error.message });
    return { ok: false, status: 'failed', error: error.message };
  }
}

function extractTextFromPdfBlob(pdfBlob, fileName) {
  // Use Drive API PDF-to-Doc conversion with OCR language hint for best results
  const fileResource = {
    name: `${fileName.replace('.pdf', '')} - Temp Processing Doc`,
    mimeType: 'application/vnd.google-apps.document'
  };
  // ocrLanguage hint improves recognition accuracy for English invoices
  const docFile = Drive.Files.create(fileResource, pdfBlob, { ocrLanguage: 'en' });
  try {
    const doc = DocumentApp.openById(docFile.id);
    const rawText = doc.getBody().getText();
    Logger.log('Drive OCR extracted ' + rawText.length + ' chars from: ' + fileName);
    // Clean up common OCR artifacts to improve downstream extraction
    return cleanOcrText(rawText);
  } finally {
    try {
      DriveApp.getFileById(docFile.id).setTrashed(true);
    } catch (cleanupError) {
      Logger.log(`Temp doc cleanup failed (${docFile.id}): ${cleanupError.message}`);
    }
  }
}

/**
 * Clean up common OCR artifacts from Drive PDF-to-Doc conversion.
 * Normalizes whitespace, fixes character substitutions, and improves
 * field extraction reliability.
 */
function cleanOcrText(text) {
  if (!text) return '';

  var cleaned = text;

  // Normalize various unicode whitespace characters to regular spaces
  cleaned = cleaned.replace(/[\u00A0\u2000-\u200B\u202F\u205F\u3000\uFEFF]/g, ' ');

  // Fix common OCR character substitutions in numeric contexts
  // e.g., "lnvoice" → "Invoice", "$1,0OO" → "$1,000"
  cleaned = cleaned.replace(/lnvoice/gi, 'Invoice');
  cleaned = cleaned.replace(/lnv\s*#/gi, 'Inv #');

  // Normalize dashes/hyphens (OCR often produces em-dashes, en-dashes)
  cleaned = cleaned.replace(/[\u2010-\u2015\u2212\uFE58\uFE63\uFF0D]/g, '-');

  // Normalize quotes
  cleaned = cleaned.replace(/[\u2018\u2019\u201A\uFF07]/g, "'");
  cleaned = cleaned.replace(/[\u201C\u201D\u201E\uFF02]/g, '"');

  // Collapse runs of spaces (but preserve newlines for structure)
  cleaned = cleaned.replace(/[^\S\n]+/g, ' ');

  // Collapse 3+ consecutive blank lines into 2 (keep some structure)
  cleaned = cleaned.replace(/\n{4,}/g, '\n\n\n');

  // Trim each line
  cleaned = cleaned.split('\n').map(function(line) { return line.trim(); }).join('\n');

  return cleaned.trim();
}

function extractTextFromDriveFile(fileId) {
  const file = DriveApp.getFileById(fileId);
  return extractTextFromPdfBlob(file.getBlob(), file.getName());
}

function applyMappedExtraction(text, fileName) {
  const profile = selectMappingProfile(text, fileName, getExtractionMappings());
  if (!profile) {
    return { profileName: null, values: {} };
  }
  const values = extractValuesByProfile(text, profile);
  return {
    profileName: profile.name,
    values
  };
}

function mergeInvoiceData(baseData, mappedValues, ocrText) {
  const result = Object.assign({}, baseData || {});
  MAPPING_FIELDS.forEach(field => {
    if (field === 'carrierType' || field === 'appliedCoding') {
      return;
    }
    const mappedValue = mappedValues[field];
    const normalizedMappedValue = normalizeMappedValue(field, mappedValue);
    if (normalizedMappedValue && isHighConfidenceMappedValue(field, normalizedMappedValue)) {
      result[field] = normalizedMappedValue;
      if (field === 'amount') {
        // A mapping profile is carrier-specific and hand-authored, so an amount
        // it produces is authoritative — keep the numeric twin in step and
        // promote the confidence accordingly.
        result.amountValue = parseMoneyToken(normalizedMappedValue);
        result.amountConfidence = 'high';
        result.amountLabel = 'mapping profile';
      }
    }
  });

  // Apply learned corrections from past reviews
  if (ocrText) {
    var learned = applyLearnedCorrections(result, ocrText);
    Object.keys(learned).forEach(function(field) {
      if (learned[field]) {
        result[field] = learned[field];
      }
    });
  }

  return result;
}

function normalizeMappedValue(field, value) {
  if (!value) {
    return '';
  }
  const text = String(value).replace(/\s+/g, ' ').trim();
  if (!text) {
    return '';
  }

  if (field === 'amount') {
    // Run the mapped text through the money parser so "$1,150.00", "1.150,00"
    // and "(500.00)" all land as a clean signed value.
    return normalizeAmountText(text);
  }

  return text;
}

function isHighConfidenceMappedValue(field, value) {
  const text = String(value || '').trim();
  if (!text) {
    return false;
  }

  const lowered = text.toLowerCase();
  if (/^(address|headquarters|not found|see below|n\/a|unknown)$/i.test(text)) {
    return false;
  }

  if (field === 'invoiceNumber') {
    return /^[A-Z0-9-]{6,}$/i.test(text) && /\d/.test(text);
  }

  if (field === 'po') {
    return /^[A-Z0-9-]{5,}$/i.test(text) && /\d/.test(text);
  }

  if (field === 'shipDate' || field === 'deliveryDate') {
    return /^(?:\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4}|[A-Za-z]+\s+\d{1,2},?\s+\d{4})$/.test(text);
  }

  if (field === 'amount') {
    return /^-?[0-9]+\.[0-9]{2}$/.test(text);
  }

  if (field === 'origin' || field === 'destination' || field === 'productType') {
    if (text.length < 4 || text.length > 120) {
      return false;
    }
    return !/\b(remit to|ship date|delivery date|po box|amount line)\b/i.test(lowered);
  }

  if (field === 'remitInfo') {
    return text.length >= 10;
  }

  return true;
}

function selectMappingProfile(text, fileName, profiles) {
  const haystack = `${fileName || ''}\n${text || ''}`.toLowerCase();
  let bestProfile = null;
  let bestScore = -1;

  profiles.forEach(profile => {
    const keywords = Array.isArray(profile.matchKeywords) ? profile.matchKeywords : [];
    if (keywords.length === 0) {
      return;
    }

    let score = 0;
    for (let i = 0; i < keywords.length; i += 1) {
      if (haystack.includes(String(keywords[i]).toLowerCase())) {
        score += 1;
      }
    }

    if (score === keywords.length && score > bestScore) {
      bestProfile = profile;
      bestScore = score;
    }
  });

  return bestProfile;
}

function extractValuesByProfile(text, profile) {
  const values = {};
  const fields = (profile && profile.fields) || {};

  Object.keys(fields).forEach(fieldName => {
    if (MAPPING_FIELDS.indexOf(fieldName) === -1) {
      return;
    }
    const rule = fields[fieldName];
    const extracted = extractWithRule(text, rule);
    if (extracted) {
      values[fieldName] = extracted;
    }
  });

  return values;
}

function extractWithRule(text, rule) {
  if (!rule || !rule.mode) {
    return '';
  }

  if (rule.mode === 'regex') {
    try {
      const groupIndex = Number(rule.group || 1);
      const pattern = String(rule.pattern || '');
      if (!pattern) {
        return '';
      }
      const flags = sanitizeRegexFlags(rule.flags || 'i');
      const rx = new RegExp(pattern, flags);
      const match = text.match(rx);
      if (!match) {
        return '';
      }
      return cleanExtractedValue(match[groupIndex] || match[0] || '');
    } catch (error) {
      return '';
    }
  }

  if (rule.mode === 'between') {
    const start = String(rule.start || '');
    const end = String(rule.end || '');
    if (!start) {
      return '';
    }

    const lower = text.toLowerCase();
    const startIndex = lower.indexOf(start.toLowerCase());
    if (startIndex < 0) {
      return '';
    }

    const begin = startIndex + start.length;
    let finish = text.length;
    if (end) {
      const endIndex = lower.indexOf(end.toLowerCase(), begin);
      if (endIndex >= 0) {
        finish = endIndex;
      }
    }

    const maxChars = Math.max(1, Number(rule.maxChars || 120));
    const chunk = text.slice(begin, Math.min(finish, begin + maxChars));
    return cleanExtractedValue(chunk);
  }

  return '';
}

function cleanExtractedValue(value) {
  return String(value || '')
    .replace(/^[\s:\-]+/, '')
    .replace(/[\s\n]+/g, ' ')
    .trim();
}

function sanitizeRegexFlags(flags) {
  const unique = Array.from(new Set(String(flags || '').split(''))).join('');
  return unique.replace(/[^gimsuy]/g, '') || 'i';
}

function getExtractionMappings() {
  const raw = PropertiesService.getScriptProperties().getProperty(PROPERTY_KEYS.EXTRACTION_MAPPINGS);
  if (!raw) {
    return [];
  }
  try {
    const parsed = JSON.parse(raw);
    if (!Array.isArray(parsed)) {
      return [];
    }
    return parsed.map(profile => sanitizeMappingProfile(profile));
  } catch (error) {
    return [];
  }
}

function saveExtractionMappingProfile(profile) {
  const safeProfile = sanitizeMappingProfile(profile);
  if (!safeProfile.id) {
    throw new Error('Profile id is required.');
  }
  if (!safeProfile.name) {
    throw new Error('Profile name is required.');
  }

  const profiles = getExtractionMappings();
  const idx = profiles.findIndex(item => item.id === safeProfile.id);
  if (idx >= 0) {
    profiles[idx] = safeProfile;
  } else {
    profiles.push(safeProfile);
  }
  PropertiesService.getScriptProperties().setProperty(PROPERTY_KEYS.EXTRACTION_MAPPINGS, JSON.stringify(profiles));
  appendProcessingFeed('info', `Saved mapping profile: ${safeProfile.name}`, { profileId: safeProfile.id });
  return safeProfile;
}

function createOrUpdateArriveTemplateProfile() {
  const profile = {
    id: 'arrive-template-v1',
    name: 'Arrive Template (V3)',
    description: 'Auto-tuned Arrive Logistics profile. Extracts invoice data from standard Arrive format with header-row alignment and multiline PO capture.',
    matchKeywords: [
      'arrive',
      'arrive logistics',
      'ship date',
      'delivery date',
      'po #'
    ],
    fields: {
      invoiceNumber: {
        mode: 'regex',
        pattern: '(?:Invoice(?:\\s*(?:No|#|Number))?|INV(?:OICE)?)\\s*[:#-]?\\s*([A-Z0-9-]*\\d[A-Z0-9-]{4,})',
        flags: 'i',
        group: 1
      },
      po: {
        mode: 'regex',
        pattern: 'PO\\s*#\\s*\\n.*\\b([A-Z0-9][A-Z0-9\\-]{3,})\\s*$',
        flags: 'im',
        group: 1
      },
      shipDate: {
        mode: 'regex',
        pattern: 'Ship\\s*Date\\s+Delivery\\s*Date\\s+Pick\\s*Up\\s*#\\s+BOL\\s*#\\s+Delivery\\s*#\\s+Shipment\\s*ID\\s+PO\\s*#\\s+([0-9]{1,2}[\\/\\-][0-9]{1,2}[\\/\\-][0-9]{2,4})',
        flags: 'i',
        group: 1
      },
      deliveryDate: {
        mode: 'regex',
        pattern: 'Ship\\s*Date\\s+Delivery\\s*Date\\s+Pick\\s*Up\\s*#\\s+BOL\\s*#\\s+Delivery\\s*#\\s+Shipment\\s*ID\\s+PO\\s*#\\s+[0-9]{1,2}[\\/\\-][0-9]{1,2}[\\/\\-][0-9]{2,4}\\s+([0-9]{1,2}[\\/\\-][0-9]{1,2}[\\/\\-][0-9]{2,4})',
        flags: 'i',
        group: 1
      },
      amount: {
        mode: 'regex',
        pattern: '(?:Total|Amount\\s*Due|Line\\s*Haul|Balance)\\D{0,20}\\$?\\s*([0-9,]+\\.[0-9]{2})',
        flags: 'i',
        group: 1
      },
      carrierType: {
        mode: 'regex',
        pattern: '(Arrive\\s+Logistics)',
        flags: 'i',
        group: 1
      },
      remitInfo: {
        mode: 'between',
        start: 'Remit To',
        end: 'Ship Date',
        maxChars: 180
      }
    }
  };

  return saveExtractionMappingProfile(profile);
}

function deleteExtractionMappingProfile(profileId) {
  const targetId = String(profileId || '').trim();
  const profiles = getExtractionMappings().filter(item => item.id !== targetId);
  PropertiesService.getScriptProperties().setProperty(PROPERTY_KEYS.EXTRACTION_MAPPINGS, JSON.stringify(profiles));
  appendProcessingFeed('info', `Deleted mapping profile: ${targetId}`, { profileId: targetId });
}

function sanitizeMappingProfile(profile) {
  const source = profile || {};
  const profileId = String(source.id || source.name || `profile-${Date.now()}`)
    .trim()
    .replace(/\s+/g, '-');
  const normalized = {
    id: profileId,
    name: String(source.name || profileId).trim(),
    description: String(source.description || '').trim(),
    matchKeywords: normalizeKeywords(source.matchKeywords),
    fields: {}
  };

  const fields = source.fields || {};
  MAPPING_FIELDS.forEach(field => {
    if (!fields[field]) {
      return;
    }
    const rule = fields[field];
    if (rule.mode !== 'regex' && rule.mode !== 'between') {
      return;
    }

    if (rule.mode === 'regex') {
      normalized.fields[field] = {
        mode: 'regex',
        pattern: String(rule.pattern || '').trim(),
        flags: sanitizeRegexFlags(rule.flags || 'i'),
        group: Number(rule.group || 1)
      };
      return;
    }

    normalized.fields[field] = {
      mode: 'between',
      start: String(rule.start || '').trim(),
      end: String(rule.end || '').trim(),
      maxChars: Number(rule.maxChars || 120)
    };
  });

  return normalized;
}

function normalizeKeywords(input) {
  if (Array.isArray(input)) {
    return input
      .map(item => String(item || '').trim())
      .filter(Boolean);
  }

  return String(input || '')
    .split(/[\n,]/)
    .map(item => item.trim())
    .filter(Boolean);
}

function mergePdfsBestEffort(invoiceData, appliedCoding, originalPdfBlob, fileName) {
  const sheetsResult = mergePdfsViaSheets(invoiceData, appliedCoding, originalPdfBlob, fileName);
  if (sheetsResult.ok) return sheetsResult;

  return {
    ok: false,
    error: 'Sheets merge failed: ' + (sheetsResult.error || 'unknown')
  };
}

function mergePdfsViaSheets(invoiceData, appliedCoding, originalPdfBlob, fileName) {
  var tempIds = [];
  try {
    var token = ScriptApp.getOAuthToken();
    var baseName = (fileName || 'invoice').replace(/\.pdf$/i, '');

    var masterMeta = Drive.Files.create({
      name: '_master_sheet_merge_' + Date.now(),
      mimeType: 'application/vnd.google-apps.spreadsheet'
    });
    tempIds.push(masterMeta.id);
    var masterSS = waitForSpreadsheetOpen(masterMeta.id, 'master spreadsheet');
    var summarySheet = masterSS.getActiveSheet();
    summarySheet.setName('Coded Summary');

    var rows = [
      ['INVOICE PROCESSING SUMMARY', ''],
      ['', ''],
      ['Coding Applied', appliedCoding || 'N/A'],
      ['Invoice Number', (invoiceData && invoiceData.invoiceNumber) || MISSING_VALUE_LABEL],
      ['PO Number', (invoiceData && invoiceData.po) || MISSING_VALUE_LABEL],
      ['Ship Date', (invoiceData && invoiceData.shipDate) || MISSING_VALUE_LABEL],
      ['Delivery Date', (invoiceData && invoiceData.deliveryDate) || MISSING_VALUE_LABEL],
      ['Amount', (invoiceData && invoiceData.amount) || MISSING_VALUE_LABEL],
      ['Origin', (invoiceData && invoiceData.origin) || 'Review Required'],
      ['Destination', (invoiceData && invoiceData.destination) || 'Review Required'],
      ['Product Type', (invoiceData && invoiceData.productType) || 'Review Required'],
      ['Remit Info', (invoiceData && invoiceData.remitInfo) || MISSING_VALUE_LABEL],
      ['Source File', fileName || '']
    ];

    summarySheet.getRange(1, 1, rows.length, 2).setValues(rows);
    summarySheet.getRange('A1:B1')
      .merge()
      .setBackground('#1a73e8')
      .setFontColor('#ffffff')
      .setFontSize(14)
      .setFontWeight('bold')
      .setHorizontalAlignment('center');
    summarySheet.getRange(3, 1, rows.length - 2, 1)
      .setBackground('#f1f3f4')
      .setFontWeight('bold');
    summarySheet.getRange(3, 2, rows.length - 2, 1).setWrap(true);
    summarySheet.setColumnWidth(1, 180);
    summarySheet.setColumnWidth(2, 380);
    summarySheet.getRange(1, 1, rows.length, 2)
      .setBorder(true, true, true, true, true, true, '#cccccc', SpreadsheetApp.BorderStyle.SOLID);

    var origSheetMeta = Drive.Files.create(
      { name: '_orig_sheet_' + Date.now(), mimeType: 'application/vnd.google-apps.spreadsheet' },
      originalPdfBlob
    );
    if (!origSheetMeta || !origSheetMeta.id) {
      throw new Error('Drive conversion did not return a spreadsheet id for original PDF');
    }
    tempIds.push(origSheetMeta.id);
    var origSS = waitForSpreadsheetOpen(origSheetMeta.id, 'original PDF conversion');
    var origSheets = origSS.getSheets();
    if (!origSheets || origSheets.length === 0) {
      throw new Error('Converted original PDF produced no sheets/pages');
    }
    for (var i = 0; i < origSheets.length; i++) {
      origSheets[i].copyTo(masterSS);
    }

    var defaultSheet = masterSS.getSheetByName('Sheet1');
    if (defaultSheet && masterSS.getSheets().length > 1) {
      masterSS.deleteSheet(defaultSheet);
    }

    SpreadsheetApp.flush();

    var exportUrl = 'https://docs.google.com/spreadsheets/d/' + masterMeta.id +
      '/export?format=pdf&size=letter&portrait=true&fitw=true&gridlines=false&printtitle=false&sheetnames=false&fzr=false';

    var response = UrlFetchApp.fetch(exportUrl, {
      headers: { Authorization: 'Bearer ' + token },
      muteHttpExceptions: true
    });

    if (response.getResponseCode() !== 200) {
      throw new Error('Sheet PDF export returned HTTP ' + response.getResponseCode());
    }

    var mergedBlob = response.getBlob();
    mergedBlob.setName('Coded_' + baseName + '.pdf');
    return { ok: true, blob: mergedBlob, method: 'sheets' };
  } catch (err) {
    Logger.log('mergePdfsViaSheets failed: ' + err.toString());
    return { ok: false, error: err.toString() };
  } finally {
    tempIds.forEach(function(id) {
      try { DriveApp.getFileById(id).setTrashed(true); } catch (e) {}
    });
  }
}

function waitForSpreadsheetOpen(fileId, contextLabel) {
  var maxAttempts = 12;
  var lastErr = '';
  for (var attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      // Drive conversion can be eventually consistent; verify mimeType first.
      var meta = Drive.Files.get(fileId);
      if (meta && meta.mimeType === 'application/vnd.google-apps.spreadsheet') {
        return SpreadsheetApp.openById(fileId);
      }
      lastErr = 'mimeType=' + (meta && meta.mimeType ? meta.mimeType : 'unknown');
    } catch (e) {
      lastErr = e.toString();
    }

    Utilities.sleep(1000 * attempt);
  }

  throw new Error('Timed out waiting for spreadsheet readiness (' + contextLabel + '): ' + fileId + ' (' + lastErr + ')');
}

/* ═══════════════════════════════════════════════════════════════════════════
 * SPREADSHEET INVOICES
 *
 * The first version of this only understood one workbook shape: a tab literally
 * named "Shifts" whose header row contained PO, Dropoff Location and Delivery
 * Date. Anything else threw "Shifts tab not found" and the file never got
 * processed — including the common case of a register workbook holding many
 * invoices, one per line.
 *
 * What is handled now:
 *   · any tab, found by scoring candidate header rows against column synonyms
 *   · one invoice per workbook (line items summed), as before
 *   · MANY invoices per workbook, grouped by an invoice-number column
 *   · totals reconciled against the workbook's own declared total
 *   · spreadsheet error cells (#REF!, #VALUE!) surfaced instead of read as 0
 * ═══════════════════════════════════════════════════════════════════════════ */

// Header spellings seen in the wild, normalized (lowercased, non-alphanumerics
// stripped). Lists must stay disjoint — a spelling may only belong to one field.
const SPREADSHEET_COLUMN_SYNONYMS = {
  invoiceNumber: ['invoiceno', 'invoicenumber', 'invoiceid', 'invno', 'invnumber', 'inv', 'billnumber', 'billno', 'documentnumber', 'docno'],
  deliveryDate: ['deliverydate', 'dropoffdate', 'datedelivered', 'delivereddate', 'delivered', 'dropdate', 'date'],
  shipDate: ['shipdate', 'shippeddate', 'pickupdate', 'collectiondate', 'loaddate', 'servicedate'],
  shipmentNumber: ['s', 'shipment', 'shipmentnumber', 'shipmentid', 'loadnumber', 'load', 'pronumber', 'pro', 'bol', 'bolnumber', 'tripnumber', 'trip'],
  po: ['po', 'ponumber', 'purchaseorder', 'purchaseorderno', 'ordernumber', 'order', 'customerpo'],
  pickupLocation: ['pickuplocation', 'pickup', 'origin', 'origincity', 'originlocation', 'pickupcity', 'from', 'shipfrom'],
  dropoffLocation: ['dropofflocation', 'dropoff', 'destination', 'destinationcity', 'deliverylocation', 'dropoffcity', 'to', 'shipto'],
  transportCost: ['transportcost', 'linehaul', 'linehaulcost', 'freight', 'freightcharge', 'basecharge', 'baserate', 'rate'],
  accessorials: ['tolls', 'toll', 'fuelsurcharge', 'fuel', 'accessorial', 'accessorials', 'detention', 'lumper', 'othercharges', 'extracharges'],
  total: ['total', 'totalamount', 'amount', 'amountdue', 'invoicetotal', 'grandtotal', 'totalcharge', 'charges', 'charge', 'netamount', 'totalcost', 'cost'],
  carrier: ['carrier', 'carriername', 'vendor', 'vendorname', 'supplier', 'truckingcompany'],
  description: ['description', 'commodity', 'product', 'producttype', 'service', 'details', 'notes']
};

const SPREADSHEET_AMOUNT_FIELDS = ['total', 'transportCost'];
const SPREADSHEET_ERROR_CELL_PATTERN = /^#(?:REF|VALUE|DIV\/0|NAME\?|N\/A|NULL|NUM)!?/i;
const SPREADSHEET_TOTAL_ROW_PATTERN = /^(?:grand\s*)?(?:total|subtotal|sum|totals)\b/i;
const SPREADSHEET_SINGLE_INVOICE_KEY = '__single_invoice__';
const SPREADSHEET_MAX_HEADER_SCAN_ROWS = 40;
const SPREADSHEET_MAX_BLANK_STREAK = 10;

function extractSpreadsheetInvoiceData(spreadsheetBlob, fileName, source) {
  const tempIds = [];
  try {
    const converted = Drive.Files.create({
      name: `${stripInvoiceExtension(fileName)} - Temp Processing Sheet`,
      mimeType: GOOGLE_SHEETS_MIME_TYPE
    }, spreadsheetBlob);
    if (!converted || !converted.id) {
      throw new Error('Spreadsheet conversion did not return a file id.');
    }

    tempIds.push(converted.id);
    const spreadsheet = waitForSpreadsheetOpen(converted.id, 'spreadsheet invoice conversion');
    const invoiceMeta = extractSpreadsheetInvoiceMeta(spreadsheet, fileName);
    const table = findInvoiceTable(spreadsheet);

    if (!table) {
      throw new Error('No invoice line-item table found. Expected a header row with an amount column (Total / Amount / Transport Cost) plus at least two of: PO, Invoice #, Delivery Date, Pickup Location, Dropoff Location.');
    }

    const lineRows = extractSpreadsheetLineRows(table);
    if (lineRows.rows.length === 0) {
      throw new Error(`No invoice rows found beneath the header on "${table.sheetName}".`);
    }

    const invoices = buildSpreadsheetInvoices(lineRows.rows, invoiceMeta, fileName);
    const reconciliation = reconcileSpreadsheetTotals(invoices, invoiceMeta, lineRows);
    const carrierType = resolveSpreadsheetCarrier(lineRows.rows, invoiceMeta);
    const codeSheetBlob = generateSpreadsheetSplitHtmlPdf(invoices, invoiceMeta, carrierType, reconciliation, fileName);
    const originalPdfBlob = exportSpreadsheetAsPdf(converted.id, fileName);

    const logRows = [];
    invoices.forEach(function(invoice) {
      buildSpreadsheetLogRows(invoice, fileName, source).forEach(function(row) {
        logRows.push(row);
      });
    });

    return {
      carrierType: applyCarrierTypeAutoFix(carrierType),
      codeSheetBlob: codeSheetBlob,
      originalPdfBlob: originalPdfBlob,
      invoices: invoices,
      reconciliation: reconciliation,
      sheetName: table.sheetName,
      codingSummary: buildSpreadsheetCodingSummary(invoices),
      logRows: logRows
    };
  } finally {
    tempIds.forEach(function(id) {
      try { DriveApp.getFileById(id).setTrashed(true); } catch (error) {}
    });
  }
}

/**
 * Header/summary values that live outside the line-item table: carrier details,
 * the invoice number, and the total the workbook itself claims to be due.
 */
function extractSpreadsheetInvoiceMeta(spreadsheet, fileName) {
  const invoiceSheet = findSheetByNamePattern(spreadsheet, /invoice|summary|cover/i) || spreadsheet.getSheets()[0];
  const values = invoiceSheet ? invoiceSheet.getDataRange().getDisplayValues() : [];

  const carrierType = applyCarrierTypeAutoFix(
    findLabelValue(values, /^carrier\s*(?:name)?\s*:?$/i) ||
    findLabelValue(values, /^vendor\s*(?:name)?\s*:?$/i) ||
    stripInvoiceExtension(fileName)
  );
  const carrierEmail = findLabelValue(values, /^e-?mail\s*:?$/i) || '';
  const carrierAddress = [
    findLabelValue(values, /^carrier\s*address\s*:?$/i),
    findLabelValue(values, /^address\s*:?$/i),
    findLabelValue(values, /^remit\s*(?:to)?\s*:?$/i)
  ].filter(Boolean)[0] || '';

  // Prefer the number the workbook states over one guessed from the filename.
  const declaredInvoiceNumber =
    findLabelValue(values, /^invoice\s*#\s*:?$/i) ||
    findLabelValue(values, /^invoice\s*(?:no|num|number)\.?\s*[#:]?\s*:?$/i) ||
    findLabelValue(values, /^(?:inv|bill)\s*(?:no|num|number|#)\.?\s*:?$/i) ||
    '';
  const invoiceNumber = declaredInvoiceNumber ||
    extractInvoiceNumberFromFileName(fileName) ||
    MISSING_VALUE_LABEL;

  const invoiceDate =
    findLabelValue(values, /^invoice\s*date\s*:?$/i) ||
    findLabelValue(values, /^date\s*:?$/i) ||
    '';

  const declaredTotalRaw =
    findLabelValue(values, /^total\s*amount\s*due\s*:?$/i) ||
    findLabelValue(values, /^(?:grand\s*)?total\s*(?:due)?\s*:?$/i) ||
    findLabelValue(values, /^amount\s*due\s*:?$/i) ||
    '';
  const declaredTotal = parseMoneyToken(declaredTotalRaw);

  return {
    carrierType: carrierType,
    carrierEmail: carrierEmail,
    carrierAddress: carrierAddress,
    invoiceNumber: invoiceNumber,
    invoiceDate: invoiceDate,
    declaredTotal: declaredTotal,
    declaredTotalRaw: declaredTotalRaw
  };
}

/**
 * Find the line-item table anywhere in the workbook.
 *
 * Every row of every sheet is scored on how many known invoice columns it
 * names; the best-scoring row that is followed by data wins. This replaces the
 * old exact-match requirement on a tab called "Shifts".
 */
function findInvoiceTable(spreadsheet) {
  const sheets = spreadsheet.getSheets();
  let best = null;

  for (let s = 0; s < sheets.length; s++) {
    const sheet = sheets[s];
    let range;
    try {
      range = sheet.getDataRange();
    } catch (error) {
      continue;
    }

    const displayValues = range.getDisplayValues();
    if (displayValues.length < 2) continue;
    const rawValues = range.getValues();
    const scanLimit = Math.min(displayValues.length - 1, SPREADSHEET_MAX_HEADER_SCAN_ROWS);

    for (let r = 0; r < scanLimit; r++) {
      const headerMap = mapInvoiceTableColumns(displayValues[r] || []);
      const fieldNames = Object.keys(headerMap);
      if (fieldNames.length < 3) continue;

      // Without an amount column there is nothing to code or reconcile.
      let hasAmount = false;
      for (let a = 0; a < SPREADSHEET_AMOUNT_FIELDS.length; a++) {
        if (headerMap[SPREADSHEET_AMOUNT_FIELDS[a]] !== undefined) { hasAmount = true; break; }
      }
      if (!hasAmount) continue;

      // The header must actually be followed by data.
      let hasDataBelow = false;
      for (let d = r + 1; d < Math.min(displayValues.length, r + 6); d++) {
        if ((displayValues[d] || []).join('').trim() !== '') { hasDataBelow = true; break; }
      }
      if (!hasDataBelow) continue;

      const score = fieldNames.length + (headerMap.invoiceNumber !== undefined ? 2 : 0);
      if (!best || score > best.score) {
        best = {
          score: score,
          sheetName: sheet.getName(),
          headerIndex: r,
          headerMap: headerMap,
          displayValues: displayValues,
          rawValues: rawValues
        };
      }
      break; // one header row per sheet is enough
    }
  }

  return best;
}

/**
 * Map a header row onto canonical field names.
 * The first column claiming a field keeps it, so a stray later column cannot
 * hijack an already-identified one.
 */
function mapInvoiceTableColumns(headerRow) {
  const map = {};
  const fields = Object.keys(SPREADSHEET_COLUMN_SYNONYMS);

  (headerRow || []).forEach(function(header, index) {
    const normalized = normalizeHeaderValue(header);
    if (!normalized) return;

    for (let f = 0; f < fields.length; f++) {
      const field = fields[f];
      if (map[field] !== undefined) continue;
      if (SPREADSHEET_COLUMN_SYNONYMS[field].indexOf(normalized) >= 0) {
        map[field] = index;
        return;
      }
    }
  });

  return map;
}

/**
 * Read the data rows beneath a table header.
 *
 * Returns { rows, errorCells, skippedTotalRows }. Rows carrying spreadsheet
 * error values are kept but marked, so a broken formula shows up as a review
 * flag instead of quietly contributing zero to the invoice total.
 */
function extractSpreadsheetLineRows(table) {
  const displayValues = table.displayValues;
  const rawValues = table.rawValues;
  const headerMap = table.headerMap;
  const rows = [];
  const errorCells = [];
  let skippedTotalRows = 0;
  let blankStreak = 0;

  for (let rowIndex = table.headerIndex + 1; rowIndex < displayValues.length; rowIndex++) {
    const displayRow = displayValues[rowIndex] || [];
    const rawRow = rawValues[rowIndex] || [];

    if (displayRow.join('').trim() === '') {
      blankStreak += 1;
      if (rows.length > 0 && blankStreak >= SPREADSHEET_MAX_BLANK_STREAK) break;
      continue;
    }
    blankStreak = 0;

    // "Total" / "Grand Total" footer rows restate the sum; counting them would
    // double the invoice.
    let firstCell = '';
    for (let c = 0; c < displayRow.length; c++) {
      const cellText = String(displayRow[c] || '').trim();
      if (cellText) { firstCell = cellText; break; }
    }
    if (SPREADSHEET_TOTAL_ROW_PATTERN.test(firstCell)) {
      skippedTotalRows += 1;
      continue;
    }

    const amount = readSpreadsheetRowAmount(displayRow, rawRow, headerMap);
    if (amount.errorText) {
      errorCells.push({ row: rowIndex + 1, value: amount.errorText });
    }

    const row = {
      rowNumber: rowIndex + 1,
      invoiceNumber: getCellByIndex(displayRow, headerMap.invoiceNumber),
      deliveryDate: headerMap.deliveryDate === undefined
        ? ''
        : formatSheetDate(getRawCellByIndex(rawRow, headerMap.deliveryDate), getCellByIndex(displayRow, headerMap.deliveryDate)),
      shipDate: headerMap.shipDate === undefined
        ? ''
        : formatSheetDate(getRawCellByIndex(rawRow, headerMap.shipDate), getCellByIndex(displayRow, headerMap.shipDate)),
      shipmentNumber: getCellByIndex(displayRow, headerMap.shipmentNumber),
      po: getCellByIndex(displayRow, headerMap.po),
      pickupLocation: getCellByIndex(displayRow, headerMap.pickupLocation),
      dropoffLocation: getCellByIndex(displayRow, headerMap.dropoffLocation),
      carrier: getCellByIndex(displayRow, headerMap.carrier),
      description: getCellByIndex(displayRow, headerMap.description),
      totalAmount: amount.value === null ? 0 : amount.value,
      amountIsError: !!amount.errorText,
      amountMissing: amount.value === null
    };

    const hasIdentity = row.invoiceNumber || row.po || row.shipmentNumber ||
      row.pickupLocation || row.dropoffLocation;
    if (!hasIdentity && amount.value === null) {
      continue;
    }

    rows.push(row);
  }

  return { rows: rows, errorCells: errorCells, skippedTotalRows: skippedTotalRows };
}

/**
 * The amount for one line: the explicit total column when present, otherwise
 * transport cost plus any accessorial column.
 * Returns { value, errorText } — value is null when nothing usable was found.
 */
function readSpreadsheetRowAmount(displayRow, rawRow, headerMap) {
  const readCell = function(index) {
    if (index === undefined) return { value: null, errorText: null };
    const display = String(getCellByIndex(displayRow, index) || '').trim();
    if (SPREADSHEET_ERROR_CELL_PATTERN.test(display)) {
      return { value: null, errorText: display };
    }
    const raw = getRawCellByIndex(rawRow, index);
    if (typeof raw === 'number' && !isNaN(raw)) {
      return { value: raw, errorText: null };
    }
    if (typeof raw === 'string' && SPREADSHEET_ERROR_CELL_PATTERN.test(raw.trim())) {
      return { value: null, errorText: raw.trim() };
    }
    return { value: parseMoneyToken(display), errorText: null };
  };

  const totalCell = readCell(headerMap.total);
  if (totalCell.value !== null) {
    return totalCell;
  }

  const transportCell = readCell(headerMap.transportCost);
  const accessorialCell = readCell(headerMap.accessorials);
  if (transportCell.value !== null || accessorialCell.value !== null) {
    return {
      value: (transportCell.value || 0) + (accessorialCell.value || 0),
      errorText: totalCell.errorText || transportCell.errorText || accessorialCell.errorText
    };
  }

  return {
    value: null,
    errorText: totalCell.errorText || transportCell.errorText || accessorialCell.errorText
  };
}

/**
 * Split the line rows into invoices.
 *
 * With an invoice-number column, each distinct number is its own invoice — this
 * is the "one excel file, many invoices" case. Without one, the whole table is
 * a single invoice, as before.
 */
function buildSpreadsheetInvoices(rows, invoiceMeta, fileName) {
  const grouped = {};
  const order = [];

  rows.forEach(function(row) {
    const key = String(row.invoiceNumber || '').trim() || SPREADSHEET_SINGLE_INVOICE_KEY;
    if (!grouped[key]) {
      grouped[key] = [];
      order.push(key);
    }
    grouped[key].push(row);
  });

  const isMulti = order.length > 1;

  return order.map(function(key) {
    const invoiceRows = grouped[key];
    const invoiceNumber = key === SPREADSHEET_SINGLE_INVOICE_KEY ? invoiceMeta.invoiceNumber : key;
    const groups = groupSpreadsheetRowsByCoding(invoiceRows, invoiceMeta, fileName);
    return buildSpreadsheetInvoiceSummary(invoiceMeta, groups, invoiceNumber, invoiceRows, isMulti);
  });
}

/**
 * Compare what the rows add up to against the total the workbook declares.
 * A mismatch is the single most useful signal that a spreadsheet invoice was
 * read wrongly, so it is surfaced rather than swallowed.
 */
function reconcileSpreadsheetTotals(invoices, invoiceMeta, lineRows) {
  const computedTotal = invoices.reduce(function(sum, invoice) {
    return sum + Number(invoice.totalAmount || 0);
  }, 0);

  const issues = [];
  if (lineRows.errorCells.length > 0) {
    issues.push(`${lineRows.errorCells.length} row(s) contain spreadsheet errors (${lineRows.errorCells.slice(0, 3).map(function(cell) {
      return `row ${cell.row}: ${cell.value}`;
    }).join('; ')})`);
  }

  let declaredTotal = null;
  let difference = null;
  let matches = null;

  if (invoiceMeta.declaredTotal !== null && invoiceMeta.declaredTotal !== undefined) {
    declaredTotal = invoiceMeta.declaredTotal;
    difference = Number((computedTotal - declaredTotal).toFixed(2));
    matches = Math.abs(difference) < 0.01;
    if (!matches) {
      issues.push(`Line items total ${formatAmountNumber(computedTotal)} but the invoice declares ${formatAmountNumber(declaredTotal)} (difference ${formatAmountNumber(difference)})`);
    }
  }

  return {
    computedTotal: computedTotal,
    declaredTotal: declaredTotal,
    difference: difference,
    matches: matches,
    errorCells: lineRows.errorCells,
    skippedTotalRows: lineRows.skippedTotalRows,
    issues: issues,
    needsReview: issues.length > 0
  };
}

/**
 * A register listing several carriers cannot be filed under one of them; fall
 * back to the workbook's stated carrier in that case.
 */
function resolveSpreadsheetCarrier(rows, invoiceMeta) {
  const seen = {};
  rows.forEach(function(row) {
    const carrier = String(row.carrier || '').trim();
    if (carrier) seen[carrier] = true;
  });
  const names = Object.keys(seen);
  return names.length === 1 ? names[0] : invoiceMeta.carrierType;
}

function buildSpreadsheetCodingSummary(invoices) {
  return invoices.map(function(invoice) {
    const groupText = invoice.groups.map(function(group) {
      return `${group.rdcCode}: ${group.coding} ($${formatAmountNumber(group.totalAmount)})`;
    }).join(' | ');
    return invoices.length > 1 ? `${invoice.invoiceNumber}: ${groupText}` : groupText;
  }).join('  ||  ');
}

function groupSpreadsheetRowsByCoding(rows, invoiceMeta, fileName) {
  const grouped = {};

  rows.forEach(function(row) {
    const coding = determineCoding([
      invoiceMeta.carrierType,
      row.carrier,
      row.pickupLocation,
      row.dropoffLocation,
      row.description,
      row.po,
      fileName
    ].filter(Boolean).join(' '));
    const key = coding;
    if (!grouped[key]) {
      grouped[key] = {
        coding: coding,
        rdcCode: extractRdcCodeFromCoding(coding),
        totalAmount: 0,
        shipmentCount: 0,
        hasErrors: false,
        poMap: {},
        pickupMap: {},
        dropoffMap: {},
        deliveryDateMap: {}
      };
    }

    grouped[key].totalAmount += Number(row.totalAmount || 0);
    grouped[key].shipmentCount += 1;
    if (row.amountIsError || row.amountMissing) grouped[key].hasErrors = true;
    if (row.po) grouped[key].poMap[row.po] = true;
    if (row.pickupLocation) grouped[key].pickupMap[row.pickupLocation] = true;
    if (row.dropoffLocation) grouped[key].dropoffMap[row.dropoffLocation] = true;
    if (row.deliveryDate) grouped[key].deliveryDateMap[row.deliveryDate] = true;
  });

  return Object.keys(grouped)
    .sort()
    .map(function(key) {
      const group = grouped[key];
      group.poNumbers = Object.keys(group.poMap).sort();
      group.pickupLocations = Object.keys(group.pickupMap).sort();
      group.dropoffLocations = Object.keys(group.dropoffMap).sort();
      group.deliveryDates = Object.keys(group.deliveryDateMap).sort();
      delete group.poMap;
      delete group.pickupMap;
      delete group.dropoffMap;
      delete group.deliveryDateMap;
      return group;
    });
}

function buildSpreadsheetInvoiceSummary(invoiceMeta, groups, invoiceNumber, rows, isMulti) {
  const totalAmount = groups.reduce(function(sum, group) {
    return sum + Number(group.totalAmount || 0);
  }, 0);
  const allPOs = {};
  const allOrigins = {};
  const allDestinations = {};
  const allDeliveryDates = {};
  const allShipDates = {};

  groups.forEach(function(group) {
    group.poNumbers.forEach(function(po) { allPOs[po] = true; });
    group.pickupLocations.forEach(function(location) { allOrigins[location] = true; });
    group.dropoffLocations.forEach(function(location) { allDestinations[location] = true; });
    group.deliveryDates.forEach(function(date) { allDeliveryDates[date] = true; });
  });
  (rows || []).forEach(function(row) {
    if (row.shipDate && row.shipDate !== MISSING_VALUE_LABEL) allShipDates[row.shipDate] = true;
  });

  const poNumbers = Object.keys(allPOs).sort();
  const origins = Object.keys(allOrigins).sort();
  const destinations = Object.keys(allDestinations).sort();
  const deliveryDates = Object.keys(allDeliveryDates).sort();
  const shipDates = Object.keys(allShipDates).sort();
  const hasErrors = groups.some(function(group) { return group.hasErrors; });

  const remitBits = [];
  if (invoiceMeta.carrierAddress) remitBits.push(invoiceMeta.carrierAddress);
  if (invoiceMeta.carrierEmail) remitBits.push(invoiceMeta.carrierEmail);
  remitBits.push(`Split across ${groups.length} RDC code${groups.length === 1 ? '' : 's'}`);
  if (isMulti) {
    remitBits.push('One of several invoices in this workbook');
  }

  return {
    carrierType: invoiceMeta.carrierType,
    invoiceNumber: invoiceNumber || invoiceMeta.invoiceNumber,
    totalAmount: totalAmount,
    rowCount: (rows || []).length,
    hasErrors: hasErrors,
    groups: groups,
    invoiceData: {
      invoiceNumber: invoiceNumber || invoiceMeta.invoiceNumber,
      po: summarizeValues(poNumbers, 6),
      shipDate: summarizeDateValues(shipDates),
      deliveryDate: summarizeDateValues(deliveryDates),
      amount: formatAmountNumber(totalAmount),
      amountValue: totalAmount,
      // Spreadsheet totals are arithmetic on real cells, not a read of printed
      // text, so they are only ever doubted when reconciliation says so.
      amountConfidence: hasErrors ? 'low' : 'high',
      origin: summarizeValues(origins, 3) || 'Review Required',
      destination: groups.length > 1
        ? `Multiple RDCs (${groups.length})`
        : (summarizeValues(destinations, 3) || 'Review Required'),
      productType: 'Spreadsheet Invoice',
      remitInfo: remitBits.join('\n')
    }
  };
}

function buildSpreadsheetLogRows(invoiceSummary, fileName, source) {
  return invoiceSummary.groups.map(function(group) {
    return [
      new Date(),
      source || 'Spreadsheet Invoice',
      fileName,
      invoiceSummary.invoiceNumber,
      summarizeValues(group.poNumbers, 12),
      MISSING_VALUE_LABEL,
      summarizeDateValues(group.deliveryDates),
      Number(group.totalAmount || 0),
      summarizeValues(group.pickupLocations, 3) || 'Review Required',
      'Spreadsheet Invoice',
      summarizeValues(group.dropoffLocations, 3) || 'Review Required',
      `Shipments: ${group.shipmentCount}`,
      group.coding
    ];
  });
}

/**
 * The coding summary PDF that gets emailed and filed alongside the original.
 * Lists every invoice found in the workbook, split by RDC code, and prints any
 * reconciliation problem at the top where it cannot be missed.
 */
function generateSpreadsheetSplitHtmlPdf(invoices, invoiceMeta, carrierType, reconciliation, originalName) {
  const safeCarrier = escapeHtml(carrierType);
  const safeOriginalName = escapeHtml(originalName);
  const isMulti = invoices.length > 1;
  const grandTotal = invoices.reduce(function(sum, invoice) {
    return sum + Number(invoice.totalAmount || 0);
  }, 0);

  const rowsHtml = invoices.map(function(invoice) {
    return invoice.groups.map(function(group) {
      return `
      <tr>
        <td style="padding: 9px 8px; border-bottom: 1px solid #ddd;">${escapeHtml(invoice.invoiceNumber)}</td>
        <td style="padding: 9px 8px; border-bottom: 1px solid #ddd;">${escapeHtml(group.rdcCode)}</td>
        <td style="padding: 9px 8px; border-bottom: 1px solid #ddd; font-weight: bold; color: #0b57d0;">${escapeHtml(group.coding)}</td>
        <td style="padding: 9px 8px; border-bottom: 1px solid #ddd;">${group.shipmentCount}</td>
        <td style="padding: 9px 8px; border-bottom: 1px solid #ddd; text-align: right;">$${escapeHtml(formatAmountNumber(group.totalAmount))}</td>
        <td style="padding: 9px 8px; border-bottom: 1px solid #ddd;">${escapeHtml(summarizeValues(group.poNumbers, 8))}</td>
        <td style="padding: 9px 8px; border-bottom: 1px solid #ddd;">${escapeHtml(summarizeValues(group.dropoffLocations, 3))}</td>
      </tr>
    `;
    }).join('');
  }).join('');

  const warningHtml = (reconciliation && reconciliation.issues.length > 0)
    ? `
      <div style="margin-top: 16px; padding: 12px 14px; border-left: 4px solid #b3261e; background: #fce8e6; color: #7a1a12;">
        <strong>Check before posting</strong>
        <ul style="margin: 8px 0 0; padding-left: 18px;">
          ${reconciliation.issues.map(function(issue) { return `<li>${escapeHtml(issue)}</li>`; }).join('')}
        </ul>
      </div>
    `
    : '';

  const declaredRowHtml = (reconciliation && reconciliation.declaredTotal !== null)
    ? `
        <tr>
          <td style="padding: 10px 8px; background: #f5f7fa;"><strong>Declared on Invoice</strong></td>
          <td style="padding: 10px 8px;">$${escapeHtml(formatAmountNumber(reconciliation.declaredTotal))}${reconciliation.matches ? ' <span style="color:#137333;">(reconciled)</span>' : ' <span style="color:#b3261e;">(does not match line items)</span>'}</td>
        </tr>
    `
    : '';

  const htmlContent = `
    <div style="font-family: Arial, sans-serif; padding: 28px; color: #222;">
      <h1 style="margin: 0 0 12px; color: #0b57d0; border-bottom: 2px solid #0b57d0; padding-bottom: 10px;">Spreadsheet Invoice Coding Summary</h1>
      ${warningHtml}
      <table style="width: 100%; border-collapse: collapse; margin-top: 16px; font-size: 14px;">
        <tr>
          <td style="padding: 10px 8px; background: #f5f7fa; width: 28%;"><strong>Carrier</strong></td>
          <td style="padding: 10px 8px;">${safeCarrier}</td>
        </tr>
        <tr>
          <td style="padding: 10px 8px; background: #f5f7fa;"><strong>Invoices in File</strong></td>
          <td style="padding: 10px 8px;">${invoices.length}${isMulti ? ' (multi-invoice workbook)' : ''}</td>
        </tr>
        <tr>
          <td style="padding: 10px 8px; background: #f5f7fa;"><strong>Total Amount</strong></td>
          <td style="padding: 10px 8px; font-weight: bold;">$${escapeHtml(formatAmountNumber(grandTotal))}</td>
        </tr>
        ${declaredRowHtml}
        <tr>
          <td style="padding: 10px 8px; background: #f5f7fa;"><strong>Source File</strong></td>
          <td style="padding: 10px 8px;">${safeOriginalName}</td>
        </tr>
      </table>

      <h2 style="margin-top: 28px; color: #222;">Coding Split</h2>
      <table style="width: 100%; border-collapse: collapse; margin-top: 10px; font-size: 13px;">
        <thead>
          <tr style="background: #e8f0fe; text-align: left;">
            <th style="padding: 10px 8px;">Invoice #</th>
            <th style="padding: 10px 8px;">RDC</th>
            <th style="padding: 10px 8px;">Coding</th>
            <th style="padding: 10px 8px;">Shipments</th>
            <th style="padding: 10px 8px; text-align: right;">Amount</th>
            <th style="padding: 10px 8px;">POs</th>
            <th style="padding: 10px 8px;">Destinations</th>
          </tr>
        </thead>
        <tbody>${rowsHtml}</tbody>
        <tfoot>
          <tr style="background: #f5f7fa; font-weight: bold;">
            <td style="padding: 10px 8px;" colspan="4">Grand Total</td>
            <td style="padding: 10px 8px; text-align: right;">$${escapeHtml(formatAmountNumber(grandTotal))}</td>
            <td style="padding: 10px 8px;" colspan="2"></td>
          </tr>
        </tfoot>
      </table>
    </div>
  `;

  return Utilities.newBlob(htmlContent, MimeType.HTML, 'spreadsheet-summary.html')
    .getAs(MimeType.PDF)
    .setName('Coded_Summary_' + stripInvoiceExtension(originalName) + '.pdf');
}

function exportSpreadsheetAsPdf(fileId, fileName) {
  const token = ScriptApp.getOAuthToken();
  const exportUrl = 'https://docs.google.com/spreadsheets/d/' + fileId +
    '/export?format=pdf&size=letter&portrait=true&fitw=true&gridlines=false&printtitle=false&sheetnames=false&fzr=false';
  const response = UrlFetchApp.fetch(exportUrl, {
    headers: { Authorization: 'Bearer ' + token },
    muteHttpExceptions: true
  });

  if (response.getResponseCode() !== 200) {
    throw new Error('Spreadsheet PDF export returned HTTP ' + response.getResponseCode());
  }

  return response.getBlob().setName(stripInvoiceExtension(fileName) + '.pdf');
}

function findSheetByNamePattern(spreadsheet, pattern) {
  const sheets = spreadsheet.getSheets();
  for (let i = 0; i < sheets.length; i++) {
    if (pattern.test(sheets[i].getName())) {
      return sheets[i];
    }
  }
  return null;
}

function findLabelValue(values, labelPattern) {
  // Invoice tabs put their totals at the bottom, well past the old 50-row cap.
  const maxRows = Math.min(values.length, 400);

  for (let row = 0; row < maxRows; row++) {
    const currentRow = values[row] || [];
    for (let col = 0; col < currentRow.length; col++) {
      const cell = String(currentRow[col] || '').trim();
      if (!cell || !labelPattern.test(cell)) {
        continue;
      }

      for (let offset = 1; offset <= 3; offset++) {
        const sameRowValue = String(currentRow[col + offset] || '').trim();
        if (sameRowValue && !isSpreadsheetLabelCell(sameRowValue)) {
          return sameRowValue;
        }
      }

      for (let rowOffset = 1; rowOffset <= 3 && row + rowOffset < values.length; rowOffset++) {
        const nextRow = values[row + rowOffset] || [];
        const belowValue = String(nextRow[col] || nextRow[col + 1] || '').trim();
        if (belowValue && !isSpreadsheetLabelCell(belowValue)) {
          return belowValue;
        }
      }
    }
  }
  return '';
}

/**
 * A cell that is itself a caption ("Send Invoice To:") is never the value of
 * the label above or beside it.
 */
function isSpreadsheetLabelCell(value) {
  return /:\s*$/.test(String(value || ''));
}



function normalizeHeaderValue(value) {
  return String(value || '').toLowerCase().replace(/[^a-z0-9]+/g, '');
}

function getCellByIndex(row, index) {
  if (index === undefined || index === null || index < 0) {
    return '';
  }
  return row[index] === undefined || row[index] === null ? '' : String(row[index]).trim();
}

function getRawCellByIndex(row, index) {
  if (index === undefined || index === null || index < 0) {
    return '';
  }
  return row[index] === undefined || row[index] === null ? '' : row[index];
}

/**
 * First usable amount among the supplied candidates (raw cell value first, then
 * its displayed text). Returns 0 when nothing parses, matching the old
 * behaviour for callers that just want a number to add up.
 */
function parseAmountValue() {
  for (let i = 0; i < arguments.length; i++) {
    const parsed = parseMoneyToken(arguments[i]);
    if (parsed !== null) {
      return parsed;
    }
  }
  return 0;
}

function formatSheetDate(rawValue, displayValue) {
  if (Object.prototype.toString.call(rawValue) === '[object Date]' && !isNaN(rawValue.getTime())) {
    return Utilities.formatDate(rawValue, Session.getScriptTimeZone(), 'MM/dd/yyyy');
  }
  const display = String(displayValue || '').trim();
  if (display) {
    return display;
  }
  if (typeof rawValue === 'number' && !isNaN(rawValue)) {
    return Utilities.formatDate(new Date(Math.round((rawValue - 25569) * 86400 * 1000)), Session.getScriptTimeZone(), 'MM/dd/yyyy');
  }
  return MISSING_VALUE_LABEL;
}

function summarizeValues(values, limit) {
  const items = (values || []).filter(Boolean);
  if (items.length === 0) {
    return '';
  }
  const maxItems = Math.max(1, Number(limit) || items.length);
  if (items.length <= maxItems) {
    return items.join(', ');
  }
  return items.slice(0, maxItems).join(', ') + ` +${items.length - maxItems} more`;
}

function summarizeDateValues(values) {
  const items = (values || []).filter(Boolean).sort();
  if (items.length === 0) {
    return MISSING_VALUE_LABEL;
  }
  if (items.length === 1) {
    return items[0];
  }
  return `${items[0]} - ${items[items.length - 1]}`;
}

function formatAmountNumber(value) {
  const amount = Number(value || 0);
  return amount.toFixed(2);
}

function extractRdcCodeFromCoding(coding) {
  const match = String(coding || '').match(/^\s*[^,]+,\s*([^,]+)/);
  return match && match[1] ? match[1].trim() : 'RDC-UNKNOWN';
}

function extractInvoiceNumberFromFileName(fileName) {
  const fileNameInv = String(fileName || '').match(/\b(INV[-#]?\d{4,})\b/i) ||
    String(fileName || '').match(/invoice[-_ ]([A-Z0-9]{4,})/i) ||
    String(fileName || '').match(/\b([A-Z]{2,4}\d{5,})\b/i);
  return fileNameInv ? fileNameInv[1] : '';
}

function stripInvoiceExtension(fileName) {
  return String(fileName || 'invoice').replace(/\.(pdf|xlsx|xlsm|xls|csv|zip)$/i, '').trim() || 'invoice';
}

function isPdfInvoiceFile(file) {
  return isPdfInvoiceMimeType(file.getMimeType(), file.getName());
}

function isSupportedInvoiceFile(file) {
  return isSupportedInvoiceMimeType(file.getMimeType(), file.getName());
}

function isSupportedInvoiceAttachment(attachment) {
  return isSupportedInvoiceMimeType(attachment.getContentType(), attachment.getName());
}

function isSupportedInvoiceMimeType(mimeType, fileName) {
  return isPdfInvoiceMimeType(mimeType, fileName) || isSpreadsheetInvoiceMimeType(mimeType, fileName);
}

/**
 * Everything the tool will pick up from a source folder or mailbox: invoices
 * themselves plus archives that contain them.
 */
function isIngestibleInvoiceMimeType(mimeType, fileName) {
  return isSupportedInvoiceMimeType(mimeType, fileName) || isArchiveMimeType(mimeType, fileName);
}

function isIngestibleInvoiceFile(file) {
  return isIngestibleInvoiceMimeType(file.getMimeType(), file.getName());
}

function isIngestibleInvoiceAttachment(attachment) {
  return isIngestibleInvoiceMimeType(attachment.getContentType(), attachment.getName());
}

function isPdfInvoiceMimeType(mimeType, fileName) {
  const normalizedMimeType = String(mimeType || '').toLowerCase();
  return normalizedMimeType === 'application/pdf' || /\.pdf$/i.test(String(fileName || ''));
}

const SPREADSHEET_MIME_TYPES = [
  XLSX_MIME_TYPE,
  GOOGLE_SHEETS_MIME_TYPE,
  'application/vnd.ms-excel',
  'application/vnd.ms-excel.sheet.macroenabled.12',
  'text/csv',
  'application/csv'
];

function isSpreadsheetInvoiceMimeType(mimeType, fileName) {
  const normalizedMimeType = String(mimeType || '').toLowerCase();
  if (SPREADSHEET_MIME_TYPES.indexOf(normalizedMimeType) >= 0) {
    return true;
  }
  return /\.(xlsx|xlsm|xls|csv)$/i.test(String(fileName || ''));
}

function generateHtmlPdf(data, coding, originalName) {
  const safe = {
    coding: escapeHtml(coding),
    invoiceNumber: escapeHtml(data.invoiceNumber),
    po: escapeHtml(data.po),
    shipDate: escapeHtml(data.shipDate),
    deliveryDate: escapeHtml(data.deliveryDate),
    origin: escapeHtml(data.origin),
    destination: escapeHtml(data.destination),
    productType: escapeHtml(data.productType),
    remitInfo: escapeHtml(data.remitInfo).replace(/\n/g, '<br>'),
    originalName: escapeHtml(originalName)
  };

  const amountNoDollar = (data.amount || '').replace(/\$/g, '').trim();
  const amountDisplay = amountNoDollar ? `$${escapeHtml(amountNoDollar)}` : MISSING_VALUE_LABEL;

  // The coding is what AP cannot reconstruct, so it is always trustworthy here.
  // An amount we could not tie to an explicit label still goes out, but it says
  // so on the face of the sheet rather than looking like a confirmed figure.
  const amountUncertain = isAmountUncertain(data);
  const amountBanner = amountUncertain
    ? `<div style="margin: 16px 0; padding: 12px 14px; border-left: 4px solid #d93025; background: #fce8e6; color: #7a1a12; font-size: 13px;">
         <strong>Check the amount before posting.</strong>
         The coding above is confirmed; the amount was read as <strong>${amountDisplay}</strong>${
           data.amountLabel ? ` near &ldquo;${escapeHtml(String(data.amountLabel).trim())}&rdquo;` : ''
         } and could not be tied to an explicit total on the invoice.
       </div>`
    : '';

  const htmlContent = `
    <div style="font-family: Arial, sans-serif; padding: 30px; color: #333;">
      <h1 style="border-bottom: 2px solid #4285F4; padding-bottom: 10px; color: #4285F4;">Invoice Processing Summary</h1>
      ${amountBanner}
      <table style="width: 100%; border-collapse: collapse; margin-top: 20px; font-size: 14px;">
        <tr>
          <td style="padding: 12px 8px; border-bottom: 1px solid #eee; width: 30%; background-color: #f8f9fa;"><strong>Coding Applied</strong></td>
          <td style="padding: 12px 8px; border-bottom: 1px solid #eee; font-weight: bold; color: #d93025; font-size: 16px;">${safe.coding}</td>
        </tr>
        <tr>
          <td style="padding: 12px 8px; border-bottom: 1px solid #eee; background-color: #f8f9fa;"><strong>Invoice #</strong></td>
          <td style="padding: 12px 8px; border-bottom: 1px solid #eee;">${safe.invoiceNumber}</td>
        </tr>
        <tr>
          <td style="padding: 12px 8px; border-bottom: 1px solid #eee; background-color: #f8f9fa;"><strong>PO #</strong></td>
          <td style="padding: 12px 8px; border-bottom: 1px solid #eee;">${safe.po}</td>
        </tr>
        <tr>
          <td style="padding: 12px 8px; border-bottom: 1px solid #eee; background-color: #f8f9fa;"><strong>Ship Date</strong></td>
          <td style="padding: 12px 8px; border-bottom: 1px solid #eee;">${safe.shipDate}</td>
        </tr>
        <tr>
          <td style="padding: 12px 8px; border-bottom: 1px solid #eee; background-color: #f8f9fa;"><strong>Delivery Date</strong></td>
          <td style="padding: 12px 8px; border-bottom: 1px solid #eee;">${safe.deliveryDate}</td>
        </tr>
        <tr>
          <td style="padding: 12px 8px; border-bottom: 1px solid #eee; background-color: #f8f9fa;"><strong>Amount</strong></td>
          <td style="padding: 12px 8px; border-bottom: 1px solid #eee; font-weight: bold;">${amountDisplay}</td>
        </tr>
        <tr>
          <td style="padding: 12px 8px; border-bottom: 1px solid #eee; background-color: #f8f9fa;"><strong>Origin</strong></td>
          <td style="padding: 12px 8px; border-bottom: 1px solid #eee;">${safe.origin}</td>
        </tr>
        <tr>
          <td style="padding: 12px 8px; border-bottom: 1px solid #eee; background-color: #f8f9fa;"><strong>Destination</strong></td>
          <td style="padding: 12px 8px; border-bottom: 1px solid #eee;">${safe.destination}</td>
        </tr>
        <tr>
          <td style="padding: 12px 8px; border-bottom: 1px solid #eee; background-color: #f8f9fa;"><strong>Product Type</strong></td>
          <td style="padding: 12px 8px; border-bottom: 1px solid #eee;">${safe.productType}</td>
        </tr>
        <tr>
          <td style="padding: 12px 8px; border-bottom: 1px solid #eee; background-color: #f8f9fa;"><strong>Remittance Info</strong></td>
          <td style="padding: 12px 8px; border-bottom: 1px solid #eee; color: #555;">${safe.remitInfo}</td>
        </tr>
      </table>
      <p style="margin-top: 40px; font-size: 12px; color: #999; text-align: center;">
        Document automatically generated by Google Apps Script<br>
        Original File: ${safe.originalName}
      </p>
    </div>
  `;

  const blob = Utilities.newBlob(htmlContent, MimeType.HTML, 'temp.html');
  return blob.getAs(MimeType.PDF).setName('Coded_Summary_' + originalName);
}

/* ═══════════════════════════════════════════════════════════════════════════
 * MONEY PARSING & AMOUNT SELECTION
 *
 * Invoice totals used to be picked with "first string that looks like money",
 * which happily returned a line-item rate, a quantity, a weight or a zip code.
 * Everything below replaces that with an explicit candidate model: find every
 * money-shaped token, read the label sitting in front of it, score it, and
 * keep the best one along with a confidence rating so weak reads can be sent
 * to manual review instead of being logged as fact.
 * ═══════════════════════════════════════════════════════════════════════════ */

// Labels that sit immediately in front of a number, strongest first. The first
// rule that matches the text preceding a number decides its base score.
const AMOUNT_LABEL_RULES = [
  {
    score: 120,
    name: 'invoice-total',
    pattern: /\b(?:total\s+amount\s+due|amount\s+due|amount\s+payable|balance\s+due|total\s+due|total\s+payable|invoice\s+total|total\s+invoice(?:\s+amount)?|grand\s+total|net\s+(?:amount\s+)?due|please\s+pay(?:\s+this\s+amount)?|pay\s+this\s+amount|total\s+charges?)\b/i
  },
  {
    score: 80,
    name: 'total',
    pattern: /\b(?:total|net\s+total|invoice\s+amount|amount)\b/i
  },
  {
    score: 45,
    name: 'line-item',
    pattern: /\b(?:sub\s*-?\s*total|line\s*haul|linehaul|freight(?:\s+charges?)?|transport(?:ation)?\s+cost|carrier\s+charges?|charges?|fuel(?:\s+surcharge)?|surcharge|accessorial|detention|layover|lumper|stop\s*off|tolls?|rate|cost|price|fee)\b/i
  }
];

// A number preceded by one of these is a measurement, an identifier or a term —
// never the invoice total. Anchored to the end so only the *nearest* label counts.
const AMOUNT_REJECT_LABEL_PATTERN = /\b(?:weights?|wgt|gross|net\s+weight|lbs?|kgs?|pounds?|kilos?|qty|quantity|pieces?|pcs|pallets?|cases?|units?|cartons?|skids?|miles?|mileage|temp(?:erature)?|degrees?|zip|postal(?:\s+code)?|phone|fax|tel(?:ephone)?|mobile|cell|account(?:\s*(?:no|number|#))?|routing|aba|swift|iban|ein|tax\s*id|vat(?:\s*(?:no|number|#))?|terms|net|page|suite|ste|box|load|bol|pro|po|purchase\s+order|invoice|shipment|pickup|delivery|reference|ref|quote|trailer|truck|container|seal|order|customer|vendor|driver|year|percent|per\s+(?:mile|lb|cwt|hour|unit)|hours?|days?)\s*(?:no\.?|number|#|:)?\s*$/i;

// Units that follow a number and prove it is not currency.
const AMOUNT_REJECT_SUFFIX_PATTERN = /^\s*(?:%|lbs?\b|kgs?\b|pounds?\b|miles?\b|pcs\b|pieces?\b|units?\b|cases?\b|pallets?\b|cartons?\b|gal(?:lons?)?\b|hrs?\b|hours?\b|days?\b|°|deg\b)/i;

const AMOUNT_CURRENCY_BEFORE_PATTERN = /(?:[$€£]|\b(?:USD|US\$|CAD|EUR|GBP)\s*)\s*$/i;
const AMOUNT_CURRENCY_AFTER_PATTERN = /^\s*(?:USD|CAD|EUR|GBP|dollars?)\b/i;

/**
 * Convert a money-looking string into a Number.
 *
 * Handles currency prefixes/suffixes, thousands separators, parenthesised and
 * trailing-minus credits, "CR" markers, and European `1.234,56` formatting.
 * Returns null when the text cannot be read as an amount, so callers can tell
 * "no amount" apart from "an amount of zero".
 */
function parseMoneyToken(rawValue) {
  if (rawValue === null || rawValue === undefined) {
    return null;
  }
  if (typeof rawValue === 'number') {
    return isNaN(rawValue) ? null : rawValue;
  }

  var text = String(rawValue).trim();
  if (!text) {
    return null;
  }

  var negative = false;

  if (/^\(.*\)$/.test(text)) {
    negative = true;
    text = text.slice(1, -1);
  }
  if (/\b(?:cr|credit)\s*$/i.test(text)) {
    negative = true;
    text = text.replace(/\b(?:cr|credit)\s*$/i, '');
  }
  if (/-\s*$/.test(text)) {
    negative = true;
    text = text.replace(/-\s*$/, '');
  }

  text = text
    .replace(/\b(?:USD|US\$|CAD|EUR|GBP|dollars?)\b/gi, '')
    .replace(/[$€£]/g, '')
    .replace(/\s+/g, '');

  if (/^-/.test(text)) {
    negative = true;
    text = text.replace(/^-+/, '');
  }

  if (!/^[0-9.,]+$/.test(text) || !/\d/.test(text)) {
    return null;
  }

  if (/^\d{1,3}(?:\.\d{3})+,\d{1,2}$/.test(text)) {
    // European grouping: 1.234.567,89
    text = text.replace(/\./g, '').replace(',', '.');
  } else if (/^\d+,\d{1,2}$/.test(text)) {
    // Decimal comma: 1234,56
    text = text.replace(',', '.');
  } else {
    text = text.replace(/,/g, '');
  }

  if (!/^\d*\.?\d*$/.test(text)) {
    return null;
  }

  var value = Number(text);
  if (isNaN(value)) {
    return null;
  }
  return negative ? -value : value;
}

/**
 * Scan OCR text for every money-shaped token and describe each one: its value,
 * the label in front of it, whether it carried a currency marker, and where it
 * sits in the document. Tokens that are clearly part of an identifier, a date,
 * a measurement or a percentage are dropped here rather than scored.
 */
function findAmountCandidates(text) {
  var lines = String(text || '').split('\n');
  var candidates = [];
  var previousLine = '';

  for (var lineIndex = 0; lineIndex < lines.length; lineIndex++) {
    var line = lines[lineIndex];
    if (!line || !/\d/.test(line)) {
      if (line && line.trim()) previousLine = line.trim();
      continue;
    }

    var tokenPattern = /\d[\d,]*(?:\.\d{1,2})?/g;
    var match;
    while ((match = tokenPattern.exec(line)) !== null) {
      var token = match[0];
      var start = match.index;
      var end = start + token.length;
      var before = line.slice(0, start);
      var after = line.slice(end);

      // Part of a longer identifier, date, time or decimal we truncated.
      if (/[A-Za-z0-9]$/.test(before)) continue;
      if (/[\/:#]$/.test(before)) continue;
      if (/\d[.\-]$/.test(before)) continue;
      if (/^[\/:]/.test(after)) continue;
      if (/^\d/.test(after)) continue;
      if (/^-\d/.test(after)) continue;
      if (/^[A-Za-z]/.test(after) && !AMOUNT_CURRENCY_AFTER_PATTERN.test(after)) continue;
      if (AMOUNT_REJECT_SUFFIX_PATTERN.test(after)) continue;

      var hasCurrency = AMOUNT_CURRENCY_BEFORE_PATTERN.test(before) ||
        AMOUNT_CURRENCY_AFTER_PATTERN.test(after);
      var hasCents = /\.\d{2}$/.test(token);
      var negative = /\(\s*$/.test(before) && /^\s*\)/.test(after);
      if (!negative && /^\s*(?:-|\bCR\b)/i.test(after)) negative = true;
      if (!negative && /(?:^|[^\d])-\s*$/.test(before) && hasCurrency) negative = true;

      var value = parseMoneyToken(token);
      if (value === null) continue;
      if (negative) value = -Math.abs(value);

      // Label context: the text in front of the number on this line, or — for
      // table layouts where the number sits alone in its cell/row — the label
      // line above it.
      var labelContext = before;
      if (labelContext.replace(/[^A-Za-z]/g, '').length < 3) {
        labelContext = previousLine + ' ' + before;
      }

      candidates.push({
        value: value,
        token: token,
        hasCurrency: hasCurrency,
        hasCents: hasCents,
        negative: negative,
        lineIndex: lineIndex,
        labelContext: labelContext.replace(/\s+/g, ' ').trim()
      });
    }

    if (line.trim()) previousLine = line.trim();
  }

  return candidates;
}

/**
 * Score a single candidate. Returns null when its nearest label proves it is
 * not currency (weights, quantities, reference numbers, terms, ...).
 */
function scoreAmountCandidate(candidate) {
  var context = candidate.labelContext || '';
  var immediate = context.slice(-40);

  if (AMOUNT_REJECT_LABEL_PATTERN.test(immediate)) {
    return null;
  }

  var labelScore = 0;
  var labelName = 'unlabelled';
  for (var i = 0; i < AMOUNT_LABEL_RULES.length; i++) {
    if (AMOUNT_LABEL_RULES[i].pattern.test(immediate)) {
      labelScore = AMOUNT_LABEL_RULES[i].score;
      labelName = AMOUNT_LABEL_RULES[i].name;
      break;
    }
  }

  var score = labelScore;
  if (candidate.hasCents) score += 25;
  if (candidate.hasCurrency) score += 20;
  if (!candidate.hasCents && !candidate.hasCurrency) score -= 60;
  if (!candidate.hasCents && !candidate.hasCurrency && /^\d{5,}$/.test(candidate.token.replace(/,/g, ''))) {
    score -= 60; // bare long integer: almost certainly an identifier
  }
  if (candidate.value === 0) score -= 40;

  return { score: score, labelScore: labelScore, labelName: labelName };
}

/**
 * Pick the invoice total out of OCR text.
 *
 * Returns null when nothing usable was found, otherwise:
 *   { value, formatted, confidence, label, agreement, alternatives }
 * `confidence` is 'high' | 'medium' | 'low'; callers use it to decide whether
 * the amount can be logged straight through or needs a human look.
 */
function selectInvoiceAmount(text) {
  var candidates = findAmountCandidates(text);
  if (candidates.length === 0) {
    return null;
  }

  var scored = [];
  for (var i = 0; i < candidates.length; i++) {
    var scoring = scoreAmountCandidate(candidates[i]);
    if (!scoring) continue;
    var entry = candidates[i];
    entry.score = scoring.score;
    entry.labelScore = scoring.labelScore;
    entry.labelName = scoring.labelName;
    scored.push(entry);
  }

  if (scored.length === 0) {
    return null;
  }

  // Agreement: an amount repeated under several labels (Total / Amount Due /
  // Balance Due) is far more likely to be the real total than a one-off number.
  var valueCounts = {};
  scored.forEach(function(entry) {
    var key = entry.value.toFixed(2);
    valueCounts[key] = (valueCounts[key] || 0) + 1;
  });

  // Line-item cross-check: when the itemised charges add up to one of the
  // candidates, that candidate is the total even if its label was weak.
  var lineItemSum = 0;
  var lineItemCount = 0;
  scored.forEach(function(entry) {
    if (entry.labelName === 'line-item' && (entry.hasCents || entry.hasCurrency)) {
      lineItemSum += entry.value;
      lineItemCount += 1;
    }
  });

  scored.forEach(function(entry) {
    var key = entry.value.toFixed(2);
    entry.agreement = (valueCounts[key] || 1) - 1;
    entry.score += Math.min(entry.agreement * 12, 36);
    if (lineItemCount >= 2 && Math.abs(entry.value - lineItemSum) < 0.005) {
      entry.score += 30;
      entry.matchesLineItemSum = true;
    }
  });

  scored.sort(function(a, b) {
    if (b.score !== a.score) return b.score - a.score;
    if (b.lineIndex !== a.lineIndex) return b.lineIndex - a.lineIndex;
    return Math.abs(b.value) - Math.abs(a.value);
  });

  var best = scored[0];

  var confidence = 'low';
  if (best.labelScore >= 120 && (best.hasCents || best.hasCurrency)) {
    confidence = 'high';
  } else if (best.labelScore >= 80 && (best.agreement >= 1 || best.matchesLineItemSum)) {
    confidence = 'high';
  } else if (best.labelScore >= 80 || best.matchesLineItemSum) {
    confidence = 'medium';
  } else if (best.labelScore >= 45 && (best.hasCents || best.hasCurrency)) {
    confidence = 'medium';
  }

  // Two different values fighting for the same top label tier means we cannot
  // tell which one is the total — say so rather than guessing.
  var rivals = scored.filter(function(entry) {
    return entry.labelScore === best.labelScore && Math.abs(entry.value - best.value) >= 0.005;
  });
  if (rivals.length > 0 && confidence === 'high') {
    confidence = 'medium';
  }

  var alternatives = [];
  var seen = {};
  seen[best.value.toFixed(2)] = true;
  for (var j = 1; j < scored.length && alternatives.length < 4; j++) {
    var key2 = scored[j].value.toFixed(2);
    if (seen[key2]) continue;
    seen[key2] = true;
    alternatives.push({
      value: scored[j].value,
      formatted: formatAmountNumber(scored[j].value),
      label: scored[j].labelContext.slice(-40)
    });
  }

  return {
    value: best.value,
    formatted: formatAmountNumber(best.value),
    confidence: confidence,
    label: best.labelContext.slice(-40),
    labelName: best.labelName,
    agreement: best.agreement,
    matchesLineItemSum: !!best.matchesLineItemSum,
    alternatives: alternatives
  };
}

/**
 * Normalize any amount-ish value (string from a mapping profile, spreadsheet
 * cell, user edit) to the "1234.56" string the log sheet and PDFs expect.
 * Returns '' when the value is not an amount.
 */
function normalizeAmountText(value) {
  var parsed = parseMoneyToken(value);
  if (parsed === null) {
    var fallback = selectInvoiceAmount(String(value || ''));
    if (!fallback) return '';
    parsed = fallback.value;
  }
  return formatAmountNumber(parsed);
}

/**
 * The value written into the sheet's Amount column. Real numbers are written as
 * numbers so downstream SUM/pivot formulas work; anything unparseable falls
 * back to the placeholder text.
 */
function sheetAmountValue(invoiceData) {
  if (!invoiceData) return MISSING_VALUE_LABEL;

  // Read the displayed amount first: a mapping profile, a learned correction or
  // a reviewer's edit all write to `amount`, and parsing it here means the
  // numeric twin can never drift out of step with what the review UI showed.
  var parsed = parseMoneyToken(invoiceData.amount);
  if (parsed !== null) {
    return parsed;
  }
  if (typeof invoiceData.amountValue === 'number' && !isNaN(invoiceData.amountValue)) {
    return invoiceData.amountValue;
  }
  return invoiceData.amount || MISSING_VALUE_LABEL;
}

/* ═══════════════════════════════════════════════════════════════════════════
 * LAYOUT-AWARE FIELD HELPERS
 *
 * PDF-to-text flattening destroys column structure: a header row and its value
 * row become two adjacent lines, and side-by-side address blocks merge into a
 * single line per row. The helpers below read those shapes directly instead of
 * relying on "label immediately followed by value", which never matches.
 * ═══════════════════════════════════════════════════════════════════════════ */

const DATE_TOKEN_PATTERN_G = /(?:[A-Za-z]{3,9}\.?\s+\d{1,2},?\s+\d{2,4}|\d{1,2}[\/\-.]\d{1,2}[\/\-.]\d{2,4})/g;
const CITY_STATE_ZIP_PATTERN_G = /([A-Z][A-Za-z.'\-]*(?:[ ][A-Z][A-Za-z.'\-]*){0,3},\s*(?:[A-Z]{2}|[A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)\s+\d{5}(?:-\d{4})?)/g;

/**
 * Find a header line matching every supplied pattern, plus the first non-empty
 * line beneath it (its value row).
 * Returns null when no such pair exists.
 */
function findHeaderValueRow(text, requiredHeaderPatterns) {
  const lines = String(text || '').split('\n');
  const patterns = requiredHeaderPatterns || [];

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];
    if (!line || !line.trim()) continue;

    let matchesAll = patterns.length > 0;
    for (let r = 0; r < patterns.length; r++) {
      if (!patterns[r].test(line)) { matchesAll = false; break; }
    }
    if (!matchesAll) continue;

    for (let j = i + 1; j < Math.min(lines.length, i + 4); j++) {
      if (lines[j] && lines[j].trim()) {
        return {
          header: line.trim(),
          values: lines[j].trim(),
          headerIndex: i,
          valueIndex: j
        };
      }
    }
  }
  return null;
}

/**
 * Pull the origin and destination out of a side-by-side address block.
 * The block header ("Origin Address ... Destination Address", "Shipper ...
 * Consignee") is located first so remit-to and bill-to addresses elsewhere on
 * the page cannot be mistaken for shipment endpoints.
 */
function extractAddressPair(text) {
  const empty = { origin: null, destination: null };
  const lines = String(text || '').split('\n');

  const originPattern = /\b(?:Origin|Shipper|Pick\s*Up|Pickup|Ship\s*From)\b/i;
  const destinationPattern = /\b(?:Destination|Consignee|Delivery|Drop\s*Off|Ship\s*To)\b/i;

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];
    if (!line || !originPattern.test(line) || !destinationPattern.test(line)) continue;
    // "Ship Date  Delivery Date  ..." is a shipment header, not an address block.
    if (/\bdate\b/i.test(line) && !/\b(?:address|location)\b/i.test(line)) continue;

    const block = lines.slice(i + 1, i + 8).join('\n');
    const locations = block.match(CITY_STATE_ZIP_PATTERN_G) || [];
    if (locations.length === 0) continue;

    return {
      origin: locations[0] ? locations[0].replace(/\s+/g, ' ').trim() : null,
      destination: locations[1] ? locations[1].replace(/\s+/g, ' ').trim() : null
    };
  }

  return empty;
}


function extractInvoiceData(text, fileName) {
  const cleanText = text
    .replace(/","/g, '\n')
    .replace(/"\s*,\s*"/g, '\n')
    .replace(/",\s*\n/g, '\n')
    .replace(/\n\s*,\s*"/g, '\n')
    .replace(/"/g, '')
    .replace(/\r\n/g, '\n');

  const clean = (val, fallback, preserveLines) => {
    if (!val) return fallback;
    if (preserveLines) return val.trim() || fallback;
    return val.replace(/\s+/g, ' ').trim() || fallback;
  };

  // Generic words that should never be accepted as field values
  const GENERIC_WORDS = /^(drop|pickup|destination|origin|weight|commodity|equipment|total|amount|number|description|date|invoice|payment|from|ship|billing|company|headquarters|location|address|contact|remit|notes|page|services|logistics|freight|transport|carriers?|inc|llc|corp|ltd)$/i;

  // Walk EVERY match of each pattern, not just the first. The old version gave
  // up on a pattern as soon as its first hit failed validation, so an invoice
  // with "PO Box 207779" in the remit block never reached the real "PO # ..."
  // further down the page.
  const extractFrom = (regexes, requireDigit) => {
    for (let i = 0; i < regexes.length; i++) {
      const source = regexes[i];
      const flags = source.flags.indexOf('g') >= 0 ? source.flags : source.flags + 'g';
      const scanner = new RegExp(source.source, flags);
      let match;
      while ((match = scanner.exec(cleanText)) !== null) {
        if (match[0].length === 0) { scanner.lastIndex += 1; continue; }
        if (!match[1]) continue;
        const value = match[1].trim();
        if (value.length <= 1) continue;
        if (GENERIC_WORDS.test(value)) continue;
        if (requireDigit && !/\d/.test(value)) continue;
        return value;
      }
    }
    return null;
  };

  const extractFirst = regexes => extractFrom(regexes, false);

  // Extract a value that MUST contain at least one digit (for IDs, numbers, dates, amounts)
  const extractNumeric = regexes => extractFrom(regexes, true);

  // Freight invoices very often print shipment fields as a header row followed
  // by a value row. Read those positionally — label-proximity regexes cannot
  // see across a column layout.
  const shipmentRow = findHeaderValueRow(cleanText, [
    /\bShip(?:ping)?\s*Date\b/i,
    /\b(?:Delivery|Drop\s*Off|Deliver)\s*Date\b/i
  ]);
  let rowShipDate = null;
  let rowDeliveryDate = null;
  let rowPo = null;
  if (shipmentRow) {
    const rowDates = shipmentRow.values.match(DATE_TOKEN_PATTERN_G) || [];
    if (rowDates.length >= 1) rowShipDate = rowDates[0];
    if (rowDates.length >= 2) rowDeliveryDate = rowDates[1];
    // When "PO #" is the final header column, the trailing value is the PO.
    if (/\bP\.?O\.?\s*#?\s*$/i.test(shipmentRow.header)) {
      const tailMatch = shipmentRow.values.match(/([A-Z]{0,3}\d{5,})\s*$/i);
      if (tailMatch) rowPo = tailMatch[1];
    }
  }

  // ── Invoice Number ──────────────────────────────────────────────────────────
  // Strategy 1: Explicit label with number/# keyword followed by alphanumeric ID
  let invoiceNumber = extractNumeric([
    /\b(?:Invoice|INV)\s*(?:No\.?|Number|#|Num\.?)\s*:?\s*([A-Z0-9][-A-Z0-9]{2,})/i,
    /\b(?:Invoice|Bill)\s*(?:No\.?|#)\s*:?\s*([A-Z0-9][-A-Z0-9]{2,})/i
  ]);

  // Strategy 2: "Invoice" on its own line followed immediately by just an ID on the next line
  if (!invoiceNumber) {
    const invLineMatch = cleanText.match(/\bInvoice\b\s*\n\s*([A-Z]{0,4}\d[\w\-]{2,})\s*\n/i);
    if (invLineMatch && /\d/.test(invLineMatch[1])) {
      invoiceNumber = invLineMatch[1].trim();
    }
  }

  // Strategy 3: Pattern like "INV-XXXXXX" or "INV123456" anywhere in text
  if (!invoiceNumber) {
    const invPatternMatch = cleanText.match(/\b(INV[-#]?\d{4,})\b/i);
    if (invPatternMatch) invoiceNumber = invPatternMatch[1];
  }

  // Strategy 4: Extract from filename (e.g. "invoice-INV6744248.pdf" → "INV6744248")
  if (!invoiceNumber && fileName) {
    const fileNameInv = fileName.match(/\b(INV[-#]?\d{4,})\b/i) ||
                        fileName.match(/invoice[-_]([A-Z0-9]{4,})/i) ||
                        fileName.match(/\b([A-Z]{2,4}\d{5,})\b/i);
    if (fileNameInv) invoiceNumber = fileNameInv[1];
  }

  // ── PO Number ───────────────────────────────────────────────────────────────
  // Horizontal whitespace only: a PO label at the end of a header row must not
  // swallow the first token of the row beneath it.
  const po = rowPo || extractNumeric([
    /\b(?:PO|P\.O\.|Purchase\s*Order)\s*(?:No\.?|Number|#)?[ \t]*:?[ \t]*([A-Z0-9][-A-Z0-9]{2,})/i,
    /\b(?:Customer|Client)\s*(?:PO|Order)\s*(?:No\.?|Number|#)?[ \t]*:?[ \t]*([A-Z0-9][-A-Z0-9]{2,})/i,
    /\b(?:Reference|Ref)\s*(?:No\.?|#|Number)?[ \t]*:?[ \t]*([A-Z0-9]{3,}[-A-Z0-9]*)/i
  ]);

  // ── Amount ──────────────────────────────────────────────────────────────────
  // Scored candidate selection (see selectInvoiceAmount) rather than "first
  // thing on the page that looks like money" — that old approach happily
  // returned line-item rates, weights and quantities as the invoice total.
  const amountResult = selectInvoiceAmount(cleanText);
  const amount = amountResult ? amountResult.formatted : null;

  // ── Remit Info ───────────────────────────────────────────────────────────────
  const remitMatch = cleanText.match(/\b(?:ACH\s*Remittance|Payment\s*Remittance(?:\s*Instructions)?|Remit\s*To|Bank\s*Transfers?)\b[\s:]*\n+([\s\S]{15,300}?)(?=\n(?:Total|Amount|Special\s*Instructions|Page\s*\d|Notes|$))/i);
  const remitInfo = remitMatch ? remitMatch[1] : null;

  // ── Dates ───────────────────────────────────────────────────────────────────
  const DATE_PAT = /([A-Za-z]+\s+\d{1,2},?\s+\d{4}|\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4})/;
  const DATE_PAT_G = new RegExp(DATE_PAT.source, 'g');

  let shipDate = rowShipDate || extractNumeric([
    /\b(?:Ship|Pickup|Pick\s*Up|Ship\s*Date|Pickup\s*Date)\b[\s:]*([A-Za-z]+\s+\d{1,2},?\s+\d{4}|\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4})/i,
    /\b(?:Bill(?:ing)?\s*Date|Invoice\s*Date)\b[\s:]*([A-Za-z]+\s+\d{1,2},?\s+\d{4}|\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4})/i
  ]);

  // NOTE: "Due Date" is the *payment* due date driven by the payment terms, not
  // the delivery date. Reading it as a delivery date logged the wrong date on
  // every invoice that printed payment terms, so it is deliberately absent here.
  let deliveryDate = rowDeliveryDate || extractNumeric([
    /\b(?:Delivery|Drop\s*Off|Delivered|Deliver)\s*Date\b[\s:]*([A-Za-z]+\s+\d{1,2},?\s+\d{4}|\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4})/i,
    /\b(?:Date\s*Delivered|Actual\s*Delivery)\b[\s:]*([A-Za-z]+\s+\d{1,2},?\s+\d{4}|\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4})/i
  ]);

  // Fallback: grab dates from a "Pickup Date ... Drop Off Date" context block
  if (!shipDate || !deliveryDate) {
    const datesBlock = cleanText.match(/\b(?:Pickup|Ship)\b.*?Date[\s\S]{0,80}?\b(?:Drop\s*Off|Delivery)\b.*?Date[\s\S]{0,40}?(?:\n\n|\n[A-Z])/i);
    if (datesBlock) {
      const found = datesBlock[0].match(DATE_PAT_G);
      if (found && !shipDate) shipDate = found[0];
      if (found && found.length >= 2 && !deliveryDate) deliveryDate = found[1];
    }
  }

  // ── Origin / Destination ─────────────────────────────────────────────────────
  // Side-by-side "Origin Address    Destination Address" blocks merge into one
  // line per row once the PDF is flattened to text, so pull the City/State/ZIP
  // pairs out of the block instead of trying to read a single labelled value.
  const addressPair = extractAddressPair(cleanText);

  const origin = addressPair.origin || extractFirst([
    /\b(?:Shipper|Origin|Stop\s*1)\b[\s:]*\n\s*([^,\n]{5,})/i,
    /\b(?:Pickup\s*Location|Origin\s*City|Ship\s*From)\b[\s:]*\n?\s*([^\n,]{5,80})/i
  ]);

  const destination = addressPair.destination || extractFirst([
    /\b(?:Consignee|Destination|Stop\s*2)\b[\s:]*\n\s*([^,\n]{5,})/i,
    /\b(?:Delivery\s*Location|Destination\s*City|Ship\s*To)\b[\s:]*\n?\s*([^\n,]{5,80})/i
  ]);

  // ── Product Type ─────────────────────────────────────────────────────────────
  const productType = extractFirst([
    /\bCommodity\b\s*\n\s*Equipment\s*Type\s*\n\s*([^\n]+)/i,
    /\bEquipment\s*Type\b[\s:]*([A-Za-z][A-Za-z0-9\/ -]{1,28}?)(?=\s*(?:Weight|Temp|Commodity|Qty|$))/i,
    /\b(?:Commodity\s*Description|Product\s*Description|Item\s*Description)\b[\s:]*\n?\s*([^\n]{3,80})/i,
    /\b(?:Product|Commodity)\b[\s:]*([^\n]{3,60})/i
  ]);

  return {
    invoiceNumber: clean(invoiceNumber, MISSING_VALUE_LABEL),
    po: clean(po, MISSING_VALUE_LABEL),
    shipDate: clean(shipDate, MISSING_VALUE_LABEL),
    deliveryDate: clean(deliveryDate, MISSING_VALUE_LABEL),
    amount: clean(amount, MISSING_VALUE_LABEL),
    amountValue: amountResult ? amountResult.value : null,
    amountConfidence: amountResult ? amountResult.confidence : 'none',
    amountLabel: amountResult ? amountResult.label : '',
    amountAlternatives: amountResult ? amountResult.alternatives : [],
    origin: clean(origin, 'Review Required'),
    destination: clean(destination, 'Review Required'),
    productType: clean(productType, 'Review Required'),
    remitInfo: clean(remitInfo, MISSING_VALUE_LABEL, true)
  };
}

/**
 * Is the routing code on this invoice usable?
 *
 * determineCoding() falls back to RDC-UNKNOWN when nothing in the document
 * matched a routing keyword. That is the one thing a person genuinely has to
 * resolve — AP can correct an amount, but they cannot guess which RDC a load
 * belonged to.
 */
function isCodingResolved(appliedCoding) {
  const coding = String(appliedCoding || '').trim();
  if (!coding) return false;
  const rdc = extractRdcCodeFromCoding(coding);
  return !!rdc && rdc !== UNRESOLVED_RDC_CODE;
}

/**
 * Does the amount need flagging in the email and on the code sheet?
 * Flagging is not holding — the invoice still goes out.
 */
function isAmountUncertain(invoiceData) {
  if (!invoiceData) return true;
  const confidence = invoiceData.amountConfidence;
  return confidence !== 'high' && confidence !== 'confirmed';
}

/**
 * Should this invoice be held back from the ledger entirely?
 *
 * Only when the coding is unresolved. A questionable amount rides along with
 * a flag (see isAmountUncertain) unless HOLD_LOW_CONFIDENCE_AMOUNTS is on.
 */
function shouldHoldForAmountReview(invoiceData, appliedCoding) {
  if (!isCodingResolved(appliedCoding)) {
    return true;
  }
  return HOLD_LOW_CONFIDENCE_AMOUNTS && isAmountUncertain(invoiceData);
}


function determineCoding(text) {
  let rdc = 'RDC-UNKNOWN';
  let cat = 'CAT 4';
  const searchSpace = text.replace(/\s+/g, ' ').toLowerCase();

  for (let i = 0; i < ROUTING_RULES.rdcRules.length; i += 1) {
    if (searchSpace.includes(ROUTING_RULES.rdcRules[i].keyword)) {
      rdc = ROUTING_RULES.rdcRules[i].code;
      break;
    }
  }

  for (let i = 0; i < ROUTING_RULES.catRules.length; i += 1) {
    if (searchSpace.includes(ROUTING_RULES.catRules[i].keyword)) {
      cat = ROUTING_RULES.catRules[i].code;
      break;
    }
  }

  const outputString = `${ROUTING_RULES.INVOICE_TYPE}, ${rdc}, ${cat}`;
  return outputString.replace(/\s+/g, ' ').trim();
}

function determineCarrierType(text, invoiceData, appliedCoding, fileName) {
  const searchSpace = text.replace(/\s+/g, ' ').toLowerCase();

  // 1. An explicit carrier/vendor label always wins.
  const labelledCarrier = matchCarrierRegexes(text, [
    /\b(?:Carrier|Motor\s*Carrier|Hauler|Vendor|Supplier)\s*(?:Name)?\s*:\s*([^\n,]{3,80})/i,
    /\b(?:Carrier|Motor\s*Carrier|Hauler|Vendor)\s*(?:Name)?\s*:?\s*\n\s*([^\n,]{3,80})/i
  ]);
  if (labelledCarrier) return labelledCarrier;

  // 2. The letterhead of the invoice: the party billing us IS the carrier.
  const vendorName = extractVendorNameFromLetterhead(text);
  if (vendorName) return vendorName;

  // 3. Commodity keywords. These describe what moved rather than who moved it,
  //    so they are a late fallback, not the first thing tried.
  for (let i = 0; i < ROUTING_RULES.catRules.length; i += 1) {
    const rule = ROUTING_RULES.catRules[i];
    if (searchSpace.includes(rule.keyword)) {
      return toTitleCase(rule.keyword);
    }
  }

  const billToCarrier = matchCarrierRegexes(text, [
    /\b(?:Trucking(?:\s+Company)?|Logistics\s+Provider)\b[\s\n]*:?\s*([^\n,]{3,80})/i,
    /\bRemit\s*To\b[\s\n]*:?\s*\n\s*([^\n,]{3,80})/i
  ]);
  if (billToCarrier) return billToCarrier;

  // 4. Product type, but never an equipment type — "Van" is not a carrier.
  if (invoiceData && invoiceData.productType &&
      invoiceData.productType !== 'Review Required' &&
      !isEquipmentTypeValue(invoiceData.productType)) {
    return invoiceData.productType;
  }

  if (appliedCoding) {
    const segments = appliedCoding.split(',').map(item => item.trim());
    if (segments.length >= 3) {
      return segments[2];
    }
  }

  return stripInvoiceExtension(fileName).slice(0, 48) || 'Unknown Carrier';
}

/**
 * Return the first regex capture that survives isLikelyCarrierType.
 */
function matchCarrierRegexes(text, regexes) {
  for (let i = 0; i < regexes.length; i += 1) {
    const match = text.match(regexes[i]);
    if (match && match[1]) {
      const candidate = match[1].replace(/\s+/g, ' ').trim();
      if (isLikelyCarrierType(candidate)) {
        return candidate;
      }
    }
  }
  return null;
}

/**
 * Read the carrier's trading name off the invoice letterhead.
 *
 * Handles the common "<Legal Entity> d/b/a" / "<Trading Name>" pair — without
 * it, an invoice headed "DM Trans, LLC d/b/a" / "Arrive Logistics" filed itself
 * under the shipper or the equipment type instead of under Arrive.
 */
function extractVendorNameFromLetterhead(text) {
  const lines = String(text || '').split('\n').map(function(line) { return line.trim(); });

  // "d/b/a" on its own or at the end of a line: the trading name is next.
  for (let i = 0; i < Math.min(lines.length, 12); i++) {
    if (!/\bd\/?\s*b\/?\s*a\.?\s*$/i.test(lines[i])) continue;
    for (let j = i + 1; j < Math.min(lines.length, i + 3); j++) {
      const candidate = lines[j].replace(/\s+/g, ' ').trim();
      if (candidate && !isAddressLine(candidate) && isLikelyCarrierType(candidate)) {
        return candidate;
      }
    }
  }

  // Otherwise the first company-shaped line in the letterhead block.
  for (let i = 0; i < Math.min(lines.length, 8); i++) {
    const candidate = lines[i].replace(/\s+/g, ' ').trim();
    if (!candidate || isAddressLine(candidate)) continue;
    if (/\b(?:invoice|statement|bill|remittance|headquarters|page)\b/i.test(candidate)) continue;
    if (!/\b(?:inc|llc|l\.l\.c|ltd|corp(?:oration)?|co|company|logistics|transport(?:ation)?|trucking|freight|carriers?|express|lines|group|services)\b\.?/i.test(candidate)) continue;
    if (isLikelyCarrierType(candidate)) {
      return candidate;
    }
  }

  return null;
}

/**
 * Street addresses, city/state/ZIP lines and phone numbers are not names.
 */
function isAddressLine(value) {
  const text = String(value || '').trim();
  if (!text) return true;
  if (/^\d/.test(text)) return true;
  if (/\b\d{5}(?:-\d{4})?\b/.test(text)) return true;
  if (/\b(?:P\.?O\.?\s*Box|Suite|Ste\.?|Floor|Fl\.?|Building|Bldg\.?|Drive|Street|Avenue|Road|Blvd|Boulevard|Lane|Parkway|Hwy|Highway)\b/i.test(text)) return true;
  if (/^\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4}$/.test(text)) return true;
  return false;
}

/**
 * Trailer/equipment descriptors that must never be treated as a carrier name.
 */
function isEquipmentTypeValue(value) {
  return /^(?:dry\s*)?(?:van|reefer|refrigerated|flat\s*bed|flatbed|step\s*deck|stepdeck|container|intermodal|box\s*truck|straight\s*truck|tanker|hopper|conestoga|power\s*only|ltl|ftl|dry)\b/i
    .test(String(value || '').trim());
}

function isLikelyCarrierType(value) {
  const cleaned = String(value || '').trim();
  if (!cleaned || cleaned.length < 3) {
    return false;
  }
  if (cleaned.length > 60) {
    return false;
  }
  if (/\b(remit|po box|ship date|delivery date|address|amount line)\b/i.test(cleaned)) {
    return false;
  }
  if (/^(invoice|number|date|amount|origin|destination|po)$/i.test(cleaned)) {
    return false;
  }
  return true;
}

function normalizeCarrierType(value) {
  const sanitized = String(value || 'Unknown Carrier')
    .replace(/[\\/:*?"<>|]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
  return sanitized || 'Unknown Carrier';
}

function buildMergePairId(seed) {
  const digest = digestToHex(Utilities.computeDigest(
    Utilities.DigestAlgorithm.MD5,
    String(seed || `${Date.now()}`),
    Utilities.Charset.UTF_8
  ));
  return digest.slice(0, 10).toUpperCase();
}

function buildOutputFileNames(carrierType, originalName, mergePairId) {
  const safeCarrier = normalizeCarrierType(carrierType);
  const baseName = stripInvoiceExtension(originalName);
  const pairSegment = mergePairId ? `Pair ${mergePairId} - ` : '';
  return {
    originalPdfName: `${safeCarrier} - ${pairSegment}${baseName}.pdf`,
    codeSheetName: `${safeCarrier} - ${pairSegment}Code Sheet - ${baseName}.pdf`
  };
}

/* ═══════════════════════════════════════════════════════════════════════════
 * OUTGOING MAIL
 *
 * Processed invoices have to leave as the delegated AP mailbox, not as whoever
 * happens to own the script. MailApp cannot do that at all — it always sends
 * as the effective user — so everything goes through GmailApp, which honours a
 * verified "send mail as" alias.
 *
 * The alias has to be set up once in Gmail (Settings → Accounts → Send mail as)
 * for the account running this script. If it is not there, Gmail silently
 * ignores the `from` and sends as the owner, so the alias is checked against
 * getAliases() first and a mismatch is reported loudly rather than quietly
 * producing mail from the wrong address — which is exactly the symptom that
 * started this.
 * ═══════════════════════════════════════════════════════════════════════════ */

/**
 * One-time correction for a mail configuration that sends invoices to the
 * operator instead of to AP.
 *
 * Only two cases are touched, both of which are unambiguous misconfigurations
 * rather than choices: TARGET_EMAIL unset, or TARGET_EMAIL equal to the address
 * running the script. Anything else is left alone. Runs once, guarded by a
 * flag, and says what it did in the activity feed.
 */
function ensureMailConfiguration() {
  const properties = PropertiesService.getScriptProperties();
  if (properties.getProperty(MAIL_CONFIG_REPAIR_FLAG)) {
    return { repaired: false, reason: 'already_checked' };
  }
  properties.setProperty(MAIL_CONFIG_REPAIR_FLAG, new Date().toISOString());

  let owner = '';
  try {
    owner = String(Session.getEffectiveUser().getEmail() || '').toLowerCase();
  } catch (error) {
    owner = '';
  }

  const current = String(properties.getProperty(PROPERTY_KEYS.TARGET_EMAIL) || '').trim();
  const sendsToSelf = !!owner && current.toLowerCase() === owner;

  if (current && !sendsToSelf) {
    return { repaired: false, reason: 'looks_intentional', targetEmail: current };
  }

  properties.setProperty(PROPERTY_KEYS.TARGET_EMAIL, DEFAULTS.TARGET_EMAIL);
  if (!properties.getProperty(PROPERTY_KEYS.SEND_AS_ALIAS)) {
    properties.setProperty(PROPERTY_KEYS.SEND_AS_ALIAS, DEFAULTS.SEND_AS_ALIAS);
  }

  appendProcessingFeed('warning', `Processed invoices were addressed to ${current || '(nobody)'}; switched to ${DEFAULTS.TARGET_EMAIL}.`, {
    from: current,
    to: DEFAULTS.TARGET_EMAIL,
    sendAsAlias: properties.getProperty(PROPERTY_KEYS.SEND_AS_ALIAS),
    reason: sendsToSelf ? 'was addressed to the script owner' : 'was not set'
  });

  return { repaired: true, from: current, to: DEFAULTS.TARGET_EMAIL };
}


/**
 * Aliases this account may legitimately send as, lowercased.
 * Cached per execution: getAliases() is a network call and invoices go out in
 * batches.
 */
function getAvailableSendAsAliases() {
  if (sendAsAliasCache) {
    return sendAsAliasCache;
  }
  try {
    sendAsAliasCache = GmailApp.getAliases().map(function(alias) {
      return String(alias || '').toLowerCase().trim();
    }).filter(Boolean);
  } catch (error) {
    Logger.log('Could not read Gmail aliases: ' + error.message);
    sendAsAliasCache = [];
  }
  return sendAsAliasCache;
}

function clearSendAsAliasCache() {
  sendAsAliasCache = null;
}

/**
 * Work out which address to send as.
 * Returns { alias, usable, reason } — `alias` is '' when the message should go
 * out as the account owner.
 */
function resolveSendAsAlias(config) {
  const requested = String((config && config.SEND_AS_ALIAS) || '').toLowerCase().trim();
  if (!requested) {
    return { alias: '', usable: false, reason: 'No send-as alias configured.' };
  }

  const available = getAvailableSendAsAliases();
  if (available.length === 0) {
    return {
      alias: '',
      usable: false,
      reason: `Gmail reported no send-as aliases for this account, so "${requested}" cannot be used.`
    };
  }
  if (available.indexOf(requested) === -1) {
    return {
      alias: '',
      usable: false,
      reason: `"${requested}" is not a verified send-as alias on this account (available: ${available.join(', ')}).`
    };
  }

  return { alias: requested, usable: true, reason: '' };
}

/**
 * Send one processed-invoice email.
 *
 * Fails loudly on a misconfigured sender rather than sending as the wrong
 * address: mail that appears to come from an individual instead of the AP
 * mailbox gets rejected or misrouted downstream, and nobody notices for weeks.
 */
function sendInvoiceEmail(config, message) {
  const runtimeConfig = config || getConfig();
  const to = String(runtimeConfig.TARGET_EMAIL || '').trim();

  if (!to) {
    throw new Error('TARGET_EMAIL is not set — nowhere to send the processed invoice.');
  }

  const options = {
    attachments: message.attachments || [],
    name: runtimeConfig.SEND_AS_NAME || 'Inbound Invoicing'
  };

  const sender = resolveSendAsAlias(runtimeConfig);
  if (sender.usable) {
    options.from = sender.alias;
  } else if (runtimeConfig.SEND_AS_ALIAS) {
    // Configured but unusable — say so once per message so it shows up in the
    // feed next to the invoice it affected.
    appendProcessingFeed('warning', `Sending as the account owner instead of ${runtimeConfig.SEND_AS_ALIAS}: ${sender.reason}`, {
      fileName: message.fileName || null,
      requestedAlias: runtimeConfig.SEND_AS_ALIAS,
      availableAliases: getAvailableSendAsAliases()
    });
  }

  GmailApp.sendEmail(to, message.subject, message.body, options);

  appendProcessingFeed('info', `Emailed ${message.fileName || 'invoice'} to ${to}`, {
    to: to,
    from: sender.usable ? sender.alias : '(account owner)',
    attachments: (message.attachments || []).map(function(blob) { return blob.getName(); })
  });
}

/**
 * Subject line. An amount we could not tie to an explicit total is called out
 * here so it is visible before the mail is even opened.
 */
function buildInvoiceEmailSubject(fileName, invoiceData) {
  const flag = isAmountUncertain(invoiceData) ? '[CHECK AMOUNT] ' : '';
  return `${flag}Processed Invoice: ${fileName}`;
}

function buildInvoiceEmailBody(invoiceData, appliedCoding, emailPackage) {
  const data = invoiceData || {};
  const lines = [];

  lines.push(emailPackage && emailPackage.merged
    ? 'Attached is the coded summary merged with the original invoice.'
    : 'Attached are the code sheet and the original invoice.');
  lines.push('');
  lines.push(`Coding Applied: ${appliedCoding}`);
  lines.push(`Invoice #: ${data.invoiceNumber || MISSING_VALUE_LABEL}`);
  lines.push(`PO #: ${data.po || MISSING_VALUE_LABEL}`);
  lines.push(`Amount: ${data.amount || MISSING_VALUE_LABEL}`);

  if (isAmountUncertain(data)) {
    lines.push('');
    lines.push('NOTE: the coding above is confirmed, but the amount could not be tied to an');
    lines.push('explicit total on the invoice. Please check it against the attached document');
    lines.push('before posting.');
  }

  return lines.join('\n');
}

/**
 * Report the mail setup to the web app so a wrong sender is visible in the UI
 * rather than only in delivered mail.
 */
function webGetMailStatus() {
  try {
    const config = getConfig();
    const sender = resolveSendAsAlias(config);
    let owner = '';
    try {
      owner = Session.getEffectiveUser().getEmail();
    } catch (error) {
      owner = '';
    }

    return {
      ok: true,
      targetEmail: config.TARGET_EMAIL || '',
      requestedAlias: config.SEND_AS_ALIAS || '',
      effectiveSender: sender.usable ? sender.alias : owner,
      aliasUsable: sender.usable,
      reason: sender.reason,
      availableAliases: getAvailableSendAsAliases(),
      owner: owner,
      // The bug this was written for: invoices addressed back to the operator.
      sendingToSelf: !!owner && owner.toLowerCase() === String(config.TARGET_EMAIL || '').toLowerCase()
    };
  } catch (error) {
    return { ok: false, error: error.message };
  }
}

/**
 * Send a test message with the current sender/recipient settings, so the setup
 * can be proven before the next batch runs.
 */
function webSendTestEmail() {
  try {
    const config = getConfig();
    sendInvoiceEmail(config, {
      fileName: 'mail-settings-test',
      subject: 'Inbound Invoicing — mail settings test',
      body: [
        'This is a test from the Inbound Invoicing Tool.',
        '',
        `Sent to: ${config.TARGET_EMAIL}`,
        `Requested send-as alias: ${config.SEND_AS_ALIAS || '(none)'}`,
        '',
        'If the From: address on this message is not the alias above, the alias is not',
        'verified on this account — add it in Gmail under Settings > Accounts >',
        '"Send mail as", then run this test again.'
      ].join('\n'),
      attachments: []
    });
    return { ok: true, message: `Test email sent to ${config.TARGET_EMAIL}.`, status: webGetMailStatus() };
  } catch (error) {
    return { ok: false, error: error.message };
  }
}

/**
 * The attachments that go on the outgoing email.
 *
 * AP wants ONE document: the coded summary page followed by the original
 * invoice. mergePdfsBestEffort() builds exactly that (a master spreadsheet
 * holding the summary plus the converted original, exported as a single PDF)
 * and, unlike a pdf-lib merge, it is synchronous — which matters because this
 * runs inside google.script.run handlers that cannot await.
 *
 * If the merge fails for any reason the pair is sent as two attachments rather
 * than sending nothing, and the failure is recorded in the feed.
 */
function buildInvoiceEmailAttachments(invoiceData, appliedCoding, codeSheetBlob, originalPdfBlob, fileName, outputBlobs) {
  if (!originalPdfBlob) {
    appendProcessingFeed('warning', `No original document available to merge for ${fileName}; sending the code sheet alone.`, {
      fileName: fileName
    });
    return { attachments: [codeSheetBlob], merged: false, reason: 'no_original' };
  }

  try {
    const merged = mergePdfsBestEffort(invoiceData, appliedCoding, originalPdfBlob, fileName);
    if (merged.ok && merged.blob) {
      return {
        attachments: [merged.blob.setName(buildMergedAttachmentName(fileName))],
        merged: true,
        method: merged.method
      };
    }
    appendProcessingFeed('warning', `Could not merge the code sheet with ${fileName}; sending both files separately.`, {
      fileName: fileName,
      error: merged.error || 'unknown'
    });
  } catch (error) {
    appendProcessingFeed('warning', `Merge threw for ${fileName}; sending both files separately.`, {
      fileName: fileName,
      error: error.message
    });
  }

  return {
    attachments: (outputBlobs && outputBlobs.length) ? outputBlobs : [codeSheetBlob, originalPdfBlob],
    merged: false,
    reason: 'merge_failed'
  };
}

function buildMergedAttachmentName(fileName) {
  return 'Coded_' + stripInvoiceExtension(fileName) + '.pdf';
}


function buildOutputBlobs(carrierType, originalName, codeSheetBlob, originalPdfBlob, mergePairId) {
  const names = buildOutputFileNames(carrierType, originalName, mergePairId);
  const blobs = [codeSheetBlob.copyBlob().setName(names.codeSheetName)];
  // The original is absent when a review item came from a Gmail attachment
  // rather than a Drive file; emit the code sheet on its own instead of failing.
  if (originalPdfBlob) {
    blobs.push(originalPdfBlob.copyBlob().setName(names.originalPdfName));
  }
  return blobs;
}

function schedulePostProcessingMerge() {
  const triggers = ScriptApp.getProjectTriggers();
  for (let i = 0; i < triggers.length; i++) {
    if (triggers[i].getHandlerFunction && triggers[i].getHandlerFunction() === POST_PROCESS_MERGE_TRIGGER) {
      return;
    }
  }

  ScriptApp.newTrigger(POST_PROCESS_MERGE_TRIGGER)
    .timeBased()
    .after(15 * 1000)
    .create();

  appendProcessingFeed('info', 'Scheduled post-processing merge run.', {});
}

function listProcessedPdfFiles(folder) {
  const files = folder.getFiles();
  const pdfFiles = [];

  while (files.hasNext()) {
    const file = files.next();
    if (!isPdfInvoiceFile(file)) {
      continue;
    }
    pdfFiles.push(file);
  }

  return pdfFiles;
}

function finalizeProcessedOutputBlobs(result, processedFolder, summary) {
  const outputBlobs = Array.isArray(result.outputBlobs) ? result.outputBlobs : [];
  for (let bi = 0; bi < outputBlobs.length; bi++) {
    processedFolder.createFile(outputBlobs[bi]);
    if (summary) {
      summary.summaryFilesCreated += 1;
    }
  }
  return outputBlobs;
}

function getOrCreateCarrierFolder(parentFolder, carrierType, folderCache) {
  const normalizedName = normalizeCarrierType(carrierType);

  if (folderCache[normalizedName]) {
    return { folder: folderCache[normalizedName], created: false };
  }

  const folders = parentFolder.getFoldersByName(normalizedName);
  if (folders.hasNext()) {
    const existing = folders.next();
    folderCache[normalizedName] = existing;
    return { folder: existing, created: false };
  }

  const createdFolder = parentFolder.createFolder(normalizedName);
  folderCache[normalizedName] = createdFolder;
  return { folder: createdFolder, created: true };
}

function toTitleCase(value) {
  return String(value || '')
    .toLowerCase()
    .split(/\s+/)
    .map(part => part ? `${part[0].toUpperCase()}${part.slice(1)}` : '')
    .join(' ')
    .trim();
}

function createTriggers() {
  const config = getConfig();
  const interval = Math.max(1, Number(config.RUN_INTERVAL_MINUTES) || Number(DEFAULTS.RUN_INTERVAL_MINUTES));

  const triggers = ScriptApp.getProjectTriggers();
  triggers.forEach(trigger => ScriptApp.deleteTrigger(trigger));

  ScriptApp.newTrigger('processDriveFolders')
    .timeBased()
    .everyMinutes(interval)
    .create();

  ScriptApp.newTrigger(MERGE_WATCH_TRIGGER)
    .timeBased()
    .everyMinutes(1)
    .create();

  ScriptApp.newTrigger(POST_PROCESS_MERGE_TRIGGER)
    .timeBased()
    .everyMinutes(MERGE_RUN_INTERVAL_MINUTES)
    .create();

  if (ENABLE_GMAIL_INGESTION) {
    ScriptApp.newTrigger('processIncomingPDFs')
      .timeBased()
      .everyMinutes(interval)
      .create();
  }

  const message = ENABLE_GMAIL_INGESTION
    ? `Triggers created for Drive and Gmail at ${interval}-minute intervals, plus merge every ${MERGE_RUN_INTERVAL_MINUTES} minutes.`
    : `Drive trigger created at ${interval}-minute intervals, plus merge every ${MERGE_RUN_INTERVAL_MINUTES} minutes (Gmail disabled).`;
  Logger.log(message);
  appendProcessingFeed('info', message, {
    intervalMinutes: interval,
    mergeIntervalMinutes: MERGE_RUN_INTERVAL_MINUTES,
    gmailEnabled: ENABLE_GMAIL_INGESTION
  });
  return {
    ok: true,
    message,
    intervalMinutes: interval,
    mergeIntervalMinutes: MERGE_RUN_INTERVAL_MINUTES
  };
}

function monitorProcessedFolderForMerge() {
  const config = getConfig();
  const folder = DriveApp.getFolderById(config.PROCESSED_FOLDER_ID);
  const pdfFiles = listProcessedPdfFiles(folder);
  const names = [];
  let count = 0;
  let newestUpdated = 0;
  let totalSize = 0;

  for (let i = 0; i < pdfFiles.length; i++) {
    const file = pdfFiles[i];
    names.push(file.getName());
    count += 1;
    totalSize += Number(file.getSize() || 0);
    newestUpdated = Math.max(newestUpdated, file.getLastUpdated().getTime());
  }

  const properties = PropertiesService.getScriptProperties();
  if (count === 0) {
    properties.deleteProperty(PROPERTY_KEYS.MERGE_WATCH_STATE);
    return;
  }

  names.sort();
  const signature = digestToHex(Utilities.computeDigest(
    Utilities.DigestAlgorithm.MD5,
    JSON.stringify({ count: count, newestUpdated: newestUpdated, totalSize: totalSize, names: names }),
    Utilities.Charset.UTF_8
  ));

  const now = Date.now();
  const rawState = properties.getProperty(PROPERTY_KEYS.MERGE_WATCH_STATE);
  let state = null;
  try {
    state = rawState ? JSON.parse(rawState) : null;
  } catch (error) {
    state = null;
  }

  if (!state || state.signature !== signature) {
    properties.setProperty(PROPERTY_KEYS.MERGE_WATCH_STATE, JSON.stringify({
      signature: signature,
      detectedAt: now,
      count: count,
      newestUpdated: newestUpdated,
      scheduledAt: null
    }));
    appendProcessingFeed('info', 'Merge watcher detected processed PDFs.', {
      folderId: config.PROCESSED_FOLDER_ID,
      pdfCount: count,
      newestUpdated: newestUpdated
    });
    return;
  }

  if (state.scheduledAt) {
    return;
  }

  if (now - Number(state.detectedAt || 0) < MERGE_WATCH_DEBOUNCE_MS) {
    return;
  }

  schedulePostProcessingMerge();
  state.scheduledAt = now;
  properties.setProperty(PROPERTY_KEYS.MERGE_WATCH_STATE, JSON.stringify(state));
}

function getConfig() {
  const properties = PropertiesService.getScriptProperties();
  const sourceFoldersRaw = properties.getProperty(PROPERTY_KEYS.SOURCE_FOLDERS) || DEFAULTS.SOURCE_FOLDERS;
  const carrierTypeFixesRaw = properties.getProperty(PROPERTY_KEYS.CARRIER_TYPE_FIXES) || DEFAULTS.CARRIER_TYPE_FIXES;

  return {
    // Derived from the saved email filters so the Filters tab is the single
    // place a query is defined; the stored property is only a cache of it.
    SEARCH_QUERY: buildGmailSearchQuery(getEmailFilters().fetch),
    SOURCE_FOLDERS: parseFolderIds(sourceFoldersRaw),
    SOURCE_FOLDERS_RAW: sourceFoldersRaw,
    PROCESSED_FOLDER_ID: properties.getProperty(PROPERTY_KEYS.PROCESSED_FOLDER_ID) || DEFAULTS.PROCESSED_FOLDER_ID,
    CARRIER_TYPE_FIXES_RAW: carrierTypeFixesRaw,
    CARRIER_TYPE_FIX_MAP: parseAutoFixMappings(carrierTypeFixesRaw),
    CONFIRMED_CARRIERS: parseConfirmedCarriers(properties.getProperty(PROPERTY_KEYS.CONFIRMED_CARRIERS) || ''),
    TARGET_EMAIL: properties.getProperty(PROPERTY_KEYS.TARGET_EMAIL) || DEFAULTS.TARGET_EMAIL,
    SEND_AS_ALIAS: properties.getProperty(PROPERTY_KEYS.SEND_AS_ALIAS) || DEFAULTS.SEND_AS_ALIAS,
    SEND_AS_NAME: properties.getProperty(PROPERTY_KEYS.SEND_AS_NAME) || DEFAULTS.SEND_AS_NAME,
    SHEET_ID: properties.getProperty(PROPERTY_KEYS.SHEET_ID) || DEFAULTS.SHEET_ID,
    SHEET_NAME: properties.getProperty(PROPERTY_KEYS.SHEET_NAME) || DEFAULTS.SHEET_NAME,
    RUN_INTERVAL_MINUTES: properties.getProperty(PROPERTY_KEYS.RUN_INTERVAL_MINUTES) || DEFAULTS.RUN_INTERVAL_MINUTES,
    ROUTING_RULES
  };
}

function parseFolderIds(sourceFoldersRaw) {
  return (sourceFoldersRaw || '')
    .split(/[\n,]/)
    .map(item => item.trim())
    .filter(item => item.length > 0 && !/^YOUR_/i.test(item));
}

function parseAutoFixMappings(raw) {
  const mappings = {};
  String(raw || '')
    .split(/\n+/)
    .map(function(line) { return line.trim(); })
    .filter(Boolean)
    .forEach(function(line) {
      const parts = line.split(/=>|=/);
      if (parts.length < 2) {
        return;
      }
      const from = normalizeAutoFixKey(parts[0]);
      const to = String(parts.slice(1).join('=>')).trim();
      if (!from || !to) {
        return;
      }
      mappings[from] = normalizeCarrierType(to);
    });
  return mappings;
}

function normalizeAutoFixKey(value) {
  return String(value || '')
    .toLowerCase()
    .replace(/[\s\-_]+/g, ' ')
    .trim();
}

/**
 * Parse the confirmed carriers list (one name per line) into a normalized Set.
 * Returns an empty Set if the list is blank (meaning all carriers are allowed).
 */
function parseConfirmedCarriers(raw) {
  const entries = String(raw || '')
    .split(/\n+/)
    .map(function(line) { return line.trim().toLowerCase(); })
    .filter(Boolean);
  return new Set(entries);
}

/**
 * Returns true if the carrier is confirmed (or if no confirmed list is configured).
 */
function isCarrierConfirmed(carrierType, config) {
  const list = config && config.CONFIRMED_CARRIERS;
  if (!list || list.size === 0) return true;
  return list.has(String(carrierType || '').toLowerCase().trim());
}

function applyCarrierTypeAutoFix(value, config) {
  const normalizedValue = normalizeCarrierType(value);
  const runtimeConfig = config || getConfig();
  const fixMap = runtimeConfig.CARRIER_TYPE_FIX_MAP || {};
  const match = fixMap[normalizeAutoFixKey(normalizedValue)];
  return match || normalizedValue;
}

function listSourcePdfFiles(limit) {
  const config = getConfig();
  if (!config.SOURCE_FOLDERS || config.SOURCE_FOLDERS.length === 0) {
    return [];
  }

  const maxItems = Math.max(1, Math.min(Number(limit) || 50, 500));
  const files = [];

  for (let i = 0; i < config.SOURCE_FOLDERS.length; i += 1) {
    const folder = DriveApp.getFolderById(config.SOURCE_FOLDERS[i]);
    const iter = folder.getFiles();
    while (iter.hasNext() && files.length < maxItems) {
      const file = iter.next();
      if (!isIngestibleInvoiceFile(file)) {
        continue;
      }
      files.push({
        id: file.getId(),
        name: file.getName(),
        isArchive: isArchiveFile(file),
        size: file.getSize(),
        updatedAt: file.getLastUpdated().toISOString(),
        folderId: folder.getId(),
        folderName: folder.getName()
      });
    }
    if (files.length >= maxItems) {
      break;
    }
  }

  return files.sort((a, b) => new Date(b.updatedAt).getTime() - new Date(a.updatedAt).getTime());
}

function getProcessingFeed(limit) {
  const raw = PropertiesService.getScriptProperties().getProperty(PROPERTY_KEYS.PROCESSING_FEED);
  if (!raw) {
    return [];
  }

  let events = [];
  try {
    events = JSON.parse(raw);
  } catch (error) {
    events = [];
  }

  if (!Array.isArray(events)) {
    return [];
  }

  const maxItems = Math.max(1, Number(limit) || 80);
  return events.slice(-maxItems).reverse();
}

function appendProcessingFeed(type, message, meta) {
  const properties = PropertiesService.getScriptProperties();
  const current = getProcessingFeed(FEED_LIMIT).reverse();
  current.push({
    at: new Date().toISOString(),
    type: String(type || 'info'),
    message: String(message || ''),
    meta: meta || {}
  });
  const compact = current.slice(-FEED_LIMIT);
  properties.setProperty(PROPERTY_KEYS.PROCESSING_FEED, JSON.stringify(compact));
}

function assertRequiredConfig(config, keys) {
  const missing = [];

  keys.forEach(key => {
    const value = config[key];
    if (Array.isArray(value)) {
      if (value.length === 0) {
        missing.push(key);
      }
      return;
    }
    if (!value || String(value).trim().length === 0) {
      missing.push(key);
    }
  });

  if (missing.length > 0) {
    throw new Error(`Missing required configuration: ${missing.join(', ')}. Set these in Script Properties or the sidebar UI.`);
  }
}

function getInvoiceSheet(config) {
  const cacheKey = `${config.SHEET_ID}::${config.SHEET_NAME}`;
  if (invoiceSheetCache && invoiceSheetCache.cacheKey === cacheKey) {
    return invoiceSheetCache.sheet;
  }

  const spreadsheet = SpreadsheetApp.openById(config.SHEET_ID);
  let sheet = spreadsheet.getSheetByName(config.SHEET_NAME);
  if (!sheet) {
    sheet = spreadsheet.insertSheet(config.SHEET_NAME);
  }

  ensureSheetHeaders(sheet);
  invoiceSheetCache = { cacheKey, sheet };
  return sheet;
}

function ensureSheetHeaders(sheet) {
  const maxColumns = Math.max(sheet.getMaxColumns(), LOG_HEADERS.length);
  if (sheet.getMaxColumns() < LOG_HEADERS.length) {
    sheet.insertColumnsAfter(sheet.getMaxColumns(), LOG_HEADERS.length - sheet.getMaxColumns());
  }

  const firstRow = sheet.getRange(1, 1, 1, maxColumns).getValues()[0];
  const hasHeaders = firstRow.slice(0, LOG_HEADERS.length).some(cell => String(cell || '').trim().length > 0);
  if (!hasHeaders) {
    sheet.getRange(1, 1, 1, LOG_HEADERS.length).setValues([LOG_HEADERS]);
    return;
  }

  const existingSignature = firstRow.slice(0, LOG_HEADERS.length).join('|');
  const expectedSignature = LOG_HEADERS.join('|');
  if (existingSignature !== expectedSignature) {
    sheet.getRange(1, 1, 1, LOG_HEADERS.length).setValues([LOG_HEADERS]);
  }
}

function buildDriveProcessingKey(file) {
  return ['drive', file.getId(), file.getName(), file.getSize(), file.getLastUpdated().getTime()].join('|');
}

function buildGmailProcessingKey(message, attachment) {
  const attachmentHash = digestToHex(Utilities.computeDigest(Utilities.DigestAlgorithm.MD5, attachment.copyBlob().getBytes()));
  return ['gmail', message.getId(), attachment.getName(), attachment.getSize(), attachmentHash].join('|');
}

function tryBeginProcessing(processingKey, meta) {
  const lock = LockService.getScriptLock();
  lock.waitLock(30000);

  try {
    const properties = PropertiesService.getScriptProperties();
    const stateKey = getProcessingStateKey(processingKey);
    const now = Date.now();
    const raw = properties.getProperty(stateKey);
    if (raw) {
      let parsed = null;
      try {
        parsed = JSON.parse(raw);
      } catch (error) {
        parsed = null;
      }

      if (parsed && parsed.status === PROCESSING_STATE.DONE) {
        return {
          alreadyProcessed: true,
          stateKey,
          details: parsed.details || {}
        };
      }

      if (parsed && parsed.status === PROCESSING_STATE.PROCESSING) {
        const lastUpdatedAt = Number(parsed.updatedAt || parsed.startedAt || 0);
        if (now - lastUpdatedAt < PROCESSING_STATE.STALE_MS) {
          return { inProgress: true, stateKey };
        }
      }
    }

    properties.setProperty(stateKey, JSON.stringify({
      status: PROCESSING_STATE.PROCESSING,
      startedAt: now,
      updatedAt: now,
      meta: meta || {}
    }));

    return { started: true, stateKey };
  } finally {
    lock.releaseLock();
  }
}

function markProcessingDone(claim, details) {
  const properties = PropertiesService.getScriptProperties();
  properties.setProperty(claim.stateKey, JSON.stringify({
    status: PROCESSING_STATE.DONE,
    completedAt: Date.now(),
    details: details || {}
  }));
}

function clearProcessingState(claim) {
  if (!claim || !claim.stateKey) {
    return;
  }
  PropertiesService.getScriptProperties().deleteProperty(claim.stateKey);
}

function cleanupProcessedState(retentionDays) {
  const properties = PropertiesService.getScriptProperties();
  const allProperties = properties.getProperties();
  const now = Date.now();
  const maxAgeMs = (Number(retentionDays) || PROCESSING_STATE.RETENTION_DAYS) * 24 * 60 * 60 * 1000;
  const keysToDelete = [];

  Object.keys(allProperties).forEach(key => {
    if (!key.startsWith(PROCESSING_STATE.PREFIX)) {
      return;
    }

    let parsed = null;
    try {
      parsed = JSON.parse(allProperties[key]);
    } catch (error) {
      keysToDelete.push(key);
      return;
    }

    const timestamp = Number(parsed.completedAt || parsed.updatedAt || parsed.startedAt || 0);
    if (!timestamp || now - timestamp > maxAgeMs) {
      keysToDelete.push(key);
    }
  });

  keysToDelete.forEach(key => properties.deleteProperty(key));
  return keysToDelete.length;
}

function getProcessingStateKey(processingKey) {
  return PROCESSING_STATE.PREFIX + digestToHex(Utilities.computeDigest(Utilities.DigestAlgorithm.MD5, processingKey, Utilities.Charset.UTF_8));
}

function digestToHex(digest) {
  return digest
    .map(byte => {
      const normalized = byte < 0 ? byte + 256 : byte;
      return (`0${normalized.toString(16)}`).slice(-2);
    })
    .join('');
}

function escapeHtml(value) {
  return String(value || '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function setLastRunSummary(channel, summary) {
  const payload = {
    timestamp: new Date().toISOString(),
    channel,
    summary
  };
  PropertiesService.getScriptProperties().setProperty(PROPERTY_KEYS.LAST_RUN_SUMMARY, JSON.stringify(payload));
}

function getLastRunSummary() {
  const raw = PropertiesService.getScriptProperties().getProperty(PROPERTY_KEYS.LAST_RUN_SUMMARY);
  if (!raw) {
    return null;
  }
  try {
    return JSON.parse(raw);
  } catch (error) {
    return null;
  }
}

async function mergeProcessedCarrierFiles() {
  const config = getConfig();
  const inputFolderId = config.PROCESSED_FOLDER_ID;
  const parentOutputFolderId = DEFAULT_FOLDER_IDS.MERGED_OUTPUT_PARENT_FOLDER_ID;
  const trackerSheetId = DEFAULT_TRACKER_SHEET_ID;
  const properties = PropertiesService.getScriptProperties();

  try {
    const inputFolder = DriveApp.getFolderById(inputFolderId);
    const parentOutputFolder = DriveApp.getFolderById(parentOutputFolderId);
    const files = listProcessedPdfFiles(inputFolder);
    const fileGroups = {};
    let totalFiles = 0;

    for (let fi = 0; fi < files.length; fi++) {
      const file = files[fi];
      const info = parseOutputFileInfo(file.getName());
      const groupKey = `${info.carrierName}::${info.pairKey}`;
      if (!fileGroups[groupKey]) fileGroups[groupKey] = { info: info, files: [] };
      fileGroups[groupKey].files.push(file);
      totalFiles += 1;
    }

    if (totalFiles === 0) {
      appendProcessingFeed('info', 'Post-processing merge found no PDFs to merge.', {});
      return;
    }

    const cdnUrl = 'https://cdn.jsdelivr.net/npm/pdf-lib/dist/pdf-lib.min.js';
    eval(UrlFetchApp.fetch(cdnUrl).getContentText());
    const setTimeout = function(f, t) {
      Utilities.sleep(t);
      return f();
    };

    const sheet = SpreadsheetApp.openById(trackerSheetId).getActiveSheet();

    for (const groupKey in fileGroups) {
      const group = fileGroups[groupKey];
      const carrier = group.info.carrierName;
      const carrierFiles = group.files;
      carrierFiles.sort(compareOutputFilesForMerge);

      const codeSheetFiles = carrierFiles.filter(function(file) {
        return parseOutputFileInfo(file.getName()).isCodeSheet;
      });
      const originalFiles = carrierFiles.filter(function(file) {
        return !parseOutputFileInfo(file.getName()).isCodeSheet;
      });

      if (codeSheetFiles.length !== 1 || originalFiles.length !== 1) {
        appendProcessingFeed('warning', 'Skipped merge group because files did not form exactly one matched pair.', {
          carrier: carrier,
          pairId: group.info.pairId,
          pairKey: group.info.pairKey,
          fileCount: carrierFiles.length,
          files: carrierFiles.map(function(file) { return file.getName(); })
        });
        continue;
      }

      const mergedPdf = await PDFLib.PDFDocument.create();
      for (let i = 0; i < carrierFiles.length; i++) {
        const pdfData = new Uint8Array(carrierFiles[i].getBlob().getBytes());
        const pdfDoc = await PDFLib.PDFDocument.load(pdfData);
        const pages = await mergedPdf.copyPages(pdfDoc, pdfDoc.getPageIndices());
        pages.forEach(function(page) { mergedPdf.addPage(page); });
      }

      const bytes = await mergedPdf.save();
      const targetFolder = getOrCreateNamedFolder(parentOutputFolder, carrier);
  const mergedNameBase = stripInvoiceExtension(group.info.baseName || carrier);
  const finalFileName = `${carrier}_Merged_${group.info.pairId || mergedNameBase}_${new Date().getTime()}.pdf`;
      const mergedBlob = Utilities.newBlob(Array.from(new Int8Array(bytes)), MimeType.PDF, finalFileName);
      const finalFile = targetFolder.createFile(mergedBlob);

      sheet.appendRow([new Date(), carrier, finalFileName, finalFile.getUrl()]);

      for (let i = 0; i < carrierFiles.length; i++) {
        carrierFiles[i].setTrashed(true);
      }

      appendProcessingFeed('success', 'Post-processing merge completed for carrier.', {
        carrier: carrier,
        pairId: group.info.pairId,
        mergedFile: finalFileName,
        fileCount: carrierFiles.length
      });
    }
    properties.deleteProperty(PROPERTY_KEYS.MERGE_WATCH_STATE);
  } catch (error) {
    properties.deleteProperty(PROPERTY_KEYS.MERGE_WATCH_STATE);
    appendProcessingFeed('error', 'Post-processing merge failed: ' + error.message, {});
    throw error;
  }
}

function runScheduledProcessedCarrierMerge() {
  return mergeProcessedCarrierFiles();
}

function extractCarrierNameFromOutputFile(fileName) {
  const name = String(fileName || '').trim();
  const dashed = name.match(/^(.+?)\s+-\s+/);
  if (dashed && dashed[1]) {
    return normalizeCarrierType(dashed[1]);
  }

  const underscored = name.split(/[_-]/)[0].trim();
  return normalizeCarrierType(underscored || 'Unknown Carrier');
}

function parseOutputFileInfo(fileName) {
  const name = String(fileName || '').trim().replace(/\.pdf$/i, '');
  const carrierName = extractCarrierNameFromOutputFile(fileName);
  const carrierPrefix = `${carrierName} - `;
  let remainder = name;
  if (remainder.indexOf(carrierPrefix) === 0) {
    remainder = remainder.slice(carrierPrefix.length);
  }

  let pairId = '';
  const pairMatch = remainder.match(/^Pair\s+([A-F0-9]{10})\s+-\s+/i);
  if (pairMatch) {
    pairId = String(pairMatch[1] || '').toUpperCase();
    remainder = remainder.slice(pairMatch[0].length);
  }

  const codeSheetPrefix = 'Code Sheet - ';
  const isCodeSheet = remainder.indexOf(codeSheetPrefix) === 0;
  const baseName = isCodeSheet ? remainder.slice(codeSheetPrefix.length) : remainder;

  return {
    carrierName: carrierName,
    pairId: pairId,
    pairKey: pairId || `legacy:${baseName.toLowerCase()}`,
    isCodeSheet: isCodeSheet,
    baseName: baseName,
    originalName: name.toLowerCase()
  };
}

function getOutputFileSortInfo(fileName) {
  const parsed = parseOutputFileInfo(fileName);

  return {
    pairKey: parsed.pairKey,
    baseName: parsed.baseName.toLowerCase(),
    typeRank: parsed.isCodeSheet ? 0 : 1,
    originalName: parsed.originalName
  };
}

function compareOutputFilesForMerge(a, b) {
  const aInfo = getOutputFileSortInfo(a.getName());
  const bInfo = getOutputFileSortInfo(b.getName());

  if (aInfo.pairKey < bInfo.pairKey) return -1;
  if (aInfo.pairKey > bInfo.pairKey) return 1;
  if (aInfo.baseName < bInfo.baseName) return -1;
  if (aInfo.baseName > bInfo.baseName) return 1;
  if (aInfo.typeRank !== bInfo.typeRank) return aInfo.typeRank - bInfo.typeRank;
  if (aInfo.originalName < bInfo.originalName) return -1;
  if (aInfo.originalName > bInfo.originalName) return 1;
  return 0;
}

function getOrCreateNamedFolder(parentFolder, folderName) {
  const normalizedName = normalizeCarrierType(folderName);
  const folders = parentFolder.getFoldersByName(normalizedName);
  if (folders.hasNext()) {
    return folders.next();
  }
  return parentFolder.createFolder(normalizedName);
}