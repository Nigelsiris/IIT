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
  CONFIRMED_CARRIERS: 'CONFIRMED_CARRIERS'
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
  TARGET_EMAIL: '',
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
    searchQuery: properties.getProperty(PROPERTY_KEYS.SEARCH_QUERY) || DEFAULTS.SEARCH_QUERY,
    sourceFolders: properties.getProperty(PROPERTY_KEYS.SOURCE_FOLDERS) || DEFAULTS.SOURCE_FOLDERS,
    processedFolderId: properties.getProperty(PROPERTY_KEYS.PROCESSED_FOLDER_ID) || DEFAULTS.PROCESSED_FOLDER_ID,
    carrierTypeFixes: properties.getProperty(PROPERTY_KEYS.CARRIER_TYPE_FIXES) || DEFAULTS.CARRIER_TYPE_FIXES,
    confirmedCarriers: properties.getProperty(PROPERTY_KEYS.CONFIRMED_CARRIERS) || '',
    targetEmail: properties.getProperty(PROPERTY_KEYS.TARGET_EMAIL) || '',
    sheetId: properties.getProperty(PROPERTY_KEYS.SHEET_ID) || '',
    sheetName: properties.getProperty(PROPERTY_KEYS.SHEET_NAME) || DEFAULTS.SHEET_NAME,
    runIntervalMinutes: properties.getProperty(PROPERTY_KEYS.RUN_INTERVAL_MINUTES) || DEFAULTS.RUN_INTERVAL_MINUTES,
    lastRun: getLastRunSummary()
  };
}

function saveUiConfig(payload) {
  const properties = PropertiesService.getScriptProperties();
  const runIntervalMinutes = String(Number(payload.runIntervalMinutes) || Number(DEFAULTS.RUN_INTERVAL_MINUTES));
  properties.setProperty(PROPERTY_KEYS.SEARCH_QUERY, (payload.searchQuery || DEFAULTS.SEARCH_QUERY).trim());
  properties.setProperty(PROPERTY_KEYS.SOURCE_FOLDERS, (payload.sourceFolders || DEFAULTS.SOURCE_FOLDERS).trim());
  properties.setProperty(PROPERTY_KEYS.PROCESSED_FOLDER_ID, (payload.processedFolderId || DEFAULTS.PROCESSED_FOLDER_ID).trim());
  properties.setProperty(PROPERTY_KEYS.CARRIER_TYPE_FIXES, (payload.carrierTypeFixes || DEFAULTS.CARRIER_TYPE_FIXES).trim());
  properties.setProperty(PROPERTY_KEYS.CONFIRMED_CARRIERS, (payload.confirmedCarriers || '').trim());
  properties.setProperty(PROPERTY_KEYS.TARGET_EMAIL, (payload.targetEmail || '').trim());
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
  return {
    config: getUiConfig(),
    mappings: getExtractionMappings(),
    sourceFiles: listSourcePdfFiles(100),
    lastRun: getLastRunSummary(),
    feed: getProcessingFeed(120),
    reviewQueue: getAllReviewItems()
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
    const mimeType = payload.mimeType || MimeType.PDF;
    const bytes = Utilities.base64Decode(payload.base64Data || '');
    const blob = Utilities.newBlob(bytes, mimeType, fileName);
    const created = sourceFolder.createFile(blob);

    appendProcessingFeed('upload', `Uploaded ${fileName} to source folder.`, {
      fileId: created.getId(),
      fileName,
      sourceFolderId: sourceFolder.getId()
    });

    let processed = null;
    let reviewItem = null;
    if (payload.action === 'review') {
      var reviewResult = webExtractForReview(created.getId());
      reviewItem = reviewResult.ok ? reviewResult.reviewItem : null;
    } else if (payload.action === 'process' || payload.processNow) {
      processed = processSpecificDriveFile(created.getId());
    }

    return {
      ok: true,
      file: {
        id: created.getId(),
        name: created.getName(),
        url: created.getUrl(),
        size: created.getSize(),
        updatedAt: created.getLastUpdated().toISOString()
      },
      processed,
      reviewItem,
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
      amount: editedData.amount || item.extractedData.amount || MISSING_VALUE_LABEL,
      origin: editedData.origin || item.extractedData.origin || 'Review Required',
      destination: editedData.destination || item.extractedData.destination || 'Review Required',
      productType: editedData.productType || item.extractedData.productType || 'Review Required',
      remitInfo: editedData.remitInfo || item.extractedData.remitInfo || MISSING_VALUE_LABEL
    };

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
      invoiceData.amount,
      invoiceData.origin,
      invoiceData.productType,
      invoiceData.destination,
      invoiceData.remitInfo,
      appliedCoding
    ]);

    const generatedCodedPdfBlob = generateHtmlPdf(invoiceData, appliedCoding, item.fileName);

    const file = DriveApp.getFileById(item.fileId);
    const pdfBlob = file.getBlob();
    const mergePairId = buildMergePairId(item.reviewId || item.fileId || item.fileName);
    const outputBlobs = buildOutputBlobs(carrierType, item.fileName, generatedCodedPdfBlob, pdfBlob, mergePairId);

    MailApp.sendEmail({
      to: config.TARGET_EMAIL,
      subject: 'Processed Invoice: ' + item.fileName,
      body: 'Attached are two files:\n1) Code sheet\n2) Original invoice\n\nExtracted PO: ' + invoiceData.po + '\nCoding Applied: ' + appliedCoding,
      attachments: outputBlobs
    });

    const processedFolder = DriveApp.getFolderById(config.PROCESSED_FOLDER_ID);
    outputBlobs.forEach(function(blob) {
      processedFolder.createFile(blob);
    });
    file.setTrashed(true);

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
    failed: 0
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
      if (!isPdfInvoiceFile(file)) {
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

function processDriveFolders() {
  const config = getConfig();
  assertRequiredConfig(config, ['SOURCE_FOLDERS', 'PROCESSED_FOLDER_ID', 'SHEET_ID', 'TARGET_EMAIL']);
  appendProcessingFeed('info', 'Drive processing started.', { sourceFolders: config.SOURCE_FOLDERS });
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
    carrierFoldersCreated: 0
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
      if (!isSupportedInvoiceFile(file)) {
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
  const result = processInvoiceFile(file.getBlob(), file.getName(), sourceLabel, processingKey, config, file.getMimeType(), { sendEmail: false });

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
 * One-time fix: updates the saved Search Query Script Property to use the
 * correct nested label path. Run once from the Apps Script editor.
 */
function fixGmailSearchQuery() {
  const props = PropertiesService.getScriptProperties();
  const current = props.getProperty(PROPERTY_KEYS.SEARCH_QUERY) || '';
  const fixed = current.replace(/label:ITBP\b/g, 'label:Automation-Emails/ITBP');
  props.setProperty(PROPERTY_KEYS.SEARCH_QUERY, fixed);
  Logger.log('Search query updated: "' + current + '" → "' + fixed + '"');
  appendProcessingFeed('info', 'Gmail search query fixed', { from: current, to: fixed });
  return { ok: true, from: current, to: fixed };
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
    messagesMarkedRead: 0
  };
  const startedAt = Date.now();
  let handledCount = 0;
  let stoppedEarly = false;

  const threads = GmailApp.search(config.SEARCH_QUERY);
  summary.threads = threads.length;
  summary.searchQuery = config.SEARCH_QUERY;
  appendProcessingFeed('info', `Gmail search: "${config.SEARCH_QUERY}" → ${threads.length} thread(s) found`, { searchQuery: config.SEARCH_QUERY, threadCount: threads.length });

  for (let ti = 0; ti < threads.length; ti++) {
    if (stoppedEarly) break;
    const messages = threads[ti].getMessages();

    for (let mi = 0; mi < messages.length; mi++) {
      if (stoppedEarly) break;
      const message = messages[mi];
      if (!message.isUnread()) {
        continue;
      }

      summary.unreadMessages += 1;
      const invoiceAttachments = message.getAttachments().filter(isSupportedInvoiceAttachment);
      if (invoiceAttachments.length === 0) {
        message.markRead();
        summary.messagesMarkedRead += 1;
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

    if (!isCarrierConfirmed(carrierType, runtimeConfig)) {
      const reviewId = 'rv-' + Date.now() + '-' + (claim.stateKey || fileName).slice(0, 8);
      saveReviewItem({
        reviewId: reviewId,
        fileId: (claim.details && claim.details.fileId) || null,
        fileName: fileName,
        source: source,
        ocrText: extractedText.slice(0, 4000),
        extractedData: invoiceData,
        appliedCoding: appliedCoding,
        carrierType: carrierType,
        profileName: mappingResult.profileName || null,
        heldReason: 'unconfirmed_carrier',
        createdAt: new Date().toISOString()
      });
      clearProcessingState(claim);
      appendProcessingFeed('warning', `Held for review — unconfirmed carrier: "${carrierType}" (${fileName})`, { reviewId, carrierType, fileName });
      return { ok: true, status: 'held_for_review', carrierType, reviewId };
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
      invoiceData.amount,
      invoiceData.origin,
      invoiceData.productType,
      invoiceData.destination,
      invoiceData.remitInfo,
      appliedCoding
    ]);

    const generatedCodedPdfBlob = generateHtmlPdf(invoiceData, appliedCoding, fileName);
    const mergePairId = buildMergePairId(claim.stateKey || processingKey || fileName);
    const outputBlobs = buildOutputBlobs(carrierType, fileName, generatedCodedPdfBlob, fileBlob, mergePairId);

    if (processingOptions.sendEmail !== false) {
      MailApp.sendEmail({
        to: runtimeConfig.TARGET_EMAIL,
        subject: `Processed Invoice: ${fileName}`,
        body: `Attached are two files:\n1) Code sheet\n2) Original invoice\n\nExtracted PO: ${invoiceData.po}\nCoding Applied: ${appliedCoding}`,
        attachments: outputBlobs
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

    if (!isCarrierConfirmed(spreadsheetData.carrierType, runtimeConfig)) {
      const reviewId = 'rv-' + Date.now() + '-' + (claim.stateKey || fileName).slice(0, 8);
      saveReviewItem({
        reviewId: reviewId,
        fileId: null,
        fileName: fileName,
        source: source,
        ocrText: '',
        extractedData: {},
        appliedCoding: spreadsheetData.codingSummary || '',
        carrierType: spreadsheetData.carrierType,
        profileName: 'xlsx-shifts',
        heldReason: 'unconfirmed_carrier',
        createdAt: new Date().toISOString()
      });
      clearProcessingState(claim);
      appendProcessingFeed('warning', `Held for review — unconfirmed carrier: "${spreadsheetData.carrierType}" (${fileName})`, { reviewId, carrierType: spreadsheetData.carrierType, fileName });
      return { ok: true, status: 'held_for_review', carrierType: spreadsheetData.carrierType, reviewId };
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

    if (!options || options.sendEmail !== false) {
      MailApp.sendEmail({
        to: runtimeConfig.TARGET_EMAIL,
        subject: `Processed Invoice: ${fileName}`,
        body: `Attached are two files:\n1) Code sheet\n2) Original invoice\n\nSplit coding summary: ${spreadsheetData.codingSummary}`,
        attachments: outputBlobs
      });
    }

    markProcessingDone(claim, {
      fileName,
      source,
      coding: spreadsheetData.codingSummary,
      carrierType: spreadsheetData.carrierType,
      mergePairId,
      mappingProfile: 'xlsx-shifts'
    });
    appendProcessingFeed('success', `Processed ${fileName}`, {
      fileName,
      source,
      carrierType: spreadsheetData.carrierType,
      mergePairId,
      mappingProfile: 'xlsx-shifts',
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
    const m = text.match(/([0-9,]+\.[0-9]{2})/);
    return m ? m[1] : '';
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
    return /^[0-9,]+\.[0-9]{2}$/.test(text);
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
    const spreadsheet = waitForSpreadsheetOpen(converted.id, 'xlsx invoice conversion');
    const invoiceMeta = extractSpreadsheetInvoiceMeta(spreadsheet, fileName);
    const shiftRows = extractSpreadsheetShiftRows(spreadsheet);

    if (shiftRows.length === 0) {
      throw new Error('No delivery rows found on the Shifts tab.');
    }

    const groups = groupSpreadsheetRowsByCoding(shiftRows, invoiceMeta, fileName);
    const invoiceSummary = buildSpreadsheetInvoiceSummary(invoiceMeta, groups, fileName);
    const codeSheetBlob = generateSpreadsheetSplitHtmlPdf(invoiceSummary, fileName);
    const originalPdfBlob = exportSpreadsheetAsPdf(converted.id, fileName);

    return {
      carrierType: applyCarrierTypeAutoFix(invoiceSummary.carrierType),
      codeSheetBlob: codeSheetBlob,
      originalPdfBlob: originalPdfBlob,
      codingSummary: invoiceSummary.groups.map(function(group) {
        return `${group.rdcCode}: ${group.coding} ($${formatAmountNumber(group.totalAmount)})`;
      }).join(' | '),
      logRows: buildSpreadsheetLogRows(invoiceSummary, fileName, source)
    };
  } finally {
    tempIds.forEach(function(id) {
      try { DriveApp.getFileById(id).setTrashed(true); } catch (error) {}
    });
  }
}

function extractSpreadsheetInvoiceMeta(spreadsheet, fileName) {
  const invoiceSheet = findSheetByNamePattern(spreadsheet, /invoice/i);
  const values = invoiceSheet ? invoiceSheet.getDataRange().getDisplayValues() : [];
  const carrierType = applyCarrierTypeAutoFix(findLabelValue(values, /^carrier\s*:?$/i) || stripInvoiceExtension(fileName));
  const carrierEmail = findLabelValue(values, /^email\s*:?$/i) || '';
  const carrierAddress = [
    findLabelValue(values, /^carrier\s*address\s*:?$/i),
    findLabelValue(values, /^address\s*:?$/i)
  ].filter(Boolean)[0] || '';
  const invoiceNumber = extractInvoiceNumberFromFileName(fileName) || MISSING_VALUE_LABEL;

  return {
    carrierType: carrierType,
    carrierEmail: carrierEmail,
    carrierAddress: carrierAddress,
    invoiceNumber: invoiceNumber
  };
}

function extractSpreadsheetShiftRows(spreadsheet) {
  const shiftSheet = findSheetByNamePattern(spreadsheet, /shift/i);
  if (!shiftSheet) {
    throw new Error('Shifts tab not found in spreadsheet invoice.');
  }

  const range = shiftSheet.getDataRange();
  const displayValues = range.getDisplayValues();
  const rawValues = range.getValues();
  const headerIndex = findShiftHeaderRow(displayValues);
  if (headerIndex < 0) {
    throw new Error('Shift header row not found.');
  }

  const headerMap = mapShiftColumns(displayValues[headerIndex] || []);
  const rows = [];
  let emptyStreak = 0;

  for (let rowIndex = headerIndex + 1; rowIndex < displayValues.length; rowIndex++) {
    const displayRow = displayValues[rowIndex] || [];
    const rawRow = rawValues[rowIndex] || [];
    if (displayRow.join('').trim() === '') {
      emptyStreak += 1;
      if (rows.length > 0 && emptyStreak >= 10) {
        break;
      }
      continue;
    }
    emptyStreak = 0;

    const po = getCellByIndex(displayRow, headerMap.po);
    const pickupLocation = getCellByIndex(displayRow, headerMap.pickupLocation);
    const dropoffLocation = getCellByIndex(displayRow, headerMap.dropoffLocation);
    const totalAmount = parseAmountValue(
      getRawCellByIndex(rawRow, headerMap.total),
      getCellByIndex(displayRow, headerMap.total),
      getRawCellByIndex(rawRow, headerMap.transportCost),
      getCellByIndex(displayRow, headerMap.transportCost)
    );

    if (!po && !pickupLocation && !dropoffLocation && !totalAmount) {
      continue;
    }

    rows.push({
      deliveryDate: formatSheetDate(getRawCellByIndex(rawRow, headerMap.deliveryDate), getCellByIndex(displayRow, headerMap.deliveryDate)),
      shipmentNumber: getCellByIndex(displayRow, headerMap.shipmentNumber),
      po: po,
      pickupLocation: pickupLocation,
      dropoffLocation: dropoffLocation,
      totalAmount: totalAmount
    });
  }

  return rows;
}

function groupSpreadsheetRowsByCoding(rows, invoiceMeta, fileName) {
  const grouped = {};

  rows.forEach(function(row) {
    const coding = determineCoding([
      invoiceMeta.carrierType,
      row.pickupLocation,
      row.dropoffLocation,
      row.po,
      fileName
    ].join(' '));
    const key = coding;
    if (!grouped[key]) {
      grouped[key] = {
        coding: coding,
        rdcCode: extractRdcCodeFromCoding(coding),
        totalAmount: 0,
        shipmentCount: 0,
        poMap: {},
        pickupMap: {},
        dropoffMap: {},
        deliveryDateMap: {}
      };
    }

    grouped[key].totalAmount += Number(row.totalAmount || 0);
    grouped[key].shipmentCount += 1;
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

function buildSpreadsheetInvoiceSummary(invoiceMeta, groups, fileName) {
  const totalAmount = groups.reduce(function(sum, group) {
    return sum + Number(group.totalAmount || 0);
  }, 0);
  const allPOs = {};
  const allOrigins = {};
  const allDestinations = {};
  const allDates = {};

  groups.forEach(function(group) {
    group.poNumbers.forEach(function(po) { allPOs[po] = true; });
    group.pickupLocations.forEach(function(location) { allOrigins[location] = true; });
    group.dropoffLocations.forEach(function(location) { allDestinations[location] = true; });
    group.deliveryDates.forEach(function(date) { allDates[date] = true; });
  });

  const poNumbers = Object.keys(allPOs).sort();
  const origins = Object.keys(allOrigins).sort();
  const destinations = Object.keys(allDestinations).sort();
  const deliveryDates = Object.keys(allDates).sort();
  const remitBits = [];
  if (invoiceMeta.carrierAddress) remitBits.push(invoiceMeta.carrierAddress);
  if (invoiceMeta.carrierEmail) remitBits.push(invoiceMeta.carrierEmail);
  remitBits.push(`Split across ${groups.length} RDC code${groups.length === 1 ? '' : 's'}`);

  return {
    carrierType: invoiceMeta.carrierType,
    invoiceNumber: invoiceMeta.invoiceNumber,
    totalAmount: totalAmount,
    groups: groups,
    invoiceData: {
      invoiceNumber: invoiceMeta.invoiceNumber,
      po: summarizeValues(poNumbers, 6),
      shipDate: MISSING_VALUE_LABEL,
      deliveryDate: summarizeDateValues(deliveryDates),
      amount: formatAmountNumber(totalAmount),
      origin: summarizeValues(origins, 3) || 'Review Required',
      destination: groups.length > 1 ? `Multiple RDCs (${groups.length})` : (summarizeValues(destinations, 3) || 'Review Required'),
      productType: 'XLSX Shift Invoice',
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
      formatAmountNumber(group.totalAmount),
      summarizeValues(group.pickupLocations, 3) || 'Review Required',
      'XLSX Shift Invoice',
      summarizeValues(group.dropoffLocations, 3) || 'Review Required',
      `Shipments: ${group.shipmentCount}`,
      group.coding
    ];
  });
}

function generateSpreadsheetSplitHtmlPdf(invoiceSummary, originalName) {
  const safeCarrier = escapeHtml(invoiceSummary.carrierType);
  const safeInvoiceNumber = escapeHtml(invoiceSummary.invoiceNumber);
  const safeOriginalName = escapeHtml(originalName);
  const groupRowsHtml = invoiceSummary.groups.map(function(group) {
    return `
      <tr>
        <td style="padding: 10px 8px; border-bottom: 1px solid #ddd;">${escapeHtml(group.rdcCode)}</td>
        <td style="padding: 10px 8px; border-bottom: 1px solid #ddd; font-weight: bold; color: #0b57d0;">${escapeHtml(group.coding)}</td>
        <td style="padding: 10px 8px; border-bottom: 1px solid #ddd;">${group.shipmentCount}</td>
        <td style="padding: 10px 8px; border-bottom: 1px solid #ddd;">$${escapeHtml(formatAmountNumber(group.totalAmount))}</td>
        <td style="padding: 10px 8px; border-bottom: 1px solid #ddd;">${escapeHtml(summarizeValues(group.poNumbers, 8))}</td>
        <td style="padding: 10px 8px; border-bottom: 1px solid #ddd;">${escapeHtml(summarizeValues(group.dropoffLocations, 3))}</td>
      </tr>
    `;
  }).join('');

  const htmlContent = `
    <div style="font-family: Arial, sans-serif; padding: 28px; color: #222;">
      <h1 style="margin: 0 0 12px; color: #0b57d0; border-bottom: 2px solid #0b57d0; padding-bottom: 10px;">Spreadsheet Invoice Coding Summary</h1>
      <table style="width: 100%; border-collapse: collapse; margin-top: 12px; font-size: 14px;">
        <tr>
          <td style="padding: 10px 8px; background: #f5f7fa; width: 28%;"><strong>Carrier</strong></td>
          <td style="padding: 10px 8px;">${safeCarrier}</td>
        </tr>
        <tr>
          <td style="padding: 10px 8px; background: #f5f7fa;"><strong>Invoice #</strong></td>
          <td style="padding: 10px 8px;">${safeInvoiceNumber}</td>
        </tr>
        <tr>
          <td style="padding: 10px 8px; background: #f5f7fa;"><strong>Total Amount</strong></td>
          <td style="padding: 10px 8px; font-weight: bold;">$${escapeHtml(formatAmountNumber(invoiceSummary.totalAmount))}</td>
        </tr>
        <tr>
          <td style="padding: 10px 8px; background: #f5f7fa;"><strong>Source File</strong></td>
          <td style="padding: 10px 8px;">${safeOriginalName}</td>
        </tr>
      </table>

      <h2 style="margin-top: 28px; color: #222;">RDC Split</h2>
      <table style="width: 100%; border-collapse: collapse; margin-top: 10px; font-size: 13px;">
        <thead>
          <tr style="background: #e8f0fe; text-align: left;">
            <th style="padding: 10px 8px;">RDC</th>
            <th style="padding: 10px 8px;">Coding</th>
            <th style="padding: 10px 8px;">Shipments</th>
            <th style="padding: 10px 8px;">Amount</th>
            <th style="padding: 10px 8px;">POs</th>
            <th style="padding: 10px 8px;">Destinations</th>
          </tr>
        </thead>
        <tbody>${groupRowsHtml}</tbody>
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
  const maxRows = Math.min(values.length, 50);
  for (let row = 0; row < maxRows; row++) {
    const currentRow = values[row] || [];
    for (let col = 0; col < currentRow.length; col++) {
      const cell = String(currentRow[col] || '').trim();
      if (!cell || !labelPattern.test(cell)) {
        continue;
      }

      for (let offset = 1; offset <= 3; offset++) {
        const sameRowValue = String(currentRow[col + offset] || '').trim();
        if (sameRowValue) {
          return sameRowValue;
        }
      }

      for (let rowOffset = 1; rowOffset <= 3 && row + rowOffset < values.length; rowOffset++) {
        const nextRow = values[row + rowOffset] || [];
        const belowValue = String(nextRow[col] || nextRow[col + 1] || '').trim();
        if (belowValue) {
          return belowValue;
        }
      }
    }
  }
  return '';
}

function findShiftHeaderRow(displayValues) {
  for (let rowIndex = 0; rowIndex < Math.min(displayValues.length, 20); rowIndex++) {
    const row = (displayValues[rowIndex] || []).map(normalizeHeaderValue);
    if (row.indexOf('po') >= 0 && row.indexOf('dropofflocation') >= 0 && row.indexOf('deliverydate') >= 0) {
      return rowIndex;
    }
  }
  return -1;
}

function mapShiftColumns(headerRow) {
  const map = {};
  (headerRow || []).forEach(function(header, index) {
    const normalized = normalizeHeaderValue(header);
    if (normalized === 'deliverydate') map.deliveryDate = index;
    if (normalized === 's' || normalized === 'shipment' || normalized === 'shipmentnumber') map.shipmentNumber = index;
    if (normalized === 'po') map.po = index;
    if (normalized === 'pickuplocation') map.pickupLocation = index;
    if (normalized === 'dropofflocation') map.dropoffLocation = index;
    if (normalized === 'transportcost') map.transportCost = index;
    if (normalized === 'total') map.total = index;
  });
  return map;
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

function parseAmountValue() {
  for (let i = 0; i < arguments.length; i++) {
    const value = arguments[i];
    if (value === null || value === undefined || value === '') {
      continue;
    }
    if (typeof value === 'number' && !isNaN(value)) {
      return value;
    }
    const parsed = Number(String(value).replace(/[$,\s]/g, ''));
    if (!isNaN(parsed)) {
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
  return String(fileName || 'invoice').replace(/\.(pdf|xlsx)$/i, '').trim() || 'invoice';
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

function isPdfInvoiceMimeType(mimeType, fileName) {
  const normalizedMimeType = String(mimeType || '').toLowerCase();
  return normalizedMimeType === 'application/pdf' || /\.pdf$/i.test(String(fileName || ''));
}

function isSpreadsheetInvoiceMimeType(mimeType, fileName) {
  const normalizedMimeType = String(mimeType || '').toLowerCase();
  return normalizedMimeType === XLSX_MIME_TYPE || normalizedMimeType === GOOGLE_SHEETS_MIME_TYPE || /\.xlsx$/i.test(String(fileName || ''));
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

  const htmlContent = `
    <div style="font-family: Arial, sans-serif; padding: 30px; color: #333;">
      <h1 style="border-bottom: 2px solid #4285F4; padding-bottom: 10px; color: #4285F4;">Invoice Processing Summary</h1>
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

  const extractFirst = regexes => {
    for (let i = 0; i < regexes.length; i++) {
      const match = cleanText.match(regexes[i]);
      if (!match || !match[1] || match[1].trim().length <= 1) continue;
      const value = match[1].trim();
      // Reject pure generic words or values with no digits where a number is expected
      if (GENERIC_WORDS.test(value)) continue;
      return value;
    }
    return null;
  };

  // Extract a value that MUST contain at least one digit (for IDs, numbers, dates, amounts)
  const extractNumeric = regexes => {
    for (let i = 0; i < regexes.length; i++) {
      const match = cleanText.match(regexes[i]);
      if (!match || !match[1] || match[1].trim().length <= 1) continue;
      const value = match[1].trim();
      if (GENERIC_WORDS.test(value)) continue;
      // Must contain at least one digit
      if (!/\d/.test(value)) continue;
      return value;
    }
    return null;
  };

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
  const po = extractNumeric([
    /\b(?:PO|P\.O\.|Purchase\s*Order)\s*(?:No\.?|Number|#)?\s*:?\s*([A-Z0-9][-A-Z0-9]{2,})/i,
    /\b(?:Reference|Ref)\s*(?:No\.?|#|Number)?\s*:?\s*([A-Z0-9]{3,}[-A-Z0-9]*)/i
  ]);

  // ── Amount ──────────────────────────────────────────────────────────────────
  const amount = extractNumeric([
    /\b(?:Total\s*(?:Due)?|Amount\s*(?:Due)?|Balance\s*(?:Due)?|LINE\s*HAUL)\b[\s\S]{0,60}?\$?\s*([0-9,]{1,10}\.[0-9]{2})/i,
    /\bTotal\b[^\n]{0,30}\n[^\n]{0,10}\$?\s*([0-9,]{1,10}\.[0-9]{2})/i,
    /\$\s*([0-9,]{1,10}\.[0-9]{2})/
  ]);

  // ── Remit Info ───────────────────────────────────────────────────────────────
  const remitMatch = cleanText.match(/\b(?:ACH\s*Remittance|Payment\s*Remittance(?:\s*Instructions)?|Remit\s*To|Bank\s*Transfers?)\b[\s:]*\n+([\s\S]{15,300}?)(?=\n(?:Total|Amount|Special\s*Instructions|Page\s*\d|Notes|$))/i);
  const remitInfo = remitMatch ? remitMatch[1] : null;

  // ── Dates ───────────────────────────────────────────────────────────────────
  const DATE_PAT = /([A-Za-z]+\s+\d{1,2},?\s+\d{4}|\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4})/;
  const DATE_PAT_G = new RegExp(DATE_PAT.source, 'g');

  let shipDate = extractNumeric([
    /\b(?:Ship|Pickup|Pickup\s*Date|Ship\s*Date)\b[\s:]*([A-Za-z]+\s+\d{1,2},?\s+\d{4}|\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4})/i,
    /\b(?:Bill(?:ing)?\s*Date|Invoice\s*Date)\b[\s:]*([A-Za-z]+\s+\d{1,2},?\s+\d{4}|\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4})/i
  ]);

  let deliveryDate = extractNumeric([
    /\b(?:Delivery|Drop\s*Off|Delivered|Due)\s*Date\b[\s:]*([A-Za-z]+\s+\d{1,2},?\s+\d{4}|\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4})/i
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
  const origin = extractFirst([
    /\b(?:Shipper|Origin|Stop\s*1)\b[\s:]*\n\s*([^,\n]{5,})/i,
    /\b(?:Pickup\s*Location|Origin\s*City)\b[\s:]*\n?\s*([^\n,]{5,80})/i
  ]);

  const destination = extractFirst([
    /\b(?:Consignee|Destination|Stop\s*2)\b[\s:]*\n\s*([^,\n]{5,})/i,
    /\b(?:Delivery\s*Location|Destination\s*City)\b[\s:]*\n?\s*([^\n,]{5,80})/i
  ]);

  // ── Product Type ─────────────────────────────────────────────────────────────
  const productType = extractFirst([
    /\bCommodity\b\s*\n\s*Equipment\s*Type\s*\n\s*([^\n]+)/i,
    /\b(?:Commodity\s*Description|Product\s*Description|Item\s*Description)\b[\s:]*\n?\s*([^\n]{3,80})/i,
    /\b(?:Product|Commodity)\b[\s:]*([^\n]{3,60})/i
  ]);

  return {
    invoiceNumber: clean(invoiceNumber, MISSING_VALUE_LABEL),
    po: clean(po, MISSING_VALUE_LABEL),
    shipDate: clean(shipDate, MISSING_VALUE_LABEL),
    deliveryDate: clean(deliveryDate, MISSING_VALUE_LABEL),
    amount: clean(amount, MISSING_VALUE_LABEL),
    origin: clean(origin, 'Review Required'),
    destination: clean(destination, 'Review Required'),
    productType: clean(productType, 'Review Required'),
    remitInfo: clean(remitInfo, MISSING_VALUE_LABEL, true)
  };
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

  for (let i = 0; i < ROUTING_RULES.catRules.length; i += 1) {
    const rule = ROUTING_RULES.catRules[i];
    if (searchSpace.includes(rule.keyword)) {
      return toTitleCase(rule.keyword);
    }
  }

  const carrierRegexes = [
    /\b(?:Carrier|Trucking(?:\s+Company)?|Hauler|Motor Carrier|Vendor)\b[\s\n]*:?\s*([^\n,]{3,80})/i,
    /\bBill\s*To\b[\s\n]*:?\s*([^\n,]{3,80})/i
  ];

  for (let i = 0; i < carrierRegexes.length; i += 1) {
    const match = text.match(carrierRegexes[i]);
    if (match && match[1]) {
      const candidate = match[1].replace(/\s+/g, ' ').trim();
      if (isLikelyCarrierType(candidate)) {
        return candidate;
      }
    }
  }

  if (invoiceData && invoiceData.productType && invoiceData.productType !== 'Review Required') {
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

function buildOutputBlobs(carrierType, originalName, codeSheetBlob, originalPdfBlob, mergePairId) {
  const names = buildOutputFileNames(carrierType, originalName, mergePairId);
  return [
    codeSheetBlob.copyBlob().setName(names.codeSheetName),
    originalPdfBlob.copyBlob().setName(names.originalPdfName)
  ];
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
    SEARCH_QUERY: (properties.getProperty(PROPERTY_KEYS.SEARCH_QUERY) || DEFAULTS.SEARCH_QUERY).replace(/label:ITBP\b/g, 'label:Automation-Emails/ITBP'),
    SOURCE_FOLDERS: parseFolderIds(sourceFoldersRaw),
    SOURCE_FOLDERS_RAW: sourceFoldersRaw,
    PROCESSED_FOLDER_ID: properties.getProperty(PROPERTY_KEYS.PROCESSED_FOLDER_ID) || DEFAULTS.PROCESSED_FOLDER_ID,
    CARRIER_TYPE_FIXES_RAW: carrierTypeFixesRaw,
    CARRIER_TYPE_FIX_MAP: parseAutoFixMappings(carrierTypeFixesRaw),
    CONFIRMED_CARRIERS: parseConfirmedCarriers(properties.getProperty(PROPERTY_KEYS.CONFIRMED_CARRIERS) || ''),
    TARGET_EMAIL: properties.getProperty(PROPERTY_KEYS.TARGET_EMAIL) || DEFAULTS.TARGET_EMAIL,
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
      if (!isSupportedInvoiceFile(file)) {
        continue;
      }
      files.push({
        id: file.getId(),
        name: file.getName(),
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