/**
 * Configuration variables.
 * Please update these before running the script!
 */
const CONFIG = {
  // --- GMAIL CONFIG ---
  SEARCH_QUERY: 'has:attachment filename:pdf is:unread label:inbox', 
  
  // --- GOOGLE DRIVE FOLDERS CONFIG ---
  // Add the IDs of the folders where carriers drop the invoices
  SOURCE_FOLDERS: [
    '1ogV9U27c2QjUxU53KJwrtjt3z1LE78XQ', 
    '1DPgJaaaV581nsivcVkzst0nUcisUJQEQ'
  ],
  // Folder to move the original PDFs to after processing so they aren't processed again
  PROCESSED_FOLDER_ID: '1qfE0XUL_qNA5_f7F8sJdt1pQGrt1nYP2', 
  
  // --- OUTPUT CONFIG ---
  TARGET_EMAIL: 'nigel.roark@lidl.us', 
  SHEET_ID: '1HKxhXYnjDfs-9_JSdYT1EGTjWrSihMTdiMxIem1R8rg', 
  SHEET_NAME: 'Invoice Logger', // Updated sheet name
  
  // --- DYNAMIC ROUTING & CODING CONFIG ---
  // Define keywords to look for in the extracted text to determine RDC and CAT codes
  ROUTING_RULES: {
    INVOICE_TYPE: '360100', // Default invoice type
    rdcRules: [
      { keyword: 'PYE', code: '70001' },
      { keyword: 'pye', code: '70001' },
      { keyword: 'Perryville', code: '70001' },
      { keyword: 'perryville', code: '70001' },
      { keyword: 'GRM', code: '60001' },
      { keyword: 'grm', code: '60001' },
      { keyword: 'Graham', code: '60001' },
      { keyword: 'graham', code: '60001' },
      { keyword: 'Mebane', code: '60001' },
      { keyword: 'mebane', code: '60001' },
      { keyword: 'FRG', code: '50001' },
      { keyword: 'Fredericksburg', code: '50001' },
      { keyword: 'fredericksburg', code: '50001' },
    ],
    catRules: [
      { keyword: 'Beverages', code: 'CAT 1' },
      { keyword: 'Polar', code: 'CAT 1' },
      { keyword: 'CG Roxane', code: 'CAT 1' },
      { keyword: 'Frizgerald', code: 'CAT 1' },
      { keyword: 'Lassonde', code: 'CAT 1' },
      { keyword: 'Independent Beverage', code: 'CAT 1' },
      { keyword: 'Premium Water', code: 'CAT 1' },
      { keyword: 'H&S Bakery', code: 'CAT 3' },
    ]
  }
};

/**
 * 1. Process PDFs directly from specified Google Drive Folders
 */
function processDriveFolders() {
  const processedFolder = DriveApp.getFolderById(CONFIG.PROCESSED_FOLDER_ID);
  
  CONFIG.SOURCE_FOLDERS.forEach(folderId => {
    if (folderId === 'YOUR_FIRST_FOLDER_ID_HERE') return; // Skip placeholder
    
    const folder = DriveApp.getFolderById(folderId);
    const files = folder.getFilesByType(MimeType.PDF);
    
    while (files.hasNext()) {
      const file = files.next();
      Logger.log(`Processing Drive PDF: ${file.getName()}`);
      
      const success = processPdfFile(file.getBlob(), file.getName(), `Drive Folder: ${folder.getName()}`);
      
      // Move the file to a 'Processed' folder to avoid duplicate processing
      if (success) {
        file.moveTo(processedFolder);
      }
    }
  });
}

/**
 * 2. Process PDFs from Gmail
 */
function processIncomingPDFs() {
  const threads = GmailApp.search(CONFIG.SEARCH_QUERY);
  
  if (threads.length === 0) {
    Logger.log('No new PDF emails found.');
    return;
  }

  threads.forEach(thread => {
    const messages = thread.getMessages();
    messages.forEach(message => {
      if (!message.isUnread()) return; 

      const attachments = message.getAttachments();
      attachments.forEach(attachment => {
        if (attachment.getContentType() === 'application/pdf') {
          Logger.log(`Processing Email PDF: ${attachment.getName()}`);
          processPdfFile(attachment.copyBlob(), attachment.getName(), message.getFrom());
        }
      });
      message.markRead();
    });
  });
}

/**
 * CORE LOGIC: Converts PDF, Extracts Data, Logs it, Generates Pretty PDF via HTML, and Emails both.
 * @param {GoogleAppsScript.Base.Blob} pdfBlob - The ORIGINAL PDF file blob
 * @param {string} fileName - Name of the file
 * @param {string} source - Where it came from (Email address or Folder name)
 * @returns {boolean} - True if successful
 */
function processPdfFile(pdfBlob, fileName, source) {
  const sheet = SpreadsheetApp.openById(CONFIG.SHEET_ID).getSheetByName(CONFIG.SHEET_NAME);
  
  try {
    // 1. Convert PDF to Google Doc temporarily to extract text (OCR) via Drive API
    const fileResource = {
      name: fileName.replace('.pdf', '') + ' - Temp Processing Doc',
      mimeType: 'application/vnd.google-apps.document'
    };
    const docFile = Drive.Files.create(fileResource, pdfBlob);
    
    // 2. Open Doc, extract text, and immediately delete the temporary Doc
    const doc = DocumentApp.openById(docFile.id);
    const extractedText = doc.getBody().getText();
    DriveApp.getFileById(docFile.id).setTrashed(true);
    
    // 3. Parse information from the text
    const invoiceData = extractInvoiceData(extractedText);
    
    // 4. Determine Dynamic Coding (360100, RDC, CAT)
    const appliedCoding = determineCoding(extractedText);
    
    // 5. Log data to the Invoice Logger Sheet
    sheet.appendRow([
      new Date(),
      source,
      fileName,
      invoiceData.invoiceNumber, // Added
      invoiceData.po,
      invoiceData.shipDate,
      invoiceData.deliveryDate,
      invoiceData.amount,
      invoiceData.origin,
      invoiceData.productType,
      invoiceData.destination,
      invoiceData.remitInfo,     // Added
      appliedCoding
    ]);
    
    // 6. Generate the pretty PDF coding summary directly via HTML (Bypassing Google Sheets export)
    const generatedCodedPdfBlob = generateHtmlPdf(invoiceData, appliedCoding, fileName);
    
    // 7. Send the email with BOTH the untouched original and the new HTML-generated summary PDF
    MailApp.sendEmail({
      to: CONFIG.TARGET_EMAIL,
      subject: `Processed Invoice: ${fileName}`,
      body: `Attached are two files:\n1. The original untouched invoice.\n2. The newly generated Coded Summary sheet.\n\nExtracted PO: ${invoiceData.po}\nCoding Applied: ${appliedCoding}`,
      attachments: [pdfBlob, generatedCodedPdfBlob]
    });
    
    Logger.log(`Successfully processed: ${fileName}`);
    return true;
    
  } catch (e) {
    Logger.log(`Error processing ${fileName}: ${e.message}`);
    return false;
  }
}

/**
 * Helper: Generates a highly formatted PDF Summary directly from HTML code.
 * This guarantees a clean look every time without relying on spreadsheet template layouts.
 */
function generateHtmlPdf(data, coding, originalName) {
  // We replace \n with <br> for the remittance block so the line breaks render properly in the PDF
  const safeRemitHTML = data.remitInfo.replace(/\n/g, '<br>');
  
  const htmlContent = `
    <div style="font-family: Arial, sans-serif; padding: 30px; color: #333;">
      <h1 style="border-bottom: 2px solid #4285F4; padding-bottom: 10px; color: #4285F4;">Invoice Processing Summary</h1>
      
      <table style="width: 100%; border-collapse: collapse; margin-top: 20px; font-size: 14px;">
        <tr>
          <td style="padding: 12px 8px; border-bottom: 1px solid #eee; width: 30%; background-color: #f8f9fa;"><strong>Coding Applied</strong></td>
          <td style="padding: 12px 8px; border-bottom: 1px solid #eee; font-weight: bold; color: #d93025; font-size: 16px;">${coding}</td>
        </tr>
        <tr>
          <td style="padding: 12px 8px; border-bottom: 1px solid #eee; background-color: #f8f9fa;"><strong>Invoice #</strong></td>
          <td style="padding: 12px 8px; border-bottom: 1px solid #eee;">${data.invoiceNumber}</td>
        </tr>
        <tr>
          <td style="padding: 12px 8px; border-bottom: 1px solid #eee; background-color: #f8f9fa;"><strong>PO #</strong></td>
          <td style="padding: 12px 8px; border-bottom: 1px solid #eee;">${data.po}</td>
        </tr>
        <tr>
          <td style="padding: 12px 8px; border-bottom: 1px solid #eee; background-color: #f8f9fa;"><strong>Ship Date</strong></td>
          <td style="padding: 12px 8px; border-bottom: 1px solid #eee;">${data.shipDate}</td>
        </tr>
        <tr>
          <td style="padding: 12px 8px; border-bottom: 1px solid #eee; background-color: #f8f9fa;"><strong>Delivery Date</strong></td>
          <td style="padding: 12px 8px; border-bottom: 1px solid #eee;">${data.deliveryDate}</td>
        </tr>
        <tr>
          <td style="padding: 12px 8px; border-bottom: 1px solid #eee; background-color: #f8f9fa;"><strong>Amount</strong></td>
          <td style="padding: 12px 8px; border-bottom: 1px solid #eee; font-weight: bold;">$${data.amount.replace('$', '')}</td>
        </tr>
        <tr>
          <td style="padding: 12px 8px; border-bottom: 1px solid #eee; background-color: #f8f9fa;"><strong>Origin</strong></td>
          <td style="padding: 12px 8px; border-bottom: 1px solid #eee;">${data.origin}</td>
        </tr>
        <tr>
          <td style="padding: 12px 8px; border-bottom: 1px solid #eee; background-color: #f8f9fa;"><strong>Destination</strong></td>
          <td style="padding: 12px 8px; border-bottom: 1px solid #eee;">${data.destination}</td>
        </tr>
        <tr>
          <td style="padding: 12px 8px; border-bottom: 1px solid #eee; background-color: #f8f9fa;"><strong>Product Type</strong></td>
          <td style="padding: 12px 8px; border-bottom: 1px solid #eee;">${data.productType}</td>
        </tr>
        <tr>
          <td style="padding: 12px 8px; border-bottom: 1px solid #eee; background-color: #f8f9fa;"><strong>Remittance Info</strong></td>
          <td style="padding: 12px 8px; border-bottom: 1px solid #eee; color: #555;">${safeRemitHTML}</td>
        </tr>
      </table>
      
      <p style="margin-top: 40px; font-size: 12px; color: #999; text-align: center;">
        Document automatically generated by Google Apps Script<br>
        Original File: ${originalName}
      </p>
    </div>
  `;
  
  // Convert HTML directly into a PDF Blob
  const blob = Utilities.newBlob(htmlContent, MimeType.HTML, 'temp.html');
  return blob.getAs(MimeType.PDF).setName('Coded_Summary_' + originalName);
}

/**
 * Helper: Parses invoice text for specific data points using robust fallback patterns.
 */
function extractInvoiceData(text) {
  // 1. Clean up brutal OCR artifacts
  let cleanText = text
    .replace(/","/g, '\n')        
    .replace(/"\s*,\s*"/g, '\n')  
    .replace(/",\s*\n/g, '\n')    
    .replace(/\n\s*,\s*"/g, '\n') 
    .replace(/"/g, '')            
    .replace(/\r\n/g, '\n');      

  // 2. Helper to trim spaces. If preserveLines is true, it keeps line breaks intact (useful for Remittance)
  const clean = (val, fallback, preserveLines = false) => {
    if (!val) return fallback;
    if (preserveLines) return val.trim() || fallback;
    return val.replace(/\s+/g, ' ').trim() || fallback;
  };

  // 3. Helper to test multiple regex patterns
  const extractFirst = (regexes) => {
    for (let rx of regexes) {
      const match = cleanText.match(rx);
      if (match && match[1] && match[1].trim().length > 1) {
        const val = match[1].trim();
        // Ignore "headers" that accidentally get captured
        if (!/^(drop|pickup|destination|origin|weight|commodity|equipment|total|amount|number|description|date)$/i.test(val)) {
          return val;
        }
      }
    }
    return null;
  };

  // --- EXTRACTION LOGIC ---
  
  // Extract Invoice Number (Using \b word boundaries to prevent catching "oices" from "Invoices")
  const invoiceNumber = extractFirst([
    /\b(?:Invoice|INV)\b[\s\n]*(?:Number|#)?[\s\n]*:?[\s\n]*([A-Z0-9\-]{3,})/i
  ]);

  // Extract PO Number (Using \b word boundaries to prevent catching "lar" from "Polar")
  const po = extractFirst([
    /\b(?:PO|Purchase\s*Order)\b[\s\n]*(?:Number|#)?[\s\n]*:?[\s\n]*([A-Z0-9\-]{3,})/i
  ]);

  // Extract Amount
  const amount = extractFirst([
    /\b(?:Total|Amount|Balance|LINE HAUL|Amount Due)\b[\s\S]{0,40}?\$?\s*([0-9,]+\.[0-9]{2})/i,
    /\$\s*([0-9,]+\.[0-9]{2})/
  ]);
  
  // Extract Remittance Information
  const remitMatch = cleanText.match(/\b(?:ACH Remittance|Payment Remittance(?: Instructions)?|Remit To|Bank Transfers)\b[\s\:]*\n+([\s\S]{15,250}?)(?:Total|Amount|Special Instructions|Page\s*\d|Notes|$)/i);
  const remitInfo = remitMatch ? remitMatch[1] : null;

  // Extract Dates 
  let shipDate = null;
  let deliveryDate = null;
  const dateRegexGlobal = /([a-zA-Z]+\s+\d{1,2},?\s+\d{4}|\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4})/g;
  
  const datesContext = cleanText.match(/\b(?:Pickup|Ship)\b[\s\n]*Date[\s\S]{0,60}?(?:Drop Off|Delivery)[\s\n]*Date[\s\S]{0,60}?(?:\n\n|\n[A-Z])/i);
  if (datesContext) {
    const foundDates = datesContext[0].match(dateRegexGlobal);
    if (foundDates && foundDates.length >= 1) shipDate = foundDates[0];
    if (foundDates && foundDates.length >= 2) deliveryDate = foundDates[1];
  }
  
  if (!shipDate) {
    shipDate = extractFirst([
      /\b(?:Ship|Pickup|Billing)\b[\s\n]*Date[\s\n]*:?[\s\n]*([a-zA-Z]+\s+\d{1,2},?\s+\d{4}|\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4})/i
    ]);
  }
  if (!deliveryDate) {
    deliveryDate = extractFirst([
      /\b(?:Delivery|Drop\s*Off|Due)\b[\s\n]*Date[\s\n]*:?[\s\n]*([a-zA-Z]+\s+\d{1,2},?\s+\d{4}|\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4})/i
    ]);
  }

  // Extract Origin / Shipper
  const origin = extractFirst([
    /\b(?:Shipper|Origin|Stop\s*1)\b[\s\:]*\n+\s*([^,\n]+)/i, 
    /\b(?:Pickup Location|Origin|Stop\s*1)\b[\s\n]*:?[\s\n]*([\s\S]{10,80}?)(?:Destination|Drop Off|Stop\s*2|Weight|Commodity|\n\n|Expected|Invoice)/i 
  ]);

  // Extract Destination / Consignee
  const destination = extractFirst([
    /\b(?:Consignee|Destination|Stop\s*2)\b[\s\:]*\n+\s*([^,\n]+)/i,
    /\b(?:Destination|Drop Off Location|Stop\s*2)\b[\s\n]*:?[\s\n]*([\s\S]{10,80}?)(?:Weight|Billing|Pickup|Invoice|\n\n|Expected|Amount)/i
  ]);

  // Extract Product Type / Commodity
  const productType = extractFirst([
    /\bCommodity\b\s*\n\s*Equipment Type\s*\n\s*([^\n]+)/i, 
    /\b(?:Product|Commodity Description|Item Description)\b[\s\n]*:?[\s\n]*([^\n]+)/i
  ]);

  return {
    invoiceNumber: clean(invoiceNumber, 'Not Found'),
    po: clean(po, 'Not Found'),
    shipDate: clean(shipDate, 'Not Found'),
    deliveryDate: clean(deliveryDate, 'Not Found'),
    amount: clean(amount, 'Not Found'),
    origin: clean(origin, 'Review Required'),
    destination: clean(destination, 'Review Required'),
    productType: clean(productType, 'Review Required'),
    remitInfo: clean(remitInfo, 'Not Found', true) // Preserve Line breaks!
  };
}

/**
 * Helper: Determines the RDC and CAT codes based on keywords found in the invoice.
 */
function determineCoding(text) {
  let rdc = "RDC-UNKNOWN";
  let cat = "CAT 4"; 
  
  // Condense spaces/newlines to ensure keywords aren't split across lines
  const searchSpace = text.replace(/\s+/g, ' ').toLowerCase();

  for (let rule of CONFIG.ROUTING_RULES.rdcRules) {
    if (searchSpace.includes(rule.keyword.toLowerCase())) {
      rdc = rule.code;
      break;
    }
  }

  for (let rule of CONFIG.ROUTING_RULES.catRules) {
    if (searchSpace.includes(rule.keyword.toLowerCase())) {
      cat = rule.code;
      break;
    }
  }

  const outputString = `${CONFIG.ROUTING_RULES.INVOICE_TYPE}, ${rdc}, ${cat}`;
  return outputString.replace(/\s+/g, ' ').trim(); // Final cleanup
}

/**
 * Setup triggers for both Drive and Email processing.
 */
function createTriggers() {
  const triggers = ScriptApp.getProjectTriggers();
  triggers.forEach(t => ScriptApp.deleteTrigger(t));
  
  ScriptApp.newTrigger('processDriveFolders')
    .timeBased()
    .everyMinutes(15)
    .create();
    
  ScriptApp.newTrigger('processIncomingPDFs')
    .timeBased()
    .everyMinutes(15)
    .create();
    
  Logger.log("Triggers created for both Folders and Gmail!");
}