async function dynamicMergeAndRoute() {
  // ---------------------------------------------------------
  // 1. CONFIGURATION & IDs
  // ---------------------------------------------------------
  const inputFolderId = "1tZe3fMH1s9cl3EMijdqhecXpgY6n-scr"; 
  const parentOutputFolderId = "1suhe1rYmaAVtE0Ul4BxEn5DXySO7B4G8"; // Master folder for carrier subfolders
  const trackerSheetId = "1dCjBTgYL9yPzyZd1PNM4B-R6Lv2SnOcDSzrQizV8XAg"; 
  
  const inputFolder = DriveApp.getFolderById(inputFolderId);
  const parentOutputFolder = DriveApp.getFolderById(parentOutputFolderId);
  const files = inputFolder.getFilesByType(MimeType.PDF);
  
  // ---------------------------------------------------------
  // 2. GROUP FILES BY CARRIER
  // ---------------------------------------------------------
  const fileGroups = {};
  let totalFiles = 0;

  while (files.hasNext()) {
    const file = files.next();
    const fileName = file.getName();
    
    // Extract the carrier name looking for either a hyphen or an underscore
    let carrierName = fileName.split(/[-_]/)[0].trim();
    if (!carrierName || !/[-_]/.test(fileName)) carrierName = "Unknown_Carrier";
    if (!fileGroups[carrierName]) fileGroups[carrierName] = [];
    fileGroups[carrierName].push(file);
    totalFiles++;
  }

  if (totalFiles === 0) {
    Logger.log("No PDFs found in the input folder. Exiting.");
    return;
  }

  // Load pdf-lib just once for the whole process
  const cdnUrl = "https://cdn.jsdelivr.net/npm/pdf-lib/dist/pdf-lib.min.js";
  eval(UrlFetchApp.fetch(cdnUrl).getContentText().replace(/setTimeout\(.*?,.*?(\d*?)\)/g, "Utilities.sleep($1);return t();"));

  const sheet = SpreadsheetApp.openById(trackerSheetId).getActiveSheet();

  // ---------------------------------------------------------
  // 3. PROCESS EACH CARRIER GROUP
  // ---------------------------------------------------------
  for (const carrier in fileGroups) {
    Logger.log(`Processing ${fileGroups[carrier].length} files for carrier: ${carrier}`);
    
    // Sort files alphabetically
    const carrierFiles = fileGroups[carrier];
    carrierFiles.sort((a, b) => a.getName().localeCompare(b.getName()));

    // Merge the PDFs for this specific carrier
    const mergedPdf = await PDFLib.PDFDocument.create();
    for (let i = 0; i < carrierFiles.length; i++) {
      const pdfData = new Uint8Array(carrierFiles[i].getBlob().getBytes());
      const pdfDoc = await PDFLib.PDFDocument.load(pdfData);
      const pages = await mergedPdf.copyPages(pdfDoc, pdfDoc.getPageIndices());
      pages.forEach(page => mergedPdf.addPage(page));
    }

    const bytes = await mergedPdf.save();
    
    // ---------------------------------------------------------
    // 4. DYNAMIC FOLDER CREATION & ROUTING
    // ---------------------------------------------------------
    let targetFolder;
    const folderIterator = parentOutputFolder.getFoldersByName(carrier);
    
    // Check if the carrier folder already exists. If not, create it!
    if (folderIterator.hasNext()) {
      targetFolder = folderIterator.next();
    } else {
      Logger.log(`Folder for ${carrier} not found. Creating new folder...`);
      targetFolder = parentOutputFolder.createFolder(carrier);
    }

    // Save the newly merged file into the carrier folder
    const finalFileName = `${carrier}_Merged_${new Date().getTime()}.pdf`;
    const mergedBlob = Utilities.newBlob(Array.from(new Int8Array(bytes)), MimeType.PDF, finalFileName);
    const finalFile = targetFolder.createFile(mergedBlob);

    // Log to tracker
    sheet.appendRow([new Date(), carrier, finalFileName, finalFile.getUrl()]);

    // ---------------------------------------------------------
    // 5. CLEAN UP INPUT FOLDER (Recommended)
    // ---------------------------------------------------------

     for (let i = 0; i < carrierFiles.length; i++) {
       carrierFiles[i].setTrashed(true);
     }
  }
  
  Logger.log("All processing complete!");
}