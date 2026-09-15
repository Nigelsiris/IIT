/**
 * Minimal stand-in for a Google Spreadsheet, just enough surface for the
 * spreadsheet-invoice parser: getSheets / getName / getDataRange /
 * getValues / getDisplayValues.
 *
 * Build one from { SheetName: { raw: [[...]], display: [[...]] } }.
 */
function makeSheet(name, data) {
  const raw = (data.raw || []).map(row => row.map(reviveCell));
  const display = (data.display || []).map(row => row.map(cell => (cell === null || cell === undefined ? '' : String(cell))));
  return {
    getName: () => name,
    getDataRange: () => ({
      getValues: () => raw,
      getDisplayValues: () => display
    })
  };
}

function reviveCell(cell) {
  if (cell && typeof cell === 'object' && cell.__date__) {
    return new Date(cell.__date__);
  }
  return cell === null || cell === undefined ? '' : cell;
}

function makeSpreadsheet(workbook) {
  const sheets = Object.keys(workbook).map(name => makeSheet(name, workbook[name]));
  return {
    getSheets: () => sheets,
    getSheetByName: name => sheets.find(sheet => sheet.getName() === name) || null
  };
}

/**
 * Build a workbook from a compact grid: the first row is the header, the rest
 * are data rows. Values are passed through as both raw and display.
 */
function makeGridWorkbook(sheetName, grid) {
  const display = grid.map(row => row.map(cell => (cell === null || cell === undefined ? '' : String(cell))));
  return { [sheetName]: { raw: grid, display } };
}

module.exports = { makeSpreadsheet, makeGridWorkbook };
