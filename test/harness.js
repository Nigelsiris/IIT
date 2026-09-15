/**
 * Loads code.js into a sandbox with just enough Apps Script stubbed out that
 * the pure extraction/parsing functions can be exercised from Node.
 *
 * Run with:  node test/run-tests.js
 */
const fs = require('fs');
const path = require('path');
const vm = require('vm');

function buildSandbox() {
  const noop = () => {};
  const Utilities = {
    DigestAlgorithm: { MD5: 'MD5' },
    Charset: { UTF_8: 'UTF_8' },
    computeDigest: (alg, value) => {
      const crypto = require('crypto');
      const buf = crypto.createHash('md5').update(String(value)).digest();
      return Array.from(buf).map(b => (b > 127 ? b - 256 : b));
    },
    formatDate: (date, tz, fmt) => {
      const pad = n => String(n).padStart(2, '0');
      return fmt
        .replace('MM', pad(date.getMonth() + 1))
        .replace('dd', pad(date.getDate()))
        .replace('yyyy', date.getFullYear());
    },
    newBlob: (content, type, name) => {
      const blob = {
        content,
        getName() { return name; },
        getContentType() { return type; },
        setName(n) { name = n; return blob; },
        setContentType(t) { type = t; return blob; },
        getBytes() { return Buffer.from(String(content)); },
        copyBlob() { return Utilities.newBlob(content, type, name); },
        getAs(target) { return Utilities.newBlob(content, target, name); }
      };
      return blob;
    },
    unzip: () => [],
    sleep: () => {},
    base64Decode: s => Buffer.from(s, 'base64')
  };

  const sandbox = {
    console,
    Utilities,
    Logger: { log: noop },
    MimeType: { PDF: 'application/pdf', HTML: 'text/html' },
    Session: { getScriptTimeZone: () => 'UTC' },
    PropertiesService: {
      getScriptProperties: () => ({
        getProperty: () => null,
        setProperty: noop,
        deleteProperty: noop,
        getProperties: () => ({})
      })
    },
    DriveApp: {}, Drive: {}, DocumentApp: {}, SpreadsheetApp: {},
    GmailApp: {}, MailApp: {}, ScriptApp: {}, UrlFetchApp: {}, LockService: {},
    HtmlService: {}
  };

  vm.createContext(sandbox);
  let source = fs.readFileSync(path.join(__dirname, '..', 'code.js'), 'utf8');

  // Top-level `function` declarations become globals in the sandbox, but
  // top-level `const` stays in the script's own lexical scope. Re-export the
  // constants so tests can assert against them too.
  const constNames = [];
  const declaration = /^const\s+([A-Za-z_$][\w$]*)\s*=/gm;
  let match;
  while ((match = declaration.exec(source)) !== null) {
    if (constNames.indexOf(match[1]) === -1) constNames.push(match[1]);
  }
  source += '\n;' + constNames.map(name => `globalThis.${name} = ${name};`).join('\n');

  vm.runInContext(source, sandbox, { filename: 'code.js' });
  return sandbox;
}

module.exports = { buildSandbox };
