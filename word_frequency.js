#!/usr/bin/env node

const { Command } = require('commander');
const jsforce = require('jsforce');
const fs = require('fs');
const zlib = require('zlib');
const stringify = require('csv-stringify/sync');

const program = new Command();
program
  .requiredOption('-f, --fields <fields>', 'comma separated fields to parse')
  .requiredOption('-d, --dataset <dataset>', 'CRM Analytics dataset API name')
  .option('--case-id <id>', 'filter by Case Id')
  .option('--csv <file>', 'write output to CSV instead of CRM Analytics')
  .parse(process.argv);

const options = program.opts();
const FIELDS = options.fields.split(',').map(f => f.trim());

function addWord(map, word, field, type, caseId) {
  if (!word) return;
  const key = `${field}|${word}`;
  if (!map[key]) {
    map[key] = { word, field, type, caseIds: new Set() };
  }
  map[key].caseIds.add(caseId);
}

function parseText(text, field, caseId, map) {
  if (!text) return;
  let cleaned = text.replace(/(<(.*?)>)/g, '').toLowerCase();
  cleaned = cleaned.replace(/\n/g, '.');
  cleaned = ` ${cleaned} `;

  const phrase3 = /[\W][\w%:]+[\W][\w%:]+[\W][\w%:]+[\W]/g;
  let match;
  let start = 0;
  while ((match = phrase3.exec(cleaned)) !== null) {
    const phrase = cleaned.substring(match.index + 1, match.index + match[0].length - 1);
    addWord(map, phrase, field, 'Phrase', caseId);
    start = match.index + 1;
  }

  const phrase2 = /[\W][\w%:]+[\W][\w%:]+[\W]/g;
  start = 0;
  while ((match = phrase2.exec(cleaned)) !== null) {
    const phrase = cleaned.substring(match.index + 1, match.index + match[0].length - 1);
    addWord(map, phrase, field, 'Phrase', caseId);
    start = match.index + 1;
  }

  const single = /[\W][\w%:]+[\W]/g;
  start = 0;
  while ((match = single.exec(cleaned)) !== null) {
    const word = cleaned.substring(match.index + 1, match.index + match[0].length - 1);
    addWord(map, word, field, 'Word', caseId);
    start = match.index + 1;
  }
}

async function getDatasetId(conn, name) {
  const url = `/services/data/v60.0/wave/datasets?q=${encodeURIComponent(name)}`;
  const res = await conn.request(url);
  for (const ds of res.datasets || []) {
    if (ds.name === name) {
      return `${ds.id}/${ds.currentVersionId}`;
    }
  }
  throw new Error(`Dataset ${name} not found`);
}

async function fetchRecords(conn) {
  const datasetId = await getDatasetId(conn, options.dataset);
  let saql = `q = load \"${datasetId}\";`;
  if (options.caseId) {
    saql += ` q = filter q by 'Id' == \"${options.caseId}\";`;
  }
  saql += ` q = foreach q generate Id${FIELDS.map(f => `, '${f}'`).join('')};`;
  const body = { query: saql };
  const result = await conn.request({
    method: 'POST',
    url: `/services/data/v60.0/wave/query`,
    body,
    headers: { 'Content-Type': 'application/json' }
  });
  return result.results.records;
}

async function uploadDataset(conn, records) {
  const csv = stringify.stringify(records, { header: true });
  const gz = zlib.gzipSync(Buffer.from(csv));
  const encoded = gz.toString('base64');

  const metadata = {
    fileFormat: {
      charsetName: 'UTF-8',
      fieldsDelimitedBy: ',',
      linesTerminatedBy: '\r\n'
    },
    objects: [{
      connector: 'CSV',
      fullyQualifiedName: 'WordFrequency',
      label: 'WordFrequency',
      name: 'WordFrequency',
      fields: [
        { fullyQualifiedName: 'Field', name: 'Field', type: 'Text', label: 'Field' },
        { fullyQualifiedName: 'Type', name: 'Type', type: 'Text', label: 'Type' },
        { fullyQualifiedName: 'Word', name: 'Word', type: 'Text', label: 'Word' },
        { fullyQualifiedName: 'CaseId', name: 'CaseId', type: 'Text', label: 'CaseId' }
      ]
    }]
  };

  const body = {
    datasetName: 'Word_Frequency_File',
    datasetLabel: 'Word Frequency (File)',
    operation: 'Overwrite',
    metadataJson: JSON.stringify(metadata),
    data: encoded
  };

  await conn.request({
    method: 'POST',
    url: `/services/data/v60.0/wave/datasets`,
    body,
    headers: { 'Content-Type': 'application/json' }
  });
}

async function main() {
  const conn = new jsforce.Connection({ loginUrl: process.env.SF_LOGIN_URL || 'https://login.salesforce.com' });
  await conn.login(process.env.SF_USERNAME, process.env.SF_PASSWORD);

  const records = await fetchRecords(conn);
  const map = {};
  for (const rec of records) {
    for (const f of FIELDS) {
      parseText(rec[f], f, rec.Id, map);
    }
  }

  const dataset = [];
  for (const key of Object.keys(map)) {
    const k = map[key];
    for (const id of k.caseIds) {
      dataset.push({ Field: k.field, Type: k.type, Word: k.word, CaseId: id });
    }
  }

  if (options.csv) {
    fs.writeFileSync(options.csv, stringify.stringify(dataset, { header: true }));
  } else {
    await uploadDataset(conn, dataset);
  }
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
