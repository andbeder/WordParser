#!/usr/bin/env node
const { Command } = require('commander');
const jsforce = require('jsforce');
const fs = require('fs');
const zlib = require('zlib');
const { stringify } = require('csv-stringify/sync');
// Use JWT-based Salesforce authentication
const authorize = require('./sfdcJwtAuth');

const program = new Command();
program
  .requiredOption('-f, --fields <fields>', 'comma separated fields to parse')
  .requiredOption('-d, --dataset <dataset>', 'CRM Analytics dataset API name')
  .option('--case-id <id>', 'filter by Case Id')
  .option('--csv <file>', 'write output to CSV instead of CRM Analytics')
  .option('--id-field <field>', 'field used as record Id', 'Id')
  .parse(process.argv);

const options = program.opts();
const FIELDS = options.fields.split(',').map(f => f.trim()).filter(Boolean);
let ID_FIELD = options.idField;

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
  let cleaned = text.replace(/(<[^>]+>)/g, '').toLowerCase();
  cleaned = cleaned.replace(/\n/g, '.');
  cleaned = ` ${cleaned} `;

  const phrase3 = /[\W](?:[\w%:]+\W){3}/g;
  let match;
  while ((match = phrase3.exec(cleaned)) !== null) {
    const phrase = cleaned.substring(match.index + 1, match.index + match[0].length - 1);
    addWord(map, phrase, field, 'Phrase', caseId);
  }

  const phrase2 = /[\W](?:[\w%:]+\W){2}/g;
  while ((match = phrase2.exec(cleaned)) !== null) {
    const phrase = cleaned.substring(match.index + 1, match.index + match[0].length - 1);
    addWord(map, phrase, field, 'Phrase', caseId);
  }

  const single = /[\W](?:[\w%:]+\W)/g;
  while ((match = single.exec(cleaned)) !== null) {
    const word = cleaned.substring(match.index + 1, match.index + match[0].length - 1);
    addWord(map, word, field, 'Word', caseId);
  }
}

async function requestWithRetry(conn, opts, retry = true) {
  try {
    return await conn.request(opts);
  } catch (err) {
    if (retry && err && err.errorCode === 'INVALID_SESSION_ID') {
      console.log('ℹ Session expired, reauthorizing...');
      authorize();
      if (typeof conn._establish === 'function') {
        conn._establish({
          instanceUrl: process.env.SF_INSTANCE_URL,
          accessToken: process.env.SF_ACCESS_TOKEN
        });
      } else {
        conn.instanceUrl = process.env.SF_INSTANCE_URL;
        conn.accessToken = process.env.SF_ACCESS_TOKEN;
      }
      return requestWithRetry(conn, opts, false);
    }
    throw err;
  }
}

async function getDatasetId(conn, name) {
  const url = `/services/data/v60.0/wave/datasets?q=${encodeURIComponent(name)}`;
  const res = await requestWithRetry(conn, url);
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
    saql += ` q = filter q by '${ID_FIELD}' == \"${options.caseId}\";`;
  }
  saql += ` q = foreach q generate '${ID_FIELD}'${FIELDS.map(f => `, '${f}'`).join('')};`;
  const body = { query: saql };
  const result = await requestWithRetry(conn, {
    method: 'POST',
    url: `/services/data/v60.0/wave/query`,
    body: JSON.stringify(body),
    headers: { 'Content-Type': 'application/json' }
  });
  return result.results.records;
}

function buildDatasetArray(map) {
  const dataset = [];
  for (const key of Object.keys(map)) {
    const k = map[key];
    for (const id of k.caseIds) {
      dataset.push({ Field: k.field, Type: k.type, Word: k.word, CaseId: id });
    }
  }
  return dataset;
}

async function uploadDataset(conn, records) {
  const csv = stringify(records, { header: true });
  const gz = zlib.gzipSync(Buffer.from(csv));
  const encoded = gz.toString('base64');

  const metadata = {
    fileFormat: { charsetName: 'UTF-8', fieldsDelimitedBy: ',', linesTerminatedBy: '\r\n' },
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

  await requestWithRetry(conn, {
    method: 'POST',
    url: `/services/data/v60.0/wave/datasets`,
    body: JSON.stringify(body),
    headers: { 'Content-Type': 'application/json' }
  });
}

async function main() {
  authorize();
  const conn = new jsforce.Connection({
    instanceUrl: process.env.SF_INSTANCE_URL,
    accessToken: process.env.SF_ACCESS_TOKEN
  });

  let records;
  try {
    records = await fetchRecords(conn);
  } catch (err) {
    if (err && err.errorCode === '119' && ID_FIELD === 'Id') {
      console.log('ℹ Id field not found, retrying with Record');
      ID_FIELD = 'Record';
      records = await fetchRecords(conn);
    } else {
      throw err;
    }
  }
  const map = {};
  for (const rec of records) {
    for (const f of FIELDS) {
      parseText(rec[f], f, rec[ID_FIELD], map);
    }
  }

  const dataset = buildDatasetArray(map);

  if (options.csv) {
    fs.writeFileSync(options.csv, stringify(dataset, { header: true }));
  } else {
    await uploadDataset(conn, dataset);
  }
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
