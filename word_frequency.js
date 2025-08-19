#!/usr/bin/env node
const { Command } = require('commander');
const jsforce = require('jsforce');
const fs = require('fs');
const zlib = require('zlib');
const { stringify } = require('csv-stringify/sync');
const { SingleBar, Presets } = require('cli-progress');
// Use JWT-based Salesforce authentication
const authorize = require('./sfdcJwtAuth');

const program = new Command();
program
  .requiredOption('-f, --fields <fields>', 'comma separated fields to parse')
  .requiredOption('-d, --dataset <dataset>', 'CRM Analytics dataset API name')
  .option('--case-id <id>', 'filter by Case Id')
  .option('--csv <file>', 'write output to CSV instead of CRM Analytics')
  .option('--id-field <field>', 'field used as record Id', 'Id')
  .option('-s, --segment <field>', 'field used to segment large queries')
  .option('--folder-id <id>', 'CRM Analytics folder Id to store dataset')
  .parse(process.argv);

const options = program.opts();
const FIELDS = options.fields.split(',').map(f => f.trim()).filter(Boolean);
let ID_FIELD = options.idField;
const FOLDER_ID = options.folderId || process.env.SF_FOLDER_ID;
const SEGMENT_FIELD = options.segment;
const CHUNK_SIZE = 10 * 1024 * 1024; // 10MB per upload chunk

function addWord(map, word, field, caseId) {
  if (!word) return;
  const key = `${field}|${word}`;
  if (!map[key]) {
    map[key] = { word, field, caseIds: new Set() };
  }
  map[key].caseIds.add(caseId);
}

function parseText(text, field, caseId, map) {
  if (!text) return;
  let cleaned = text.replace(/(<[^>]+>)/g, '').toLowerCase();
  cleaned = cleaned.replace(/\n/g, '.');
  cleaned = ` ${cleaned} `;

  const single = /[\W](?:[\w%:]+\W)/g;
  let match;
  while ((match = single.exec(cleaned)) !== null) {
    const word = cleaned.substring(match.index + 1, match.index + match[0].length - 1);
    addWord(map, word, field, caseId);
  }
}

function escapeValue(val) {
  return String(val).replace(/"/g, '\\"');
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
  let url = `/services/data/v60.0/wave/datasets?q=${encodeURIComponent(name)}`;
  while (url) {
    const res = await requestWithRetry(conn, url);
    for (const ds of res.datasets || []) {
      if (ds.name === name) {
        return `${ds.id}/${ds.currentVersionId}`;
      }
    }
    url = res.nextPageUrl || null;
  }
  throw new Error(`Dataset ${name} not found`);
}

async function getSegmentValues(conn, datasetId, field) {
  const saql = `q = load \"${datasetId}\"; q = group q by '${field}'; q = foreach q generate '${field}' as val;`;
  const body = { query: saql };
  const result = await requestWithRetry(conn, {
    method: 'POST',
    url: `/services/data/v60.0/wave/query`,
    body: JSON.stringify(body),
    headers: { 'Content-Type': 'application/json' }
  });
  return (result.results.records || []).map(r => r.val).filter(v => v !== undefined && v !== null);
}

async function fetchRecords(conn, datasetId, segments, progress) {
  if (segments && segments.length) {
    const all = [];
    const CONCURRENT_LIMIT = 5;
    
    const fetchSegment = async (seg, index) => {
      if (progress) progress.update(progress.value, { stage: `Fetching segment ${index + 1}/${segments.length}: ${seg}` });
      
      let saql = `q = load \"${datasetId}\";`;
      saql += ` q = filter q by '${SEGMENT_FIELD}' == \"${escapeValue(seg)}\";`;
      if (options.caseId) {
        saql += ` q = filter q by '${ID_FIELD}' == \"${options.caseId}\";`;
      }
      saql += ` q = foreach q generate '${ID_FIELD}'${FIELDS.map(f => `, '${f}'`).join('')};`;
      const body = { query: saql };
      const res = await requestWithRetry(conn, {
        method: 'POST',
        url: `/services/data/v60.0/wave/query`,
        body: JSON.stringify(body),
        headers: { 'Content-Type': 'application/json' }
      });
      if (progress) progress.increment(1, { stage: `Segment ${index + 1} complete (${res.results.records.length} records)` });
      return res.results.records;
    };

    for (let i = 0; i < segments.length; i += CONCURRENT_LIMIT) {
      const batch = segments.slice(i, i + CONCURRENT_LIMIT);
      const promises = batch.map((seg, batchIndex) => fetchSegment(seg, i + batchIndex));
      const results = await Promise.all(promises);
      results.forEach(records => all.push(...records));
    }
    return all;
  } else {
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
    if (progress) progress.increment(1, { stage: `Data fetch complete (${result.results.records.length} records)` });
    return result.results.records;
  }
}

function buildDatasetArray(map) {
  const dataset = [];
  for (const key of Object.keys(map)) {
    const k = map[key];
    for (const id of k.caseIds) {
      dataset.push({ Field: k.field, Word: k.word, CaseId: id });
    }
  }
  return dataset;
}

async function uploadDataset(conn, records, progress) {
  // Upload dataset using InsightsExternalData SObjects API for compatibility
  if (progress) progress.update(progress.value, { stage: 'Preparing CSV data...' });
  const csv = stringify(records, { header: true });
  const gz = zlib.gzipSync(Buffer.from(csv));
  const encoded = gz.toString('base64');

  const chunkCount = Math.ceil(encoded.length / CHUNK_SIZE);
  if (progress) {
    progress.setTotal(progress.total + chunkCount);
    progress.update(progress.value, { stage: `Prepared ${chunkCount} upload chunks` });
  }

  const metadata = {
    fileFormat: { charsetName: 'UTF-8', fieldsDelimitedBy: ',', linesTerminatedBy: '\r\n' },
    objects: [{
      connector: 'CSV',
      fullyQualifiedName: 'WordFrequency',
      label: 'WordFrequency',
      name: 'WordFrequency',
      fields: [
        { fullyQualifiedName: 'Field', name: 'Field', type: 'Text', label: 'Field' },
        { fullyQualifiedName: 'Word', name: 'Word', type: 'Text', label: 'Word' },
        { fullyQualifiedName: 'CaseId', name: 'CaseId', type: 'Text', label: 'CaseId' }
      ]
    }]
  };

  const header = {
    Format: 'Csv',
    EdgemartAlias: 'Word_Frequency_File',
    EdgemartLabel: 'Word Frequency (File)',
    ...(FOLDER_ID ? { EdgemartContainer: FOLDER_ID } : {}),
    Action: 'None',
    Operation: 'Overwrite',
    MetadataJson: Buffer.from(JSON.stringify(metadata)).toString('base64')
  };

  if (progress) progress.update(progress.value, { stage: 'Creating upload session...' });
  const res = await requestWithRetry(conn, {
    method: 'POST',
    url: `/services/data/v60.0/sobjects/InsightsExternalData`,
    body: JSON.stringify(header),
    headers: { 'Content-Type': 'application/json' }
  });

  const id = res.id;

  for (let i = 0; i < chunkCount; i++) {
    if (progress) progress.update(progress.value, { stage: `Uploading chunk ${i + 1}/${chunkCount}...` });
    
    const part = {
      InsightsExternalDataId: id,
      PartNumber: i + 1,
      DataFile: encoded.slice(i * CHUNK_SIZE, (i + 1) * CHUNK_SIZE)
    };

    await requestWithRetry(conn, {
      method: 'POST',
      url: `/services/data/v60.0/sobjects/InsightsExternalDataPart`,
      body: JSON.stringify(part),
      headers: { 'Content-Type': 'application/json' }
    });

    if (progress) progress.increment(1, { stage: `Chunk ${i + 1}/${chunkCount} uploaded` });
  }

  if (progress) progress.update(progress.value, { stage: 'Triggering data processing...' });
  await requestWithRetry(conn, {
    method: 'PATCH',
    url: `/services/data/v60.0/sobjects/InsightsExternalData/${id}`,
    body: JSON.stringify({ Action: 'Process' }),
    headers: { 'Content-Type': 'application/json' }
  });
}

async function main() {
  // Calculate total steps for complete lifecycle tracking
  let totalSteps = 1; // authorization
  totalSteps += 1; // dataset lookup
  if (SEGMENT_FIELD) totalSteps += 1; // segment values retrieval
  totalSteps += (SEGMENT_FIELD ? 0 : 1); // data fetching (will be calculated later for segments)
  totalSteps += 1; // text parsing
  totalSteps += 1; // dataset building
  if (!options.csv) totalSteps += 1; // upload preparation + final processing

  const progress = new SingleBar({
    format: 'Progress |{bar}| {percentage}% | {value}/{total} Steps | {stage}',
    barCompleteChar: '\u2588',
    barIncompleteChar: '\u2591',
  }, Presets.shades_classic);
  progress.start(totalSteps, 0, { stage: 'Starting...' });

  // Step 1: Authorization
  progress.update(0, { stage: 'Authorizing...' });
  authorize();
  const conn = new jsforce.Connection({
    instanceUrl: process.env.SF_INSTANCE_URL,
    accessToken: process.env.SF_ACCESS_TOKEN
  });
  progress.increment(1, { stage: 'Authorization complete' });

  // Step 2: Dataset lookup
  progress.update(progress.value, { stage: 'Looking up dataset...' });
  const datasetId = await getDatasetId(conn, options.dataset);
  progress.increment(1, { stage: 'Dataset found' });

  // Step 3: Segment values (if needed)
  let segments = [];
  if (SEGMENT_FIELD) {
    progress.update(progress.value, { stage: 'Retrieving segments...' });
    segments = await getSegmentValues(conn, datasetId, SEGMENT_FIELD);
    progress.increment(1, { stage: `Found ${segments.length} segments` });
    // Update total steps to include segment processing
    progress.setTotal(totalSteps - 1 + segments.length);
  }

  // Step 4: Data fetching
  progress.update(progress.value, { stage: 'Fetching data...' });
  let records;
  try {
    records = await fetchRecords(conn, datasetId, segments, progress);
  } catch (err) {
    if (err && err.errorCode === '119' && ID_FIELD === 'Id') {
      console.log('ℹ Id field not found, retrying with Record');
      ID_FIELD = 'Record';
      records = await fetchRecords(conn, datasetId, segments, progress);
    } else {
      progress.stop();
      throw err;
    }
  }

  // Step 5: Text parsing
  progress.update(progress.value, { stage: `Processing ${records.length} records...` });
  const map = {};
  for (const rec of records) {
    for (const f of FIELDS) {
      parseText(rec[f], f, rec[ID_FIELD], map);
    }
  }
  progress.increment(1, { stage: 'Text parsing complete' });

  // Step 6: Dataset building
  progress.update(progress.value, { stage: 'Building dataset...' });
  const dataset = buildDatasetArray(map);
  progress.increment(1, { stage: `Dataset built with ${dataset.length} records` });

  if (options.csv) {
    progress.update(progress.value, { stage: 'Writing CSV file...' });
    fs.writeFileSync(options.csv, stringify(dataset, { header: true }));
    progress.update(progress.total, { stage: 'CSV file written successfully' });
    progress.stop();
  } else {
    progress.update(progress.value, { stage: 'Uploading to Salesforce...' });
    await uploadDataset(conn, dataset, progress);
    progress.update(progress.total, { stage: 'Upload complete' });
    progress.stop();
  }
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
