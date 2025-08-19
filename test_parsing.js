#!/usr/bin/env node
const { Command } = require('commander');
const jsforce = require('jsforce');
const { SingleBar, Presets } = require('cli-progress');
const authorize = require('./sfdcJwtAuth');

const program = new Command();
program
  .requiredOption('-f, --fields <fields>', 'comma separated fields to parse')
  .requiredOption('-d, --dataset <dataset>', 'CRM Analytics dataset API name')
  .option('--case-id <id>', 'filter by Case Id')
  .option('--id-field <field>', 'field used as record Id', 'Id')
  .option('-s, --segment <field>', 'field used to segment large queries')
  .option('--filter <condition>', 'custom SAQL filter condition')
  .option('--test-records <count>', 'number of records to test (default: 5)', '5')
  .parse(process.argv);

const options = program.opts();
const FIELDS = options.fields.split(',').map(f => f.trim()).filter(Boolean);
let ID_FIELD = options.idField;
const SEGMENT_FIELD = options.segment;
const CUSTOM_FILTER = options.filter ? normalizeFilter(options.filter) : null;
const TEST_RECORD_COUNT = parseInt(options.testRecords);

function normalizeFilter(filter) {
  return filter.replace(/==\s*([^"'\s][^"'\s\]]+)(?=\s|$)/g, '== "$1"')
               .replace(/!=\s*([^"'\s][^"'\s\]]+)(?=\s|$)/g, '!= "$1"');
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
  let url = `/services/data/v60.0/wave/datasets`;
  let allDatasets = [];
  while (url) {
    const res = await requestWithRetry(conn, url);
    const datasets = res.datasets || [];
    allDatasets.push(...datasets);
    
    for (const ds of datasets) {
      if (ds.name === name) {
        return `${ds.id}/${ds.currentVersionId}`;
      }
    }
    url = res.nextPageUrl || null;
  }
  
  const availableNames = allDatasets.map(ds => ds.name).sort();
  throw new Error(`Dataset ${name} not found. Available datasets: ${availableNames.slice(0, 10).join(', ')}${availableNames.length > 10 ? ` (and ${availableNames.length - 10} more)` : ''}`);
}

async function getTestRecords(conn, datasetId) {
  let saql = `q = load "${datasetId}";`;
  if (options.caseId) {
    saql += ` q = filter q by '${ID_FIELD}' == "${options.caseId}";`;
  }
  if (CUSTOM_FILTER) {
    saql += ` q = filter q by ${CUSTOM_FILTER};`;
  }
  saql += ` q = foreach q generate '${ID_FIELD}'${FIELDS.map(f => `, '${f}'`).join('')}; q = limit q ${TEST_RECORD_COUNT};`;
  
  const body = { query: saql };
  const result = await requestWithRetry(conn, {
    method: 'POST',
    url: `/services/data/v60.0/wave/query`,
    body: JSON.stringify(body),
    headers: { 'Content-Type': 'application/json' }
  });
  return result.results.records;
}

function parseTextWithDetails(text, field, caseId) {
  if (!text) return [];
  
  console.log(`\n=== PARSING ANALYSIS FOR ${field} (${caseId}) ===`);
  console.log('ORIGINAL TEXT:');
  console.log(`"${text}"`);
  console.log(`Length: ${text.length} characters`);
  
  // NEW PARSING METHOD (Updated)
  let cleaned = text.replace(/(<[^>]+>)/g, '').toLowerCase();
  cleaned = cleaned.replace(/\n/g, ' ');
  
  console.log('\nAFTER CLEANING:');
  console.log(`"${cleaned}"`);
  
  // Extract words using word boundaries - much more accurate
  const words = cleaned.match(/\b\w+\b/g) || [];
  console.log(`\nNEW PARSING METHOD (${words.length} words):`);
  console.log(words.map(w => `"${w}"`).join(', '));
  
  // Alternative parsing methods for comparison
  console.log('\n=== COMPARISON METHODS ===');
  
  // OLD METHOD: Complex regex (for reference)
  const oldCleaned = ` ${text.replace(/(<[^>]+>)/g, '').toLowerCase().replace(/\n/g, '.')} `;
  const single = /[\W](?:[\w%:]+\W)/g;
  const oldWords = [];
  let match;
  while ((match = single.exec(oldCleaned)) !== null) {
    const word = oldCleaned.substring(match.index + 1, match.index + match[0].length - 1);
    oldWords.push(word);
  }
  console.log(`OLD METHOD (${oldWords.length} words):`, oldWords.map(w => `"${w}"`).join(', '));
  
  // Method 1: Simple split on whitespace and punctuation
  const simpleWords = text.toLowerCase()
    .replace(/[<>]/g, ' ')
    .replace(/[^\w\s]/g, ' ')
    .split(/\s+/)
    .filter(w => w.length > 0);
  console.log(`SIMPLE SPLIT (${simpleWords.length} words):`, simpleWords.slice(0, 20).join(', '));
  
  return words;
}

async function main() {
  console.log('🔍 Starting word parsing diagnostic test...\n');
  
  // Authorization
  authorize();
  const conn = new jsforce.Connection({
    instanceUrl: process.env.SF_INSTANCE_URL,
    accessToken: process.env.SF_ACCESS_TOKEN
  });
  
  // Get dataset
  console.log(`📊 Looking up dataset: ${options.dataset}`);
  const datasetId = await getDatasetId(conn, options.dataset);
  console.log(`✓ Found dataset ID: ${datasetId}\n`);
  
  // Get test records
  console.log(`📥 Fetching ${TEST_RECORD_COUNT} test records...`);
  let records;
  try {
    records = await getTestRecords(conn, datasetId);
  } catch (err) {
    if (err && err.errorCode === '119' && ID_FIELD === 'Id') {
      console.log('ℹ Id field not found, retrying with Record');
      ID_FIELD = 'Record';
      records = await getTestRecords(conn, datasetId);
    } else {
      throw err;
    }
  }
  
  console.log(`✓ Retrieved ${records.length} records\n`);
  
  // Test parsing on each record
  for (let i = 0; i < records.length; i++) {
    const rec = records[i];
    console.log(`\n${'='.repeat(80)}`);
    console.log(`RECORD ${i + 1}/${records.length}: ${rec[ID_FIELD]}`);
    console.log(`${'='.repeat(80)}`);
    
    for (const field of FIELDS) {
      if (rec[field]) {
        parseTextWithDetails(rec[field], field, rec[ID_FIELD]);
      } else {
        console.log(`\n=== FIELD ${field} (${rec[ID_FIELD]}) ===`);
        console.log('Field is empty or null');
      }
    }
  }
  
  console.log(`\n${'='.repeat(80)}`);
  console.log('DIAGNOSTIC COMPLETE');
  console.log(`${'='.repeat(80)}`);
  console.log('Review the parsing results above to identify any issues with word extraction.');
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});