#!/usr/bin/env node

const http = require('http');
const url = require('url');
const net = require('net');
const crypto = require('crypto');
const readline = require('readline');
const jsforce = require('jsforce');
const axios = require('axios');
const open = require('open');

// In-memory token cache (same pattern as sfdcJwtAuth.js)
let tokenCache = {
  accessToken: null,
  refreshToken: null,
  instanceUrl: null,
  expiry: null
};

/**
 * Generates a cryptographically random code verifier for PKCE
 */
function generateCodeVerifier() {
  // 32 bytes = 43 characters in base64url
  return crypto.randomBytes(32)
    .toString('base64')
    .replace(/\+/g, '-')
    .replace(/\//g, '_')
    .replace(/=/g, '');
}

/**
 * Generates the code challenge from the verifier using SHA256
 */
function generateCodeChallenge(verifier) {
  return crypto.createHash('sha256')
    .update(verifier)
    .digest('base64')
    .replace(/\+/g, '-')
    .replace(/\//g, '_')
    .replace(/=/g, '');
}

/**
 * Prompts user to select Salesforce environment
 */
async function promptEnvironment() {
  const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout
  });

  return new Promise((resolve) => {
    console.log('\nSelect Salesforce environment:');
    console.log('  1) Production (login.salesforce.com)');
    console.log('  2) Sandbox (test.salesforce.com)');
    rl.question('Enter choice [1]: ', (answer) => {
      rl.close();
      const choice = answer.trim() || '1';
      resolve(choice === '2' ? 'sandbox' : 'production');
    });
  });
}

/**
 * Finds an available port starting from the given port
 */
async function findAvailablePort(startPort = 8888, maxAttempts = 10) {
  for (let port = startPort; port < startPort + maxAttempts; port++) {
    const available = await new Promise((resolve) => {
      const server = net.createServer();
      server.once('error', () => resolve(false));
      server.once('listening', () => {
        server.close();
        resolve(true);
      });
      server.listen(port, 'localhost');
    });

    if (available) return port;
  }

  throw new Error('No available ports found for OAuth callback (tried 8888-8897)');
}

/**
 * Creates a local HTTP server to receive the OAuth callback
 */
function createCallbackServer(port, timeout = 120000) {
  return new Promise((resolve, reject) => {
    const server = http.createServer((req, res) => {
      const parsedUrl = url.parse(req.url, true);

      if (parsedUrl.pathname === '/callback') {
        const code = parsedUrl.query.code;
        const error = parsedUrl.query.error;
        const errorDesc = parsedUrl.query.error_description;

        if (error) {
          res.writeHead(400, { 'Content-Type': 'text/html' });
          res.end(`<html><body><h1>Authorization Failed</h1><p>${errorDesc || error}</p><p>You can close this window.</p></body></html>`);
          server.close();
          reject(new Error(`OAuth error: ${errorDesc || error}`));
          return;
        }

        if (code) {
          res.writeHead(200, { 'Content-Type': 'text/html' });
          res.end('<html><body><h1>Authorization Successful!</h1><p>You can close this window and return to the CLI.</p></body></html>');
          server.close();
          resolve(code);
        }
      } else {
        res.writeHead(404, { 'Content-Type': 'text/plain' });
        res.end('Not Found');
      }
    });

    // Timeout handling
    const timer = setTimeout(() => {
      server.close();
      reject(new Error(`OAuth timeout - no callback received within ${timeout / 1000} seconds`));
    }, timeout);

    server.on('close', () => clearTimeout(timer));
    server.on('error', (err) => {
      clearTimeout(timer);
      reject(new Error(`Failed to start callback server: ${err.message}`));
    });

    server.listen(port, 'localhost', () => {
      console.log(`OAuth callback server listening on http://localhost:${port}/callback`);
    });
  });
}

/**
 * Validates if a token is accepted by Salesforce
 */
async function isTokenValid(token, instanceUrl) {
  if (!token || !instanceUrl) return false;

  try {
    const conn = new jsforce.Connection({
      instanceUrl,
      accessToken: token
    });
    await conn.identity();
    return true;
  } catch (err) {
    return false;
  }
}

/**
 * Exchanges authorization code for tokens using PKCE
 */
async function exchangeCodeForTokens(loginUrl, clientId, redirectUri, code, codeVerifier) {
  const tokenUrl = `${loginUrl}/services/oauth2/token`;

  const params = new URLSearchParams({
    grant_type: 'authorization_code',
    client_id: clientId,
    redirect_uri: redirectUri,
    code: code,
    code_verifier: codeVerifier
  });

  const response = await axios.post(tokenUrl, params.toString(), {
    headers: {
      'Content-Type': 'application/x-www-form-urlencoded'
    }
  });

  return response.data;
}

/**
 * Performs OAuth Web Server Flow authentication with PKCE
 */
async function authorize(options = {}) {
  const clientId = process.env.SFDC_OAUTH_CLIENT_ID || process.env.SFDC_CLIENT_ID;

  if (!clientId) {
    throw new Error('SFDC_CLIENT_ID or SFDC_OAUTH_CLIENT_ID environment variable is required for OAuth');
  }

  // Check for existing valid token in environment
  if (process.env.SF_ACCESS_TOKEN && process.env.SF_INSTANCE_URL) {
    const valid = await isTokenValid(process.env.SF_ACCESS_TOKEN, process.env.SF_INSTANCE_URL);
    if (valid) {
      console.log('Using existing SF_ACCESS_TOKEN from environment');
      tokenCache = {
        accessToken: process.env.SF_ACCESS_TOKEN,
        instanceUrl: process.env.SF_INSTANCE_URL,
        expiry: Date.now() + (2 * 60 * 60 * 1000)
      };
      return {
        accessToken: tokenCache.accessToken,
        instanceUrl: tokenCache.instanceUrl
      };
    }
    console.log('Existing SF_ACCESS_TOKEN invalid, starting OAuth flow...');
  }

  // Check for valid cached token
  if (tokenCache.accessToken && tokenCache.expiry > Date.now()) {
    const valid = await isTokenValid(tokenCache.accessToken, tokenCache.instanceUrl);
    if (valid) {
      console.log('Using cached access token');
      process.env.SF_ACCESS_TOKEN = tokenCache.accessToken;
      process.env.SF_INSTANCE_URL = tokenCache.instanceUrl;
      return {
        accessToken: tokenCache.accessToken,
        instanceUrl: tokenCache.instanceUrl
      };
    }
    console.log('Cached token invalid, starting OAuth flow...');
  }

  // Determine environment
  const environment = options.environment || process.env.SFDC_ENVIRONMENT || await promptEnvironment();
  const loginUrl = environment === 'sandbox'
    ? 'https://test.salesforce.com'
    : 'https://login.salesforce.com';

  console.log(`Using ${environment} environment: ${loginUrl}`);

  // Find available port
  const port = await findAvailablePort();
  const redirectUri = `http://localhost:${port}/callback`;

  // Generate PKCE code verifier and challenge
  const codeVerifier = generateCodeVerifier();
  const codeChallenge = generateCodeChallenge(codeVerifier);

  // Build authorization URL with PKCE
  const authParams = new URLSearchParams({
    response_type: 'code',
    client_id: clientId,
    redirect_uri: redirectUri,
    scope: 'api refresh_token',
    code_challenge: codeChallenge,
    code_challenge_method: 'S256'
  });

  const authUrl = `${loginUrl}/services/oauth2/authorize?${authParams.toString()}`;

  // Start callback server
  const codePromise = createCallbackServer(port);

  // Open browser
  console.log('Opening browser for Salesforce login...');
  try {
    await open(authUrl);
  } catch (err) {
    console.log('\nCould not open browser automatically.');
    console.log('Please open this URL manually:');
    console.log(`\n  ${authUrl}\n`);
  }

  // Wait for authorization code
  console.log('Waiting for authorization...');
  const code = await codePromise;
  console.log('Authorization code received, exchanging for tokens...');

  // Exchange code for tokens with PKCE verifier
  const tokenResponse = await exchangeCodeForTokens(loginUrl, clientId, redirectUri, code, codeVerifier);

  // Cache tokens
  tokenCache = {
    accessToken: tokenResponse.access_token,
    refreshToken: tokenResponse.refresh_token,
    instanceUrl: tokenResponse.instance_url,
    expiry: Date.now() + (2 * 60 * 60 * 1000) // 2 hours
  };

  // Set environment variables
  process.env.SF_ACCESS_TOKEN = tokenCache.accessToken;
  process.env.SF_INSTANCE_URL = tokenCache.instanceUrl;

  console.log(`Access token obtained for ${tokenCache.instanceUrl}`);

  return {
    accessToken: tokenCache.accessToken,
    instanceUrl: tokenCache.instanceUrl
  };
}

// If this script is run directly, perform the authorization immediately
if (require.main === module) {
  authorize()
    .then((result) => {
      console.log('OAuth authentication successful');
      console.log(`Instance URL: ${result.instanceUrl}`);
    })
    .catch((err) => {
      console.error('OAuth authentication failed:', err.message);
      process.exit(1);
    });
}

module.exports = { authorize };
