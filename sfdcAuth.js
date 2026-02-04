#!/usr/bin/env node

/**
 * Unified Salesforce authentication facade
 * Supports both JWT and OAuth authentication methods
 */

const jwtAuth = require('./sfdcJwtAuth');
const oauthAuth = require('./sfdcOAuthAuth');

/**
 * Determines if JWT credentials are available
 */
function hasJwtCredentials() {
  return !!(
    process.env.KEY_PASS &&
    process.env.SFDC_CLIENT_ID &&
    process.env.SFDC_USERNAME
  );
}

/**
 * Authorizes with Salesforce using the appropriate method
 *
 * @param {Object} options - Authorization options
 * @param {string} options.authMethod - Force 'jwt' or 'oauth' (default: auto-detect)
 * @param {string} options.environment - 'production' or 'sandbox' (for OAuth)
 * @returns {Promise<{accessToken: string, instanceUrl: string}>}
 */
async function authorize(options = {}) {
  const authMethod = options.authMethod || process.env.SFDC_AUTH_METHOD || 'auto';

  // Force OAuth
  if (authMethod === 'oauth') {
    console.log('Using OAuth Web Server Flow...');
    return await oauthAuth.authorize(options);
  }

  // Force JWT
  if (authMethod === 'jwt') {
    if (!hasJwtCredentials()) {
      throw new Error('JWT authentication requires KEY_PASS, SFDC_CLIENT_ID, and SFDC_USERNAME environment variables');
    }
    console.log('Using JWT authentication...');
    return jwtAuth();
  }

  // Auto-detect: prefer JWT if credentials available, otherwise OAuth
  if (hasJwtCredentials()) {
    console.log('JWT credentials found, using JWT authentication...');
    return jwtAuth();
  }

  // Fall back to OAuth
  console.log('No JWT credentials found, using OAuth Web Server Flow...');
  return await oauthAuth.authorize(options);
}

// If this script is run directly, perform the authorization immediately
if (require.main === module) {
  authorize()
    .then((result) => {
      console.log('Authentication successful');
      console.log(`Instance URL: ${result.instanceUrl}`);
    })
    .catch((err) => {
      console.error('Authentication failed:', err.message);
      process.exit(1);
    });
}

module.exports = authorize;
