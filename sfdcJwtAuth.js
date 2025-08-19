#!/usr/bin/env node

const { execSync } = require("child_process");
const fs = require("fs");
const path = require("path");
const crypto = require("crypto");

/**
 * Decrypts the encrypted JWT key file using AES-256-CBC with PBKDF2
 */
function decryptJwtKey(encryptedKeyPath, keyPass) {
  try {
    const encryptedData = fs.readFileSync(encryptedKeyPath);
    
    // OpenSSL format: "Salted__" + 8-byte salt + encrypted data
    if (encryptedData.slice(0, 8).toString('ascii') !== 'Salted__') {
      throw new Error('Invalid OpenSSL encrypted file format');
    }
    
    const salt = encryptedData.slice(8, 16);
    const encrypted = encryptedData.slice(16);
    
    // Derive key and IV using OpenSSL's EVP_BytesToKey with PBKDF2
    // OpenSSL uses 10000 iterations by default for PBKDF2
    const keyAndIv = crypto.pbkdf2Sync(keyPass, salt, 10000, 48, 'sha256');
    const key = keyAndIv.slice(0, 32);
    const iv = keyAndIv.slice(32, 48);
    
    // Decrypt
    const decipher = crypto.createDecipheriv('aes-256-cbc', key, iv);
    let decrypted = decipher.update(encrypted, null, 'utf8');
    decrypted += decipher.final('utf8');
    
    return decrypted;
  } catch (err) {
    throw new Error(`Failed to decrypt JWT key: ${err.message}`);
  }
}

/**
 * Performs a JWT-based SFDX login and writes the access token to tmp/access_token.txt
 */
function isTokenAccepted(token, instanceUrl) {
  try {
    const status = execSync(
      `curl -s -o /dev/null -w "%{http_code}" -H "Authorization: Bearer ${token}" "${instanceUrl}/services/data/v60.0"`,
      { encoding: "utf8" }
    ).trim();
    return status === "200";
  } catch (err) {
    return false;
  }
}

function authorize() {
  const alias = "myJwtOrg";
  const clientId = process.env.SFDC_CLIENT_ID;
  const encryptedKeyFile = "./jwt.key.enc";
  const keyPass = process.env.KEY_PASS;
  const username = process.env.SFDC_USERNAME;
  const loginUrl = process.env.SFDC_LOGIN_URL;
  const instanceUrl = process.env.SF_INSTANCE_URL || loginUrl;
  const tokenPath = path.resolve(process.cwd(), "tmp", "access_token.txt");

  // Validate required environment variables
  if (!keyPass) {
    throw new Error("KEY_PASS environment variable is required to decrypt JWT key");
  }
  if (!clientId) {
    throw new Error("SFDC_CLIENT_ID environment variable is required");
  }
  if (!username) {
    throw new Error("SFDC_USERNAME environment variable is required");
  }

  try {
    // -1) Allow token to be provided via environment to support offline usage
    if (process.env.SF_ACCESS_TOKEN) {
      const envToken = process.env.SF_ACCESS_TOKEN;
      if (isTokenAccepted(envToken, instanceUrl)) {
        console.log("✔ Using SF_ACCESS_TOKEN from environment");
        const tmpDir = path.dirname(tokenPath);
        fs.mkdirSync(tmpDir, { recursive: true });
        fs.writeFileSync(tokenPath, envToken, "utf8");
        if (!process.env.SF_INSTANCE_URL && loginUrl) {
          process.env.SF_INSTANCE_URL = loginUrl;
        }
        return {
          accessToken: envToken,
          instanceUrl: process.env.SF_INSTANCE_URL || loginUrl,
        };
      }
      console.log("ℹ Provided SF_ACCESS_TOKEN was rejected; obtaining new token...");
    }

    // 0) Reuse existing token when possible
    if (fs.existsSync(tokenPath)) {
      const existing = fs.readFileSync(tokenPath, "utf8").trim();
      if (existing && isTokenAccepted(existing, instanceUrl)) {
        console.log("✔ Reusing existing access token");
        process.env.SF_ACCESS_TOKEN = existing;
        if (!process.env.SF_INSTANCE_URL) {
          process.env.SF_INSTANCE_URL = instanceUrl;
        }
        return {
          accessToken: existing,
          instanceUrl: process.env.SF_INSTANCE_URL || instanceUrl,
        };
      }
      console.log("ℹ Existing access token rejected; obtaining new token...");
    }

    // 1) Decrypt the JWT key and keep in memory
    const decryptedKey = decryptJwtKey(encryptedKeyFile, keyPass);

    // 2) Create a temporary file with restricted permissions for minimal exposure
    const tempKeyFile = path.resolve(process.cwd(), "tmp", `jwt_${Date.now()}_${Math.random().toString(36).substr(2, 9)}.key`);
    const tmpDir = path.dirname(tempKeyFile);
    fs.mkdirSync(tmpDir, { recursive: true });
    
    try {
      // Write with restricted permissions (owner read-only)
      fs.writeFileSync(tempKeyFile, decryptedKey, { mode: 0o600 });
      
      // 3) Log in via JWT using temporary key file
      execSync(
        `sf org login jwt \
            -i "${clientId}" \
            --jwt-key-file "${tempKeyFile}" \
            --username "${username}" \
            --alias "${alias}" \
            --instance-url "${loginUrl}" \
            --set-default`,
        { stdio: "inherit" }
      );
    } finally {
      // Immediately clean up temporary key file
      if (fs.existsSync(tempKeyFile)) {
        // Overwrite with random data before deletion for security
        const randomData = crypto.randomBytes(decryptedKey.length);
        fs.writeFileSync(tempKeyFile, randomData);
        fs.unlinkSync(tempKeyFile);
      }
    }

    // 3) Retrieve the org info as JSON
    const displayJson = execSync(
      `sf org display --target-org "${alias}" --json`,
      { encoding: "utf8" }
    );
    const info = JSON.parse(displayJson).result || {};
    const token = info.accessToken;
    if (!token)
      throw new Error("No accessToken found in sf org display output.");
    if (info.instanceUrl) {
      process.env.SF_INSTANCE_URL = info.instanceUrl;
    }
    process.env.SF_ACCESS_TOKEN = token;

    // 4) Write token to tmp/access_token.txt
    const outPath = path.join(path.dirname(tokenPath), "access_token.txt");
    fs.writeFileSync(outPath, token, "utf8");
    console.log(`✔ Access token written to ${outPath}`);
    return {
      accessToken: token,
      instanceUrl: process.env.SF_INSTANCE_URL || instanceUrl,
    };
  } catch (err) {
    console.error("❌ Error during JWT login or token write:", err.message);
    process.exit(1);
  }
}

// If this script is run directly, perform the authorization immediately
if (require.main === module) {
  authorize();
}

// Export the authorize function for programmatic use
module.exports = authorize;
