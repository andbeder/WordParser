#!/usr/bin/env node

const { execSync } = require("child_process");
const fs = require("fs");
const path = require("path");

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
  const keyFile = "./jwt.key";
  const username = process.env.SFDC_USERNAME;
  const loginUrl = process.env.SFDC_LOGIN_URL;
  const instanceUrl = process.env.SF_INSTANCE_URL || loginUrl;
  const tokenPath = path.resolve(process.cwd(), "tmp", "access_token.txt");

  try {
    // -1) Allow token to be provided via environment to support offline usage
    if (process.env.SF_ACCESS_TOKEN) {
      console.log("✔ Using SF_ACCESS_TOKEN from environment");
      const tmpDir = path.dirname(tokenPath);
      fs.mkdirSync(tmpDir, { recursive: true });
      fs.writeFileSync(tokenPath, process.env.SF_ACCESS_TOKEN, "utf8");
      if (!process.env.SF_INSTANCE_URL && loginUrl) {
        process.env.SF_INSTANCE_URL = loginUrl;
      }
      return;
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
        return;
      }
      console.log("ℹ Existing access token rejected; obtaining new token...");
    }

    // 1) Log in via JWT
    execSync(
      `sf org login jwt \
          -i "${clientId}" \
          --jwt-key-file "${keyFile}" \
          --username "${username}" \
          --alias "${alias}" \
          --instance-url "${loginUrl}" \
          --set-default`,
      { stdio: "inherit" }
    );

    // 2) Retrieve the org info as JSON
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

    // 3) Ensure tmp directory exists
    const tmpDir = path.resolve(process.cwd(), "tmp");
    fs.mkdirSync(tmpDir, { recursive: true });

    // 4) Write token to tmp/access_token.txt
    const outPath = path.join(tmpDir, "access_token.txt");
    fs.writeFileSync(outPath, token, "utf8");
    console.log(`✔ Access token written to ${outPath}`);
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
