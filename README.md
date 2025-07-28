# Word Frequency Node Application

This Node.js application replicates the functionality of the original `WordFrequencyApp.java`.
It connects to Salesforce, reads long text fields from case records, parses out words and
phrases, then uploads the results as a CRM Analytics dataset or writes them to CSV.

## Setup

1. Install dependencies:

   ```bash
   npm install
   ```

2. Set Salesforce credentials for JWT authorization using environment variables:

   - `SFDC_CLIENT_ID` – connected app consumer key
   - `SFDC_USERNAME` – Salesforce username
   - `SFDC_LOGIN_URL` – login URL (`https://login.salesforce.com` by default)
   - Place your private key in `jwt.key` or set `SFDC_JWT_KEY` to its path
   - *(optional)* `SF_ACCESS_TOKEN` – reuse an existing access token instead of performing JWT auth

The script invokes `sfdcAuthorizer.js` to obtain an OAuth access token which is
cached in `tmp/access_token.txt`. If `SF_ACCESS_TOKEN` is set and valid, it will
be reused instead of logging in again.

## Usage

```bash
node word_frequency.js -f Field1,Field2 [options]
```

Options:

- `-d, --dataset <dataset>` – CRM Analytics dataset API name
- `--case-id <id>` – filter to a single case id
- `--csv <file>` – write parsed words to CSV instead of uploading

Without the `--csv` option the script attempts to upload a dataset named
`Word_Frequency_File` to CRM Analytics using the REST API.

## Example

```bash
SFDC_CLIENT_ID=abc123 SFDC_USERNAME=myuser \
  node word_frequency.js -f Description__c,Notes__c \
  -d My_Dataset --csv output.csv
```

This command queries the specified fields, parses each record, and stores the
resulting word dataset in `output.csv`.
