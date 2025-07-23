# Word Frequency Node Application

This Node.js application replicates the functionality of the original `WordFrequencyApp.java`.
It connects to Salesforce, reads long text fields from case records, parses out words and
phrases, then uploads the results as a CRM Analytics dataset or writes them to CSV.

## Setup

1. Install dependencies:

   ```bash
   npm install
   ```

2. Set Salesforce credentials as environment variables:

   - `SF_USERNAME` – Salesforce username
   - `SF_PASSWORD` – password concatenated with security token
   - `SF_LOGIN_URL` – optional login URL (`https://login.salesforce.com` by default)

## Usage

```bash
node word_frequency.js -f Field1,Field2 [options]
```

Options:

- `-o, --object <object>` – Salesforce object name (`MCIC_Patient_Safety_Case__c` default)
- `--case-id <id>` – filter to a single case id
- `--csv <file>` – write parsed words to CSV instead of uploading

Without the `--csv` option the script attempts to upload a dataset named
`Word_Frequency_File` to CRM Analytics using the REST API.

## Example

```bash
SF_USERNAME=myuser SF_PASSWORD=mypass node word_frequency.js \
  -f Description__c,Notes__c --csv output.csv
```

This command queries the specified fields, parses each record, and stores the
resulting word dataset in `output.csv`.
