package com.mcic;


import com.mcic.analytics.wavemetadata.SalesforceREST;
import com.mcic.analytics.wavemetadata.ProgressPanel.ProgressPanelStep;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.Base64;
import java.util.concurrent.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

public class WordFrequencyReader extends ConfiguredApp {
    private SalesforceREST agent;
    private List<String> fields = new ArrayList<>();
    private String caseId;
    private boolean writeCSV = false;
    private ProgressPanel dialog;
    private static String sourceDatasetId;

    private static final String TARGET_DATASET_NAME  = "Word_Frequency_File";
    private static final String TARGET_DATASET_LABEL = "Word Frequency (File)";
    private static final String SOURCE_DATASET_NAME  = "Report_PSLP_with_Layers";
    private static final int    THREAD_POOL_SIZE     = 5;

    public static void main(String[] args) throws Exception {
    	WordFrequencyReader app = new WordFrequencyReader();
        app.parseArgs(args);
        if (app.fields.isEmpty()) {
            app.printUsage();
            return;
        }
        app.init();
    }

    private void parseArgs(String[] args) {
        for (int i = 0; i < args.length; i++) {
            switch (args[i].toLowerCase()) {
                case "-f":
                case "-fields":
                    if (i + 1 < args.length) {
                        fields = Arrays.stream(args[++i].split(",")).map(String::trim).collect(Collectors.toList());
                    }
                    break;
                case "-id":
                    if (i + 1 < args.length) {
                        caseId = args[++i];
                    }
                    break;
                case "-c":
                case "-csv":
                    writeCSV = true;
                    break;
                default:
                    // ignore unknown
            }
        }
    }

    private void printUsage() {
        System.out.println("Usage: java WordFrequencyApp -fields <field1,field2,...> [-id <CaseId>] [-csv]");
    }

    @Override
    public void init() {
        File propFile = (File) properties.get("sfConfig");
        SalesforceModel model = new SalesforceModel(propFile);
        agent = new SalesforceREST(model);
        dialog = new ProgressPanel(3);

        dialog.newTicker("Starting word-frequency extraction");
        List<Keyword> keywords = queryCrmAnalytics();
        dialog.newTicker("Uploading results");
        writeDatasets(keywords);
        dialog.newTicker("Completed");
    }

    private String getDatasetId() {
        if (sourceDatasetId == null) {
            agent.get("/services/data/v58.0/wave/datasets?q=Layers", null);
            JSONObject resp = new JSONObject(agent.getResponse());
            JSONArray arr = resp.getJSONArray("datasets");
            for (int i = 0; i < arr.length(); i++) {
                JSONObject ds = arr.getJSONObject(i);
                if (SOURCE_DATASET_NAME.equals(ds.getString("name"))) {
                    sourceDatasetId = ds.getString("id") + "/" + ds.getString("currentVersionId");
                    break;
                }
            }
            if (sourceDatasetId == null) {
                throw new IllegalStateException("Source dataset not found");
            }
        }
        return sourceDatasetId;
    }

    private List<Keyword> queryCrmAnalytics() {
        ExecutorService exec = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
        Map<String, Keyword> freq = new ConcurrentHashMap<>();
        List<Callable<Void>> tasks = new ArrayList<>();

        for (int year = 2030; year >= 2000; year--) {
            final int y = year;
            tasks.add(() -> {
                fetchYear(y, freq);
                return null;
            });
        }

        try {
            exec.invokeAll(tasks);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } finally {
            exec.shutdown();
        }

        return new ArrayList<>(freq.values());
    }

    private void fetchYear(int year, Map<String, Keyword> freq) {
        ProgressPanelStep step = dialog.newUncompleted("Year " + year);
        String saql = buildSaql(year);
        JSONObject payload = new JSONObject().put("query", saql);

        JSONObject data = null;
        while (data == null) {
            int status = agent.postJSON("/services/data/v60.0/wave/query", payload);
            if (status == SalesforceREST.SUCCESS) {
                data = agent.getResponse();
            } else {
                System.err.println("year " + year + ": " + agent.getResponse());
            }
        }

        JSONArray records = data.getJSONObject("results").getJSONArray("records");
        for (int i = 0; i < records.length(); i++) {
            JSONObject rec = records.getJSONObject(i);
            String id = rec.getString("Id");
            fields.forEach(f -> {
                if (rec.has(f) && !rec.isNull(f)) {
                    extractTerms(rec.getString(f), f, id, freq);
                }
            });
        }
        step.complete();
    }

    private String buildSaql(int year) {
        StringBuilder sb = new StringBuilder();
        sb.append("q = load \"").append(getDatasetId()).append("\";");
        if (year > 2000) {
            sb.append("q = filter q by 'Date_of_Loss__c_YEAR' == '").append(year).append("';");
        } else {
            sb.append("q = filter q by 'Date_of_Loss__c_YEAR' <= '").append(year).append("';");
        }
        if (caseId != null) {
            sb.append("q = filter q by 'Patient.Patient_Safety_Case__c' == '").append(caseId).append("';");
        }
        sb.append("q = filter q by 'Baby_Flag' == 'No';");
        sb.append("q = group q by Id;");
        sb.append("q = foreach q generate Id, first(Claim_Made_Date) as Claim_Made_Date, ");
        sb.append(fields.stream()
                .map(f -> "first('" + f + "') as '" + f + "'")
                .collect(Collectors.joining(", ")))
          .append(";");
        sb.append("q = order q by Claim_Made_Date desc;");
        return sb.toString();
    }

    private void extractTerms(String text, String field, String caseId,
                              Map<String, Keyword> freq) {
        String clean = text.replaceAll("<[^>]+>", "").replace("\\n", ".").toLowerCase();
        String sentence = " " + clean + " ";
        extractPhrases(sentence, field, caseId, freq, 3);
        extractPhrases(sentence, field, caseId, freq, 2);
        extractPhrases(sentence, field, caseId, freq, 1);
    }

    private void extractPhrases(String sentence, String field, String caseId,
                                Map<String, Keyword> freq, int n) {
        String regex = "[\\W](?:[\\w%:]+\\W){" + n + "}";
        Matcher m = Pattern.compile(regex).matcher(sentence);
        while (m.find()) {
            String phrase = m.group().trim();
            String key = field + "|" + phrase;
            freq.computeIfAbsent(key, k -> new Keyword(phrase, field, "Phrase")).cases.add(caseId);
        }
    }

    private void writeDatasets(List<Keyword> keywords) {
    	  try {
			dialog.newTicker("Compressing data");
			  DatasetBuilder db = new DatasetBuilder("Field", "Type", "Word", "CaseId");
			int total = keywords.stream().mapToInt(k -> k.cases.size()).sum();
			int count = 0;
			ProgressPanelStep step = dialog.newUncompleted("Processing");
			for (Keyword k : keywords) {
			    for (String id : k.cases) {
			        db.addRecord("Field", k.field, k.type,  k.word, id);
			        if (++count % 100000 == 0) {
			            step.setText("Processing " + count + " / " + total);
			        }
			    }
			}
			step.complete();

			    agent.writeDataset(TARGET_DATASET_NAME, TARGET_DATASET_LABEL,
			        "RDI_Development", db);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }

    private String buildCsvMetadata() {
        JSONObject meta = new JSONObject();
        meta.put("fileFormat", new JSONObject()
                .put("charsetName", "UTF-8")
                .put("fieldsDelimitedBy", ",")
                .put("linesTerminatedBy", "\r\n"));
        JSONArray objects = new JSONArray();
        JSONObject obj = new JSONObject()
                .put("connector", "CSV")
                .put("fullyQualifiedName", "word_frequency_csv")
                .put("label", "word frequency")
                .put("name", "word_frequency_csv");
        JSONArray fieldsArr = new JSONArray();
        for (String f : Arrays.asList("Field","Type","Word","CaseId")) {
            fieldsArr.put(new JSONObject()
                    .put("fullyQualifiedName", f)
                    .put("name", f)
                    .put("type", "Text")
                    .put("label", f));
        }
        obj.put("fields", fieldsArr);
        objects.put(obj);
        meta.put("objects", objects);
        return meta.toString();
    }

    private static class Keyword {
        String word, field, type;
        Set<String> cases = new TreeSet<>();

        Keyword(String word, String field, String type) {
            this.word = word;
            this.field = field;
            this.type = type;
        }
    }
}
