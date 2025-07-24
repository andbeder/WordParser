package com.mcic.analytics.wavemetadata;

import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;

import com.mcic.analytics.wavemetadata.DashboardReader.WaveDashboard;

import org.json.JSONException;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DashboardFieldReader {

    public static void main(String[] args) throws Exception {
        try (BufferedReader reader = new BufferedReader(new FileReader("Dashboard.json"))) {
            JSONTokener tokener = new JSONTokener(reader);
            JSONObject root = new JSONObject(tokener);

            Set<DashboardField> fields = parseDashboard("MyDash", root);
            System.out.println("Total fields found: " + fields.size());
            fields.forEach(f -> System.out.println(f));
        }
    }
    
    public static Set<DashboardField> getFields(WaveDashboard dashboard, SalesforceREST agent){
        String nextURL = "/services/data/v58.0/wave/dashboards/" + dashboard.id;
        int res = agent.get(nextURL, null);
        if (res == SalesforceREST.FAILURE) {
            System.err.println("Error retrieving dashboard data");
            System.exit(1);
        }
        return parseDashboard(dashboard.name, agent.getResponse());
    }
    

    public static class DashboardField {
        public final String step, dataset, field, dashboard;
        public DashboardField(String step, String ds, String fld, String dash) {
            this.step = step; this.dataset = ds; this.field = fld; this.dashboard = dash;
        }
        @Override public int hashCode() {
            return Objects.hash(dashboard, step, dataset, field);
        }
        @Override public boolean equals(Object o) {
            if (!(o instanceof DashboardField)) return false;
            DashboardField that = (DashboardField)o;
            return dashboard.equals(that.dashboard)
                && step.equals(that.step)
                && dataset.equals(that.dataset)
                && field.equals(that.field);
        }
        @Override public String toString() {
            return dashboard + " → " + step + " → " + dataset + " → " + field;
        }
    }

    // Patterns for SAQL load & fields
    private static final Pattern LOAD  = Pattern.compile("load\\s+\"(.*?)\"", Pattern.CASE_INSENSITIVE);
    private static final Pattern FIELD = Pattern.compile("'([^']+)'",         Pattern.CASE_INSENSITIVE);

    public static Set<DashboardField> parseDashboard(String dashName, JSONObject resp) {
        Set<DashboardField> fields = new LinkedHashSet<>();
        JSONObject state = resp.getJSONObject("state");

        // --- Global filters ---
        JSONArray gfs = state.optJSONArray("filters");
        if (gfs != null) {
            for (int i = 0; i < gfs.length(); i++) {
                JSONObject gf = gfs.getJSONObject(i);
                String ds = gf.getJSONObject("dataset").getString("name");
                scanArray(dashName, "Global", Collections.singletonList(ds), gf.optJSONArray("fields"), fields);
            }
        }

        // --- dataSourceLinksInfo.links ---
        JSONObject dsl = state.optJSONObject("dataSourceLinksInfo");
        if (dsl != null) {
            JSONArray links = dsl.optJSONArray("links");
            if (links != null) {
                for (int i = 0; i < links.length(); i++) {
                    JSONObject link = links.getJSONObject(i);
                    JSONArray flds = link.optJSONArray("fields");
                    if (flds != null) {
                        for (int j = 0; j < flds.length(); j++) {
                            JSONObject fld = flds.getJSONObject(j);
                            addField(dashName, "Global",
                                     fld.getString("dataSourceName"),
                                     fld.getString("fieldName"),
                                     fields);
                        }
                    }
                }
            }
        }

        // --- Steps ---
        JSONObject steps = state.optJSONObject("steps");
        if (steps != null) {
            for (String stepKey : steps.keySet()) {
                recurse(dashName, stepKey, steps.getJSONObject(stepKey), fields);
            }
        }

        return Collections.unmodifiableSet(fields);
    }

    private static void recurse(String dash, String step,
                                JSONObject obj,
                                Set<DashboardField> fields) {
        for (String key : obj.keySet()) {
            Object val = obj.get(key);

            // 1) Compact/simple query object
            if ("query".equalsIgnoreCase(key) && val instanceof JSONObject) {
                JSONObject q = (JSONObject)val;
                List<String> parents = grabDatasets(obj);
                JSONArray inject = new JSONArray();
                parents.forEach(ds -> inject.put(new JSONObject().put("name", ds)));

                // A) If stringified JSON inside "query"
                if (q.has("query") && q.optJSONArray("measures") == null) {
                    String raw = q.getString("query");
                    String unesc = unescapeHtml(raw);
                    try {
                        JSONObject inner = new JSONObject(new JSONTokener(unesc));
                        inner.put("datasets", inject);
                        extractSimple(dash, step, obj, inner, fields);
                    } catch (JSONException ex) {
                        System.err.println("🔥 Failed parsing inner JSON at step “" + step +
                            "”, field “query”.\nSnippet around char 56:\n" +
                            snippet(unesc, 56, 30) + "\nFull raw (truncated 500 chars):\n" +
                            (unesc.length()>500 ? unesc.substring(0,500)+"…" : unesc));
                        // skip this step’s query
                    }
                }
                // B) Already proper JSON
                else {
                    q.put("datasets", inject);
                    extractSimple(dash, step, obj, q, fields);
                }
            }
            // 2) SAQL or string-query
            else if (("saql".equalsIgnoreCase(key)
                   || ("query".equalsIgnoreCase(key) && val instanceof String))) {
                extractSaql(dash, step, (String)val, fields);
            }
            // 3) Recurse deeper
            else if (val instanceof JSONObject) {
                recurse(dash, step, (JSONObject)val, fields);
            }
            else if (val instanceof JSONArray) {
                JSONArray arr = (JSONArray)val;
                for (int i = 0; i < arr.length(); i++) {
                    Object e = arr.get(i);
                    if (e instanceof JSONObject) {
                        recurse(dash, step, (JSONObject)e, fields);
                    } else if (e instanceof String
                               && ("saql".equalsIgnoreCase(key)
                                || "query".equalsIgnoreCase(key))) {
                        extractSaql(dash, step, (String)e, fields);
                    }
                }
            }
        }
    }

    private static List<String> grabDatasets(JSONObject step) {
        List<String> ds = new ArrayList<>();
        JSONArray arr = step.optJSONArray("datasets");
        if (arr != null) {
            for (int i = 0; i < arr.length(); i++) {
                JSONObject d = arr.getJSONObject(i);
                ds.add(d.optString("name", d.optString("id")));
            }
        }
        return ds;
    }

    private static void extractSimple(String dash, String step,
                                      JSONObject stepObj,
                                      JSONObject q,
                                      Set<DashboardField> flds) {
        List<String> dsList = grabDatasets(q);
        // fallback to step-level
        if (dsList.isEmpty()) dsList = grabDatasets(stepObj);

        // pigql inside simple → SAQL
        if (q.has("pigql")) {
            extractSaql(dash, step, q.getString("pigql"), flds);
        }

        scanArray(dash, step, dsList, q.optJSONArray("measures"), flds);
        scanArray(dash, step, dsList, q.optJSONArray("groups"),   flds);
        scanArray(dash, step, dsList, q.optJSONArray("values"),   flds);
        scanArray(dash, step, dsList, q.optJSONArray("filters"),  flds);
        scanArray(dash, step, dsList, stepObj.optJSONArray("filters"), flds);
    }

    private static void extractSaql(String dash, String step,
                                    String saql,
                                    Set<DashboardField> flds) {
        Matcher lm = LOAD.matcher(saql);
        List<String> ds = new ArrayList<>();
        while (lm.find()) ds.add(lm.group(1));

        if (!ds.isEmpty()) {
            Matcher fm = FIELD.matcher(saql);
            while (fm.find()) {
                String field = fm.group(1);
                ds.forEach(dsrc -> addField(dash, step, dsrc, field, flds));
            }
        }
    }

    private static void scanArray(String dash, String step,
                                  List<String> ds,
                                  JSONArray arr,
                                  Set<DashboardField> flds) {
        if (arr == null) return;
        for (int i = 0; i < arr.length(); i++) {
            Object o = arr.get(i);
            if (o instanceof JSONArray) {
                scanArray(dash, step, ds, (JSONArray)o, flds);
            } else if (o instanceof String) {
                ds.forEach(dsrc -> addField(dash, step, dsrc, (String)o, flds));
            }
        }
    }

    private static void addField(String dash,
                                 String step,
                                 String ds,
                                 String fld,
                                 Set<DashboardField> flds) {
        DashboardField df = new DashboardField(step, ds, fld, dash);
        if (flds.add(df)) {
            //System.out.println(" + " + df);
        }
    }

    // enhanced HTML unescape, including numeric entities
    private static String unescapeHtml(String s) {
        return s.replace("&quot;", "\"")
                .replace("&#34;", "\"")
                .replace("&apos;", "'")
                .replace("&#39;", "'")
                .replace("&lt;", "<")
                .replace("&gt;", ">")
                .replace("&amp;", "&")
                .replace("&#92;", "\\");
    }

    // grab around the error index
    private static String snippet(String s, int idx, int radius) {
        int start = Math.max(0, idx - radius);
        int end   = Math.min(s.length(), idx + radius);
        return s.substring(start, end).replaceAll("\\s+", " ");
    }
}
