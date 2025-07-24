package com.mcic.analytics.wavemetadata;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.json.JSONArray;
import org.json.JSONObject;

import com.mcic.analytics.wavemetadata.DatasetBuilder;

/**
 * Reads metadata for all Wave datasets and writes out a CSV dataset
 * with columns: DatasetName, Id, MasterLabel, Application.
 */
public class DatasetReader  {
    /**
     * Fetches all datasets via the Analytics REST API.
     * Uses the same pagination pattern as DashboardReader.
     */
    public static Map<String, WaveDataset> getDatasets(SalesforceREST agent) {
        Map<String, WaveDataset> datasets = new TreeMap<>();
        String nextURL = "/services/data/v58.0/wave/datasets"; // List resource for datasets :contentReference[oaicite:0]{index=0}

        while (nextURL != null && !nextURL.equals("null")) {
            int res = agent.get(nextURL, null);
            if (res == SalesforceREST.FAILURE) {
                System.out.println("Error retrieving dataset data");
                System.exit(1);
            }
            // Parse out all the records
            JSONObject resp = agent.getResponse();
            JSONArray records = resp.getJSONArray("datasets");
            for (int i = 0; i < records.length(); i++) {
                JSONObject node = records.getJSONObject(i);
                // Extract [name, label, id, folder.name] in that order
                String[] fields = SalesforceREST.scrapeJSON(node, "name", "label", "id", "folder.label", "currentVersionUrl", "url");
                WaveDataset ds = new WaveDataset(fields);
                datasets.put(ds.name, ds);
            }
            // If there's a nextRecordsUrl, loop; otherwise break
            nextURL = resp.optString("nextPageUrl", null);
            System.out.println(nextURL);
        }
        return datasets;
    }
    
    public static Set<WaveDatasetField> getFields(WaveDataset dataset, SalesforceREST agent){
        Set<WaveDatasetField> fields = new TreeSet<WaveDatasetField>();
    	if (dataset.currentVersionUrl != null) {
        	int res = agent.get(dataset.currentVersionUrl, null);
        	if (res == SalesforceREST.SUCCESS) {
        		WaveDatasetField field = null;
        		JSONObject xmd = agent.getResponse().getJSONObject("xmdMain");
        		JSONArray dates = xmd.getJSONArray("dates");
        		for (int i = 0;i < dates.length();i++) {
        			JSONObject n = dates.getJSONObject(i);
        			field = new WaveDatasetField(dataset, n.getJSONObject("fields").getString("fullField"), n.getString("label"), "Date");
        			fields.add(field);
        		}
        		JSONArray dim = xmd.getJSONArray("dimensions");
        		for (int i = 0;i < dim.length();i++) {
        			JSONObject n = dim.getJSONObject(i);
        			field = new WaveDatasetField(dataset, n.getString("field"), n.getString("label"), "Dimension");
        			fields.add(field);
        		}
        		JSONArray mea = xmd.getJSONArray("measures");
        		for (int i = 0;i < mea.length();i++) {
        			JSONObject n = mea.getJSONObject(i);
        			field = new WaveDatasetField(dataset, n.getString("field"), n.getString("label"), "Measure");
        			fields.add(field);
        		}
        	}
    	}
        
        return fields;
    }

    /** Simple holder for the four dataset metadata fields */
    public static class WaveDataset {
        public String name;
        public String label;
        public String id;
        public String appName;
        public String currentVersionUrl;
        public String url;

        public WaveDataset(String... args) {
            name    = args[0];
            label   = args[1];
            id      = args[2];
            appName = args[3];
            currentVersionUrl = args[4];
            url 	= args[5];
        }
    }
    
    public static class WaveDatasetField {
    	public WaveDataset dataset;
    	public String name;
    	public String label;
    	public String type;
    	
    	public WaveDatasetField(WaveDataset dataset, String... arg) {
    		this.dataset = dataset;
    		name = arg[0];
    		label = arg[1];
    		type = arg[2];
    	}
    }
}


