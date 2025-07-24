package com.mcic.analytics.wavemetadata;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;

import org.json.JSONArray;
import org.json.JSONObject;

import com.mcic.analytics.wavemetadata.DatasetBuilder;

public class DashboardReader {
    private String apiName;
    private String label;
    private String app;

    public static Map<String, WaveDashboard> getDashboards(SalesforceREST agent) {
        Map<String, WaveDashboard> dashboards = new TreeMap<>();
        String nextURL = "/services/data/v58.0/wave/dashboards";
        while (nextURL != null && !"null".equals(nextURL)) {
        	System.out.print("Reading dashboards: " + nextURL + "...");
            int res = agent.get(nextURL, null);
        	System.out.println("done");
            if (res == SalesforceREST.FAILURE) {
                System.err.println("Error retrieving dashboard data");
                System.exit(1);
            }
            JSONObject resp = agent.getResponse();
            JSONArray items = resp.getJSONArray("dashboards");
            for (int i = 0; i < items.length(); i++) {
                JSONObject node = items.getJSONObject(i);
                String[] vals = SalesforceREST.scrapeJSON(node,
                    "name", "folder.label", "label", "id", "createdBy.name", "lastModifiedBy.name"
                );
                WaveDashboard wd = new WaveDashboard(vals);
                dashboards.put(wd.name, wd);
            }
            nextURL = resp.optString("nextPageUrl", null);
        }
        return dashboards;
    }

    public static class WaveDashboard {
        public String name, appName, label, id, createdBy, lastModifiedBy;
        public WaveDashboard(String... args) {
            this.name            = args[0];
            this.appName         = args[1];
            this.label           = args[2];
            this.id              = args[3];
            this.createdBy       = args[4];
            this.lastModifiedBy  = args[5];
        }
        
		@Override
		public String toString() {
			return name;
		}
        
        
    }
}
