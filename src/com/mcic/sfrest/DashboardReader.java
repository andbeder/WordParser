package com.mcic.sfrest;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;

import org.json.JSONArray;
import org.json.JSONObject;

import com.mcic.analytics.wavemetadata.DatasetBuilder;

public class DashboardReader extends SalesforceApp {
	private String apiName;
	private String label;
	private String app;


	public static void main(String[] args) {
		SalesforceApp app = new DashboardReader(args);
		app.execute();
	}
	
	public DashboardReader(String[]args) {
		super(args);
		
		apiName = getArgument("-n", "-name") == null ? "Test" : getArgument("-n", "-name")[0];
		label = getArgument("-l", "-label") == null ? "Test" : getArgument("-n", "-name")[0];
		app = getArgument("-a", "-app") == null ? "Test" : getArgument("-n", "-name")[0];
	}

	@Override
	public void execute() {
		SalesforceREST agent = getAgent();
		Collection<WaveDashboard> dashboards = getDashboards().values();
		try {
			DatasetBuilder db = new DatasetBuilder("DashbaordName", "Application", "MasterLabel", "Id", "CreatedBy", "LastModifiedBy");
			for (WaveDashboard d : dashboards) {
				db.addRecord(d.name, d.appName, d.label, d.id, d.createdBy, d.lastModifiedBy);
			}
			agent.writeDataset(apiName, label, app, db);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public Map<String, WaveDashboard> getDashboards() {
		SalesforceREST agent = getAgent();
		Map<String, WaveDashboard> dashboards = new TreeMap<String, WaveDashboard>();
		String nextURL = "/services/data/v58.0/wave/datasets";
		while (nextURL != null && !nextURL.equals("null")) {
			int res = agent.get(nextURL, null);
			if (res == SalesforceREST.FAILURE) {
				System.out.println("Error retreiving dashboard data");
				System.exit(1);
			}
			JSONArray records = new JSONObject(agent.getResponse()).getJSONArray("records");	
            for (int i = 0; i < records.length(); i++) {
                JSONObject node = records.getJSONObject(i);
                WaveDashboard dashboard = new WaveDashboard(scrapeJSON(node, "name", "folder.name", "label", "id", "createdBy.name", "lastModifiedBy.name"));
                dashboards.put(dashboard.name, dashboard);
            }
		}
		return dashboards;
	}
	
    public String[] scrapeJSON(JSONObject node, String...fields){
    	String[] output = new String[fields.length];
    	int i = 0;
    	for (String field : fields) {
    		JSONObject n = node;
    		while (field.indexOf('.') > 0) {
    			String parent = field.substring(0, field.indexOf('.') - 1);
    			String child = field.substring(field.indexOf('.') + 1);
    			n = n.getJSONObject(parent);
    			field = child;
    		}
    		output[i++] = n.getString(field);
    	}
    	return output;
    }
	

	public static class WaveDashboard {
    	public String name;
    	public String appName;
    	public String label;
    	public String id;
    	public String createdBy;
    	public String lastModifiedBy;
		public WaveDashboard(String...args) {
			super();
			this.name = args[0];
			this.appName = args[1];
			this.label = args[2];
			this.id = args[3];
			this.createdBy = args[4];
			this.lastModifiedBy = args[5];
		}
    	
    	
    }

//	dashboards.add("DashboardName", d.name);
//	dashboards.add("Application", d.appName);
//	dashboards.add("MasterLabel", d.label);
//	dashboards.add("Id", d.id);
//	dashboards.add("Type", d.type);
//	dashboards.add("CreatedBy", d.createdBy);
//	dashboards.add("LastModifiedBy", d.lastModifiedBy);
}
