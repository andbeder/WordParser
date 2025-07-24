package com.mcic.sfrest;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.SocketException;
import java.util.Base64;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Vector;
import java.util.stream.Collectors;
import java.util.zip.GZIPOutputStream;

import javax.swing.JOptionPane;

import org.apache.hc.client5.http.classic.methods.HttpDelete;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.classic.methods.HttpPatch;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.classic.methods.HttpUriRequestBase;
import org.apache.hc.client5.http.entity.mime.MultipartEntityBuilder;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.entity.StringEntity;

import com.mcic.util.CSVAuthor;
import com.mcic.util.Progressive;
import com.mcic.util.RecordSet;
import com.mcic.util.RecordsetOld;
import com.mcic.util.json.JSONNode;
import com.mcic.util.json.JSONObject;
import com.mcic.util.json.JSONString;
import com.mcic.util.json.ThreadCluster;
import com.mcic.wavemetadata.ui.ProgressPanel;
import com.mcic.wavemetadata.ui.ProgressPanel.ProgressPanelStep;


public class SalesforceREST extends Progressive {
	public static final int FAILURE = 1;
	public static final int SUCCESS = 2;
	public static final int INDETERMINATE = 0;
	
    String accessToken;
    SalesforceModel model;
    Map<String, Set<String>> sObjectFields;
    //public String restEndpoint;
    private CloseableHttpClient httpClient;
    JSONNode response;
    
    /*
    public static void main(String[] args) {
    	SalesforceModel m = new SalesforceModel(false);
    	SalesforceAgent a = new SalesforceAgent(m);
    	a.getAccessToken();
    }
    */
    public SalesforceREST(SalesforceModel m) {
    	accessToken = null;
    	this.model = m;
    	sObjectFields = new TreeMap<String, Set<String>>();
    }
    
    public String getAccessToken() {
    	if (accessToken == null) {
    		authenticate(model);
    	}
    	
		return accessToken;
	}

    public String getRestEndpoint() {
		return model.getEndpoint();
	}
    
    public void startJob(String id) {
    	String url = "/services/data/v58.0/wave/dataflowjobs";
    	JSONObject root = new JSONObject();
    	root.put("dataflowId", new JSONString(id));
    	root.put("command", new JSONString("start"));
    	int res = postJSON(url, root);
    	System.out.println(response);
    }
    
    
    public void writeDataset(String APIName, String label, String app, RecordSet data, String operation, String metadata) {
    	
    	String url = "/services/data/v58.0/sobjects/InsightsExternalData";
    	JSONObject root = new JSONObject();
    	root.addString("Format", "Csv");
    	root.addString("EdgemartAlias", APIName);
    	root.addString("EdgemartLabel", label);
    	root.addString("EdgemartContainer", app);
    	root.addString("Action", "None");
    	if (operation != null) {
        	root.addString("Operation", operation);
    	} else {
        	root.addString("Operation", "Overwrite");
    	}
    	if (metadata != null) {
        	metadata = Base64.getEncoder().encodeToString(metadata.getBytes());
        	root.addString("MetadataJson", metadata);
    	}    	
    	int res = postJSON(url, root);
    	if (res == SalesforceREST.SUCCESS) {
	    	String id = response.get("id").asString();
	    	//System.out.println(id);
	
	    	final String partUrl = "/services/data/v58.0/sobjects/InsightsExternalDataPart";
			String dataStr = data.toBase64();
	    	if (dataStr.length() >= 5000000) {
	            //FileInputStream fis = new FileInputStream(file);
	            //FileOutputStream fos = new FileOutputStream(gzipFile);
	    		ByteArrayOutputStream out = new ByteArrayOutputStream();
	    		
				int part = 1;
				Vector<JSONNode> packets = new Vector<JSONNode>();
				while (dataStr.length() > 0) {
					//System.out.println("Writing block number " + part);
					int nextBlockSize = dataStr.length() > 5000000 ? 5000000 : dataStr.length();
					String nextBlock = dataStr.substring(0, nextBlockSize);
					dataStr = dataStr.substring(nextBlockSize);
					JSONObject packet = new JSONObject();
		         	packet.addString("InsightsExternalDataId", id);
		         	packet.addNumber("PartNumber", part ++);
		         	packet.addString("DataFile", nextBlock);
		         	packets.add(packet);
				}
				
				/***************************************************************************************************
				 *   Kick off a set of threads uploading individual packet data
				 */
				
				ThreadCluster cluster = new ThreadCluster(5);
		        
				for (int i = 0;i < packets.size();i++) {
					final int thisPart = i + 1;
					final JSONNode thisPacket = packets.elementAt(i);
					cluster.dispatch(new Runnable() {
						
						
						public void run() {
							ProgressPanelStep step = nextStep("Writing block number " + thisPart);
				        	int res = FAILURE;
				        	while (res == FAILURE) {
					        	res = postJSON(partUrl, thisPacket);
					        	if (res != SUCCESS) {
					        		step.addNote("Failure sending, re-posting");
					        	}
				        	}
				        	step.complete();
						}
						
						
					});
					// End of Runnable
		        	
				}
				cluster.join();
	    	} else {
	        	root.clear();
	        	root.addString("InsightsExternalDataId", id);
	        	root.addNumber("PartNumber", 1);
	        	root.addString("DataFile", dataStr);
	        	res = postJSON(url, root);
	    		ProgressPanelStep step = nextStep("Processing Upload");
	        	url = "/services/data/v58.0/sobjects/InsightsExternalData/" + id;
	        	root.clear();
	        	root.addString("Action", "Process");
	        	res = patchJSON(url, root);
	        	step.complete();
	    	}
    	} else {
    		System.out.println("Error creating InsightesExternalData object");
    	}
    	
    }
    

    public Set<String> getFields(String objectName){
    	Set<String> fields = sObjectFields.get(objectName);
    	if (fields == null) {
    		fields = new TreeSet<String>();
    		sObjectFields.put(objectName, fields);
	    	String url = "/services/data/v58.0/sobjects/" + objectName + "/describe/";
	    	int res = get(url, null);
	    	for (JSONNode field : response.get("fields").values()) {
	    		String fieldName = field.get("name").asString();
	    		fields.add(fieldName);
	    	}
    	}
    	return fields;
    }

	public int get(String resource, String urlParameters) {
    	String url = getRestEndpoint() + resource;
    	HttpGet get = new HttpGet(url);
		if (urlParameters != null && !urlParameters.equals("")){
			url = url + "?" + urlParameters;
		}
		get.setHeader("accept", "application/json");
		get.setHeader("Authorization", "OAuth " + getAccessToken());
		
    	try {
			CloseableHttpResponse resp = httpClient.execute(get);
			String rstr = EntityUtils.toString(resp.getEntity());
			if (rstr.matches("^[{\\[][\\s\\S]+")) {  //  Is this JSON?
				response = JSONNode.parse(rstr);
				return SUCCESS;
    		}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	return FAILURE;
    }
	
	public int postJSON(String resource, JSONNode json) {
		return postJSON(resource, json, false);
	}
	
	public int patchJSON(String resource, JSONNode json) {
		return postJSON(resource, json, true);
	}
	
	public JSONNode delete(String resource) {
		String url = getRestEndpoint() + resource;
		HttpUriRequestBase post = new HttpDelete(url);
    	post.setHeader("Content-Type", "application/json");
    	post.setHeader("accept", "application/json");
    	post.setHeader("Authorization", "OAuth " + getAccessToken());
        try {
			CloseableHttpResponse authResponse = httpClient.execute(post);
			int code = authResponse.getCode();
			if (code != 204) {
				String jstr = EntityUtils.toString(authResponse.getEntity());
				JSONNode node = JSONNode.parse(jstr);
				return node;
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
     			e.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        return null;
	}

	public int postJSON(String resource, JSONNode json, boolean isPatch) {
		
		String url = getRestEndpoint() + resource;
		HttpUriRequestBase post = null;
    	if (isPatch) {
    		post = new HttpPatch(url);
    	} else {
    		post = new HttpPost(url);
    	}
    	
    	String jsonString = json.toString();
//    	if (json.getClass().getSimpleName().equals("JSONArray")) {
//    		jsonString = "{" + jsonString + "}";
//    	}
    	
    	post.setHeader("Content-Type", "application/json");
    	post.setHeader("accept", "application/json");
    	post.setHeader("Authorization", "OAuth " + getAccessToken());
    	HttpEntity str = new StringEntity(jsonString, ContentType.APPLICATION_JSON);
    	post.setEntity(str);
    	
        try {
        	boolean succeeded = false;
        	while (!succeeded) {
            	CloseableHttpResponse authResponse;
				try {
					authResponse = httpClient.execute(post);
	    			int code = authResponse.getCode();
	    			if (code != 204) {
	    				String jstr = EntityUtils.toString(authResponse.getEntity());
	    				response = JSONNode.parse(jstr);
	    				return SUCCESS;
	    			}
				} catch (IOException e) {
					return FAILURE;
				}
        	}
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        return FAILURE;
	}
	
	public JSONNode getResponse() {
		return response;
	}

	private void authenticate(SalesforceModel m) {
        // Replace these variables with your Salesforce credentials and token
        String username = m.getUserName();
        String password = m.getPassword();
        String securityToken = m.getSecurityKey();
        String authEndpoint = m.getEndpoint() + "/services/oauth2/token"; // Salesforce authentication URL

        // Salesforce REST API endpoint

        // Salesforce REST API resource you want to access
        String resource = "query?q=SELECT+Id,Name+FROM+Account+LIMIT+5"; // Example query

        // Create HttpClient instance
        try  {
            // Construct authentication request
        	httpClient = HttpClients.createDefault();
        	
        	MultipartEntityBuilder builder = MultipartEntityBuilder.create();
            builder.addTextBody("grant_type", "password");
            builder.addTextBody("client_id", m.getConsumerKey());
            builder.addTextBody("client_secret", m.getConsumerSecret());
            builder.addTextBody("username", username);
            builder.addTextBody("password", password);
            HttpEntity multipart = builder.build();
            
            String authBody = "grant_type=password&client_id=" + m.getConsumerKey() + "&client_secret=" + m.getConsumerSecret() + "&" +
                    "username=" + username + "&password=" + password + securityToken;

            // Create HTTP POST request to authenticate
            HttpPost authRequest = new HttpPost(authEndpoint);
            authRequest.setEntity(multipart);
            //authRequest.setEntity(new StringEntity(authBody));

            // Execute authentication request
            CloseableHttpResponse authResponse = httpClient.execute(authRequest);
            int authStatusCode = authResponse.getCode();

            if (authStatusCode == HttpStatus.SC_OK) {

                try {
                    // Parse access token from authentication response
                	JSONNode authResult = JSONNode.parse(EntityUtils.toString(authResponse.getEntity()));
                    accessToken = authResult.get("access_token").asString();
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    authResponse.close();
                }

            } else {
                System.err.println("Authentication failed: " + authStatusCode + " - " + authResponse.getReasonPhrase());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}