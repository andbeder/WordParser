package com.mcic.util.json;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collection;
import java.util.Comparator;
import java.util.Set;
import java.util.Vector;
import java.util.Map.Entry;



public abstract class JSONNode implements Comparator<JSONNode>, Comparable<JSONNode> {
	public enum Type {NULL, NUMBER, OBJECT, STRING, ARRAY, BOOLEAN};
	
	@Override
	public int compareTo(JSONNode o) {
		String n1 = get("name") != null ? get("name").asString() : toString();
		String n2 = o.get("name") != null ? o.get("name").asString() : o.toString();
		return n1.compareTo(n2);
	}

	public static void main(String[] args) {
		try {
			JSONNode n = JSONNode.parseFile(new File("C:\\Users\\abeder\\Downloads\\Compass\\retrieveUnpackaged\\wave\\Analytics_Inventory.wdash"));
			System.out.println(n.toString());
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	public JSONNode clone() {
		return parse(toString());
	}
	
	public boolean isCollection() {
		return false;
	}
	public Type getType() {
		throw new JSONException("Invalid JSON data type.");
	}
	public int size() {
		throw new JSONException("Invalid JSON data type.");
	}
	public Vector<JSONNode> getCollection() {
		throw new JSONException("Invalid JSON data type.");
	}
	public void add(JSONNode n) {
		throw new JSONException("Invalid JSON data type.");
	}
	public String asString() {
		return toString();
	}
	public void clear() {
		throw new JSONException("Invalid JSON data type.");
	}
	public void addAll(Vector<JSONNode> nodes) {
		throw new JSONException("Invalid JSON data type.");
	}
	public void addAll(JSONArray node) {
		throw new JSONException("Invalid JSON data type.");
	}
	public Set<Entry<String, JSONNode>> entrySet() {
		throw new JSONException("Invalid JSON data type.");
	}
	public double asDouble(){
		throw new JSONException("Invalid JSON data type.");
	}
	public int asInt(){
		throw new JSONException("Invalid JSON data type.");
	}
	public boolean asBoolean(){
		throw new JSONException("Invalid JSON data type.");
	}
	public Collection<JSONNode> values(){
		throw new JSONException("Invalid JSON data type.");
	}
	public Collection<String> keySet(){
		throw new JSONException("Invalid JSON data type.");
	}
	public JSONNode find(String key, String value){
		throw new JSONException("Invalid JSON data type.");
	}
	public JSONNode get(String key){
		throw new JSONException("Invalid JSON data type.");
	}
	public void put(String key, JSONNode node){
		throw new JSONException("Invalid JSON data type.");
	}
	public JSONNode elementAt(int i){
		throw new JSONException("Invalid JSON data type.");
	}
	public Collection<JSONObject> valuesAsObjects(){
		throw new JSONException("Invalid JSON data type.");
	}
	public void setString(String s){
		throw new JSONException("Invalid JSON data type.");
	}
	public void setInt(int i){
		throw new JSONException("Invalid JSON data type.");
	}
	public boolean contains(String s){
		throw new JSONException("Invalid JSON data type.");
	}
	public void setElementAt(int i, JSONNode node) {
		throw new JSONException("Invalid JSON data type.");
	}
	public void setObject(String key, JSONNode node) {
		throw new JSONException("Invalid JSON data type.");
	}

	@Override
	public int compare(JSONNode o1, JSONNode o2) {
		String n1 = o1.get("name") != null ? o1.get("name").asString() : o1.toString();
		String n2 = o2.get("name") != null ? o2.get("name").asString() : o2.toString();
		return n1.compareTo(n2);
	}

	public String toString() {
		return toString(0);
	}

	public abstract String toString(int l);

	public String toCompressedString() {
		return toString(0);
	};
	

	public int endC;
	
	static String DIGITS = "0123456789+-.";
	static String WHITE = "\t\n\r\f ";
	static int STATE_COUNT = 17;
	static int STATE_WIDTH = 256;
	
	public static JSONNode parse(String json){
		json = removeExtraWhiteSpaces(json);
		if (json.equals("")){
			return null;
		}
		JSONNode g = StateMachine(json, 0, "root");
		return g;
	}
	
	
	public static int[][] states = null;
	public static void InitStateMachine(){
		
		if (states == null){
			states = new int[STATE_COUNT][];
			for (int i = 0;i < states.length;i++){
				states[i] = new int[STATE_WIDTH];
				for (int j = 0;j < STATE_WIDTH;j++){
					states[i][j] = -3;  //  Illegal JSON
				}
			}

			setAll(0, WHITE, 0);
			states[0][' '] = 0;  // Group
			states[0]['{'] = 1;  // Group
			states[0]['['] = 8;   // Array
			states[0][']'] = -1;   // Array
			states[0]['"'] = 2;   // String
			setAll(0, DIGITS, 10);  // Digit
			setAll(0, "TFNtfn", 14);  // true / false / null

			//  Group Steps
			setAll(1, WHITE, 1);
			states[1]['"'] = 13;
			states[1]['}'] = -1;

			setAll(2, 11);	
			states[2]['"'] = -1;
			states[2]['\\'] = 12;

			setAll(3, 3);	
			states[3]['"'] = 5;
			states[3]['\\'] = 4;
						
			setAll(4, 3);
			states[4][','] = 1;
			states[4][','] = 1;
			
			setAll(5, WHITE, 5);
			states[5][':'] = 6;
			
			setAll(6, WHITE, 7);
			states[6][','] = 1;
			states[6]['}'] = -1;
			
			setAll(7, WHITE, 7);
			states[7][','] = 1;
			states[7]['}'] = -1;
			
			//  Array Steps

			setAll(8, WHITE, 9);
			states[8][','] = 8;
			states[8][']'] = -1;

			setAll(9, WHITE, 9);
			states[9][','] = 8;
			states[9][']'] = -1;
			
			
			setAll(10, -2);
			setAll(10, DIGITS, 10);  // Digit
			setAll(10, "eE", 15);  // Exponential
			
			setAll(11, 11);
			states[11]['\\'] = 12;
			states[11]['"'] = -1;

			setAll(12, 11);
			
			setAll(13, 3);
			setAll(14, -1);
			setAll(15, 16);
			setAll(16, -2);
			setAll(16, DIGITS, 16);  // Digit
		}
	}
	
	public static JSONNode StateMachine(String json, int i, String thisKey){
		if (states == null) InitStateMachine();
		
		int state = 0;
		//int lastState = 0;
		String nextKey = "";
		String strVal = "";
		String exp = "";
		double dVal = 0;
		JSONNode out = null;
		//JSONNode out = new JSONArray(parent, thisKey);
		JSONObject group = null;
		JSONArray array = null;
		JSONString string = null;
		JSONNumber number = null;
		Vector<Integer> priorStates = new Vector<Integer>();
		char l = 0;
		char p = 0;
		
		
		while (state >= 0 && i < json.length()){
			l = p;
			p = json.charAt(i++);
			char n = (i < json.length()) ? n = json.charAt(i) : 0;
			char nn = (i < json.length() - 1) ? nn = json.charAt(i + 1) : 0;
			priorStates.add(state);
			int previousState = state;
			state = (p < 256) ? state = states[state][p] : states[state][' '];
			
			switch (state){
			case 1:
				if (group == null){
					group = new JSONObject();
					out = group;
				}
				nextKey = "";
				break;
				
			case 3:
				nextKey += p;
				break;
			
			case 6:
				if (n != '}'){
//					while (WHITE.contains(n + "")) {
//						l = p;
//						p = json.charAt(i++);
//						n = (i < json.length()) ? n = json.charAt(i) : 0;
//					}
					JSONNode h = StateMachine(json, i, nextKey);
					group.put(nextKey, h);
					i = h.endC;
				}
				break;

			// Array Path, step 3
				
			case 8:
				if (array == null){
					array = new JSONArray();
					out = array;
				}
				if (n != ']'){
//					while (WHITE.contains(n + "")) {
//						l = p;
//						p = json.charAt(i++);
//						n = (i < json.length()) ? n = json.charAt(i) : 0;
//					}
					JSONNode g = StateMachine(json, i, thisKey);
					array.add(g);
					i = g.endC;
				}
				break;
				
			// Number path
				
			case 10:
				if (number == null){
					number = new JSONNumber(0);
					out = number;
					strVal = "";
					exp = "";
				}
					
				strVal += p;
				try {
					number.setValue(Double.parseDouble(strVal));
				} catch (NumberFormatException e) {
				} 
				break;	

				
			// String path, steps 6, 7, 9
			case 2:
				string = new JSONString("");
				out = string;
				strVal = "";
				break;

			case 11:
			case 12:
				strVal += p;
				string.setValue(strVal);
				break;
				
			case 14:
				if (json.substring(i - 1, i + 3).equalsIgnoreCase("true")){
					out = new JSONBoolean(true);
				} else if (json.substring(i - 1, i + 4).equalsIgnoreCase("false")){
					out = new JSONBoolean(false);
				} else if (json.substring(i - 1, i + 3).equalsIgnoreCase("null")){
					out = new JSONNull();
				}
				if (out != null){
					i += out.toString().length() - 1;
					state = -1;
				} else {
					state = -3;
				}
				break;
				
			case 16:
				exp += p;
				try {
					number.setValue(Double.parseDouble(strVal) * (Math.pow(10, Double.parseDouble(exp))));
				} catch (NumberFormatException e) {
				} 
				break;	

			case -4:
				if (array == null){
					array = new JSONArray();
				}
				out = array;
				break;	

			case -3:
				int min = (i < 10) ? 0 : i - 10;
				int max = (i + 20 < json.length()) ? i + 20 : json.length();
				String beforeError = json.substring(min, i - 1);
				String afterError = json.substring(i, max);
				System.out.println("Encountered illegal JSON at position " + i + " characters " + beforeError + ", " + afterError);
			}
		}
		if (state != -3){
			if (out == null) {
				out = new JSONArray();
			}
			out.endC = i;
		} else {
			if (i < 10){
				System.out.println("Error at beginning: d" + json);			 	
			} else {
				System.out.println("Illegal JSON: " + json.substring(i - 10, i) + "|" + json.substring(i, i + 10));
			}
		}
		if (state == -2) out.endC --; 
		return out;
	}
	
	
	public static void setAll(int state, String chars, int setting){
		for (int i = 0;i < chars.length();i++){
			states[state][chars.charAt(i)] = setting;
		}
	}
	public static void setAll(int state, int setting){
		for (int i = 0;i < states[state].length;i++){
			states[state][i] = setting;
		}
	}

	public static String removeExtraWhiteSpaces(String json) {
	    StringBuilder result = new StringBuilder(json.length());
	    boolean inQuotes = false;
	    boolean escapeMode = false;
	    for (char character : json.toCharArray()) {
	        if (escapeMode) {
	            result.append(character);
	            escapeMode = false;
	        } else if (character == '"') {
	            inQuotes = !inQuotes;
	            result.append(character);
	        } else if (character == '\\') {
	            escapeMode = true;
	            result.append(character);
	        } else if (!inQuotes && character == ' ') {
	            continue;
	        } else {
	            result.append(character);
	        }
	    }
	    return result.toString();
	}

	public static JSONNode parseFile(File f) throws FileNotFoundException{
		FileReader in = new FileReader(f);
		StringBuilder json = new StringBuilder();
		try {
			while (in.ready()){
				json.append((char)in.read());
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return parse(json.toString());
	}

}
