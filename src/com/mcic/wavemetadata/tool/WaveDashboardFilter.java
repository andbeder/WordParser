package com.mcic.wavemetadata.tool;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Vector;

import javax.swing.JOptionPane;

import com.mcic.util.json.JSONArray;
import com.mcic.util.json.JSONNode;
import com.mcic.util.json.JSONObject;
import com.mcic.util.json.JSONString;


public class WaveDashboardFilter{
	//private JSONNode widgets;
	//private String masterPageName;

	/***************************************************************
	 *  Testing main() class built to run program on a file. This 
	 *  class is meant to be invoked through the GUIDashboardFilter
	 *  class main()
	 * 
	 */
	
	public static void main(String[] args) {
		WaveDashboardFilter app = new WaveDashboardFilter();
		StringBuilder json = new StringBuilder();
		try {
			Scanner s = new Scanner(new File("C:\\Users\\abeder\\OneDrive - MCIC Vermont, Inc\\Documents\\dashboard.json"));
			while (s.hasNextLine()) {
				String line = s.nextLine();
				json.append(line);
			}
			s.close();
			String out = app.addFilters(json.toString());
			FileWriter writer = new FileWriter("C:\\Users\\abeder\\OneDrive - MCIC Vermont, Inc\\Documents\\dashboard_tabs.json");
			writer.write(out);
			writer.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	/**************************************************************************
	 * Strips all unused widgets from a CRMA page. Unused is defined as any
	 * node in the widgets array which is not referenced on any page layouts
	 */
	
	public String removeUnusedWidgets(String json) {
		JSONNode root = JSONNode.parse(json);
		JSONNode state = root.get("state");
		JSONNode pages = state.get("gridLayouts").elementAt(0).get("pages");
		Set<String> usedWidgets = new TreeSet<String>();
		
		for (JSONNode page : pages.values()) {
			for (JSONNode widget : page.get("widgets").values()) {
				usedWidgets.add(widget.get("name").asString());
			}
		}
		
		JSONNode oldWidgets = state.get("widgets");
		JSONNode newWidgets = new JSONObject();
		state.put("widgets", newWidgets);
		
		for (Entry<String, JSONNode> set : oldWidgets.entrySet()) {
			if (usedWidgets.contains(set.getKey())) {
				newWidgets.put(set.getKey(), set.getValue());
			}
		}
		
		return root.toString();
	}
	
	
	/***************************************************************************
	 * Sorts the JSON in the pages widgets array based on the order of the row
	 * first and then the column
	 */
	
	public JSONArray sortLayoutWidgets(JSONNode widgets) {
		JSONArray out = (JSONArray) widgets.clone();
		Collections.sort(out.getCollection(), new Comparator<JSONNode>() {
			@Override
			public int compare(JSONNode n1, JSONNode n2) {
				if (n1.get("row").asInt() != n2.get("row").asInt()) {
					return n1.get("row").asInt() - n2.get("row").asInt();
				} else {
					return n1.get("column").asInt() - n2.get("column").asInt();
				}
			}
		});
		return out;
	}
	
	
	public String sortLayouts(String json) {
		JSONNode root = JSONNode.parse(json);
		JSONNode state = root.get("state");
		JSONNode pages = state.get("gridLayouts").elementAt(0).get("pages");
		for (JSONNode page : pages.values()) {
			JSONNode widgets = page.get("widgets");
			sortLayoutWidgets(widgets);
		}
		
		return root.toString();
	}
		
		
	
	/*****************************************************************************
	 * Creates a set of cloned pages with a name ending in NF with any filter
	 * panels removed from the layout. Existing widgets are stretched to take up
	 * the additional space. Any existing filter-less tabs are removed.
	 */
	
	
	public String addFilters(String json) {
		// Create initial variables
		JSONNode root = JSONNode.parse(json);
		JSONNode state = root.get("state");
		JSONNode pages = state.get("gridLayouts").elementAt(0).get("pages");
		Map<String, String> tabLabelMap = new TreeMap<String, String>();
		int[] columnMap = new int[50];
		int[] colspanMap = new int[50];
		//JSONNode master = pages.elementAt(0);
		JSONNode filterContainer = null;
		JSONNode logoContainer = null;
		int res = JOptionPane.showConfirmDialog(null, "Do you want to start with the filter showing?");
		boolean startShowingFilter = res == JOptionPane.YES_OPTION;
		
		
		//  Identify widget layouts referencing the containers named Filter and Logo. 
		for (JSONNode page : pages.values()) {
			String name = page.get("name").asString();
			String label = page.get("label").asString();
			tabLabelMap.put(label,  name);
//			if (label.equals("Master")) {
//				master = page;
//			}
			for (JSONNode layout : page.get("widgets").values()) {
				String widgetName = layout.get("name").asString(); 
				if (widgetName.equals("Filter")) {
					filterContainer = layout;
				} else if (widgetName.equals("Logo")) {
					logoContainer = layout;
				}
			}
		}
		
		//  Is the filter container located on the left or the right side of the page?
		boolean isLeft = filterContainer == null ? true : filterContainer.get("column").asInt() < 5;

		//******************************************************************
		//  Find width of filter pane and set columnMap and colSpanMap
		//   variables containing the new scale of the filter-less pages
		//******************************************************************

		int filterWidth = filterContainer.get("colspan").asInt();
		int maxWidth = state.get("gridLayouts").elementAt(0).get("numColumns").asInt();
		int logoHeight = 0;
		if (logoContainer != null) {
			logoHeight = logoContainer.get("rowspan").asInt();
		}

		if (filterWidth == -1) {
			return null;
		}
		for (int i = 0;i < 50;i++) {
			columnMap[i] = -1;
		}
		
		int startColumn = isLeft ? filterWidth - 2 : 0;
		double remainder = (double)(filterWidth - 2) / (double)maxWidth;
		double val = 0;
		for (int i = 0;i < maxWidth;i++) {
			colspanMap[i] = (int)(val);
			int newColumn = (int)(val + 0.5);
			val += remainder + 1.0;
			columnMap[startColumn + i] = newColumn;
		}		

		JSONNode widgets = state.get("widgets");
		JSONArray newPages = new JSONArray();
		state.get("gridLayouts").elementAt(0).put("pages", newPages);
		int maxAllowed = isLeft ? maxWidth : maxWidth - filterWidth + 2;
		
		for (int i = 0;i < columnMap.length;i++) {
			System.out.println(i + ": " + columnMap[i] + ", " + colspanMap[i]);
		}
		
		
		/**************************************************************************
		 * Example of JSON from CRMA for a layout node
		 *      {
                "colspan": 27,
                "column": 7,
                "name": "Navigation",
                "row": 0,
                "rowspan": 3,
                "widgetStyle": {
                }

		 * Example of JSON from CMRA for a widget node (link)
		 *       "link_1": {
			        "parameters": {
			          "destinationLink": {
			            "name": "8c17e75d-9a6a-4317-9c2e-03118d7fe8ce-Activity"
			          },
			          "destinationType": "page",
			          "fontSize": 16,
			          "includeState": false,
			          "text": "Activity",
			          "textAlignment": "center",
			          "textColor": "#0070D2"
			        },
			        "type": "link"
			      },

		 */
		
		
		
		//******************************************************************
		//  Build filter-less view of all the pages except Master
		//******************************************************************
		
		int pageCount = pages.size();
		for (int i = 0;i < pageCount;i++) {
			JSONNode page = pages.elementAt(i);
			String pageLable = page.get("label").asString();
			String pageName = page.get("name").asString();

			if (pageLable.equals("Master")) {
				newPages.add(page);  //  Add page to the layouts

			} else if (!pageName.endsWith("NF")) {

				//******************************************************************
				//  Duplicate page
				//******************************************************************
			
				JSONNode newPage = page.clone();
				if (startShowingFilter) {
					newPages.add(page);
					newPages.add(newPage);
				} else {
					newPages.add(newPage);
					newPages.add(page);
				}
				newPage.get("label").setString(pageLable + "NF");
				newPage.get("name").setString(pageName + "NF");
			
				//******************************************************************
				//  Stretch page components to fit available space
				//******************************************************************
				
				//  Create new layout array and paste it into the page JSON
				JSONNode newLayouts = new JSONArray();
				newPage.put("widgets", newLayouts);
				
				for (JSONNode oldLayout : page.get("widgets").values()) {
					// Iterate through all the layout nodes on the page
					//  Set up initial variables including the position and size on the page
					JSONNode layout = oldLayout.clone();
					String widgetName = layout.get("name").asString();
					JSONNode widget = widgets.get(widgetName);
					int row = layout.get("row").asInt();
					int column = layout.get("column").asInt();
					int colspan = layout.get("colspan").asInt();
					
					//  Is this on the logo row? If so do nothing
					if (row < logoHeight) {			
						newLayouts.add(layout); //  Add the widget to the layout
					} else {
						//  Slide everything to the left or the right depending on the filter position;
						int newColumn = columnMap[column];
						int newSpan = colspanMap[colspan];
						
						//  hard-code collapsed filter position on the page
						if (widgetName.equals("Filter")) {
							newColumn = isLeft ? 0 : maxWidth - 2;
							newSpan = 2;
						}
						
						//  Is the components overflowing the page? If so trim it to fit
						if (newColumn + newSpan > maxWidth) {
							newSpan = maxWidth - newColumn;
						}
						
						// Update the layout position of all relevant widgets including the filter
						if (widgetName.equals("Filter") || (newColumn >= 0 && newColumn < maxAllowed)) {
							newLayouts.add(layout); //  Add the widget to the layout
							layout.get("column").setInt(newColumn);
							layout.get("colspan").setInt(newSpan);
						}
						
						//  Find the collapse link widget and clone it for all pages
						if (widget.get("type").asString().equals("link")
								&& widget.get("parameters").get("destinationType").asString().equals("page")) {
							String newWidgetName = widgetName + "NF";
							JSONNode newWidget = widget.clone();
							widgets.put(newWidgetName, newWidget);
							String destinationLinkName = newWidget.get("parameters").get("destinationLink").get("name").asString();
							newWidget.get("parameters").get("destinationLink").get("name").setString(destinationLinkName + "NF");
							layout.get("name").setString(newWidgetName);
						}
					}
				}
			}
		}
		
		/************************************************************************************************************
		 *  Sort layout nodes so that rows and columns are in order. Identify small gaps and stretch the colspan to
		 *  meet the next layout
		 */
		
		for (JSONNode page : pages.values()) {
			//String layoutJSON = sortLayouts(page.get("widgets").toCompressedString());
			//JSONNode layouts = JSONNode.parse(layoutJSON);
			JSONNode layouts = sortLayoutWidgets(page.get("widgets"));
			for (int i = 0;i < layouts.size() - 1;i++) {
				JSONNode thisLayout = layouts.elementAt(i);
				JSONNode nextLayout = layouts.elementAt(i + 1);
				int column = thisLayout.get("column").asInt();
				int colspan = thisLayout.get("colspan").asInt();
				int nextColumn = nextLayout.get("column").asInt();
				if (column + colspan == nextColumn - 1) {
					thisLayout.get("colspan").setInt(nextColumn - column);
					System.out.println("Stretching " + thisLayout.get("name").asString() + " from " + colspan + " to " + (nextColumn - column));
				}
			}
		}
		
		//  Clone all the navigation link tabs so they link between filter and filter-free pages
		//Set<JSONNode> linkWidgets = new TreeSet<JSONNode>();
		
		//  Go through all of the pages
		for (JSONNode page : newPages.values()) {
			String pageName = page.get("name").asString();
			for (JSONNode layout : page.get("widgets").values()) {
				String widgetName = layout.get("name").asString();
				boolean collapse = pageName.endsWith("NF");
				String linkPageName = collapse ? pageName.substring(0, pageName.length() - 2) : pageName + "NF";
				String newWidgetName = (collapse ? "collapse_" : "expand_") + widgetName;
				String carrot = isLeft ^ collapse ? ">" : "<";
				JSONNode oldWidget = widgets.get(widgetName);
				
				if (oldWidget.get("type").asString().equals("link")) {
					char c = oldWidget.get("parameters").get("text").asString().charAt(0);
					if (c == '<' || c == '>') {
						//linkWidgets.add(widget);
						
						JSONNode newWidget = oldWidget.clone();
						newWidget.get("parameters").get("destinationType").setString("page");
						newWidget.get("parameters").get("text").setString(carrot);
						if (collapse) {
							layout.get("widgetStyle").get("borderWidth").setInt(1);
							layout.get("widgetStyle").get("borderEdges").clear();
						} else {
							layout.get("widgetStyle").get("borderWidth").setInt(3);
						}
						JSONNode destinationLink = newWidget.get("parameters").get("destinationLink");
						if (destinationLink == null) {
							destinationLink = new JSONObject();
							newWidget.get("parameters").put("destinationLink", destinationLink);
							destinationLink.put("name", new JSONString("test"));
						}
						newWidget.get("parameters").get("destinationLink").get("name").setString(linkPageName);
						widgets.put(newWidgetName, newWidget);
						layout.get("name").setString(newWidgetName);
						//layout.get("column").setInt(0);
						//layout.get("widgetStyle").get("borderWidth").setInt(3);
					}
				}
			}
		}
		
		return root.toString();
	}
}
