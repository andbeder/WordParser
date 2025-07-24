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

import com.mcic.util.json.JSONArray;
import com.mcic.util.json.JSONNode;
import com.mcic.util.json.JSONObject;
import com.mcic.util.json.JSONString;
import com.mcic.wavemetadata.ui.ChooseOrIgnoreDialog;


public class WaveDashboardTabs{
	private JSONNode widgets;
	private String masterPageName;

	public static void main(String[] args) {
		WaveDashboardTabs app = new WaveDashboardTabs();
		StringBuilder json = new StringBuilder();
		try {
			Scanner s = new Scanner(new File("C:\\Users\\abeder\\OneDrive - MCIC Vermont, Inc\\Documents\\dashboard.json"));
			while (s.hasNextLine()) {
				String line = s.nextLine();
				json.append(line);
			}
			s.close();
			String out = app.buildTabs(json.toString());
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
	
	public boolean isWithin(JSONNode layout_1, JSONNode layout_2) {
		int row = layout_1.get("row").asInt();
		int col = layout_1.get("column").asInt();
		int r = layout_2.get("row").asInt();
		int rs = layout_2.get("rowspan").asInt();
		int c = layout_2.get("column").asInt();
		int cs = layout_2.get("colspan").asInt();
		if (row >= r && row < r + rs) {
			if (col >= c && col < c + cs) {
				return true;
			}
		}
		return false;
	}
	
	public String keepMaster(String json) {
		JSONNode root = JSONNode.parse(json);
		JSONNode state = root.get("state");
		JSONNode pages = state.get("gridLayouts").elementAt(0).get("pages");
		JSONNode newPages = new JSONArray();
		state.get("gridLayouts").elementAt(0).put("pages", newPages);
		
		for (JSONNode page : pages.values()) {
			String name = page.get("label").asString();
			if (name.equals("Master")) {
				newPages.add(page);
			}
		}
		return root.toString();
	}
	
	public String buildTabs(String json) {
		JSONNode root = JSONNode.parse(json);
		JSONNode state = root.get("state");
		JSONNode pages = state.get("gridLayouts").elementAt(0).get("pages");
		
		JSONObject master = null;
		for (JSONNode page : pages.values()) {
			if (page.get("label").asString().equals("Master")) {
				master = (JSONObject)page;
				masterPageName = master.get("name").asString();
			}
		}
		
		if (master != null) {
			JSONNode newPages = new JSONArray();
			state.get("gridLayouts").elementAt(0).setObject("pages", newPages);
			widgets = state.get("widgets");
			
			//  Parse Containers
			List<JSONNode> layouts = new LinkedList<JSONNode>(); 
			for (JSONNode n : master.get("widgets").values()) {
				layouts.add(n);
			}
			
			layouts.sort(new Comparator<JSONNode>() {

				@Override
				public int compare(JSONNode n1, JSONNode n2) {
					int r1 = n1.get("row").asInt();
					int r2 = n2.get("row").asInt();
					int c1 = n1.get("column").asInt();
					int c2 = n2.get("column").asInt();
					if (r1 != r2) {
						return r1 - r2;
					} else if (c1 != c2)  {
						return c1 - c2;
					} else {
						int isContainer1 = widgets.get(n1.get("name").asString()).get("type").asString().equals("container") ? 1 : 0;
						int isContainer2 = widgets.get(n2.get("name").asString()).get("type").asString().equals("container") ? 1 : 0;
						return isContainer2 - isContainer1;
					}
				}
				
			});
			
			JSONNode activeStyle = null;
			JSONNode inactiveStyle = null;
			String firstTabWidgetName = null;
			
			//  Identify tab names from link widgets
			Map<String, JSONNode> tabLinks = new LinkedHashMap<String, JSONNode>();  //  Layout JSON for links referencing tabs
			Map<String, JSONNode> tabContainers = new TreeMap<String, JSONNode>();	 //  Identified tab containers
			Map<String, JSONArray> tabWidgetList = new LinkedHashMap<String, JSONArray>();
			Map<String, String> linkMapping = new TreeMap<String, String>();
			for (JSONNode layout : layouts) {
				String widgetName = layout.get("name").asString();
				JSONNode widget = widgets.get(widgetName); 
				if (widget.get("type").asString().equals("link")) {
					String linkName = widget.get("parameters").get("text").asString();
					if (!linkName.equals("<") && !linkName.equals(">")) {
						tabLinks.put(linkName, layout);
						if (activeStyle == null) {
							activeStyle = layout.get("widgetStyle");
							firstTabWidgetName = widgetName;
						} else if (inactiveStyle == null) {
							inactiveStyle = layout.get("widgetStyle");
						}
					}
				}
			}
			
			//  Identify component layouts and remove them from the list
			int firstContainerRow = 10000;
			for (JSONNode n : layouts) {
				String widgetName = n.get("name").asString();
				if (widgets.get(widgetName).get("type").asString().equals("container")) {
					if (tabLinks.containsKey(widgetName)) {
						if (n.get("row").asInt() < firstContainerRow) {
							firstContainerRow = n.get("row").asInt();
						}
						tabContainers.put(widgetName, n);
					}
				}
			}
			
			//  Scrape Master for widgets within tab containers and everything else
			JSONArray globalLayouts = new JSONArray();
			for (JSONNode layout : layouts) {
				String inTab = null;
				int row = layout.get("row").asInt();
				int col = layout.get("column").asInt();
				for (JSONNode tab : tabContainers.values()) {
					int r = tab.get("row").asInt();
					int rs = tab.get("rowspan").asInt();
					int c = tab.get("column").asInt();
					int cs = tab.get("colspan").asInt();
					if (row >= r && row < r + rs) {
						if (col >= c && col < c + cs) {
							String tabName = tab.get("name").asString();
							inTab = tabName;
						}
					}
				}
				
				if (inTab == null) {
					globalLayouts.add(layout.clone());
				} else {
					JSONArray tabWidgets = tabWidgetList.get(inTab);
					if (tabWidgets == null) {
						tabWidgets = new JSONArray();
						tabWidgetList.put(inTab, tabWidgets);
					}
					tabWidgets.add(layout.clone());
				}
			}
			
			//  Identify any tab links that need to be mapped because the container doesn't exist
			for (String tabName : tabLinks.keySet()) {
				if (!tabContainers.containsKey(tabName)) {
					String[] options = new String[tabContainers.size()];
					int i = 0;
					for (String name : tabContainers.keySet()) {
						options[i++] = name;
					}
					ChooseOrIgnoreDialog d = new ChooseOrIgnoreDialog("Choose the container name below:", options);
					d.setVisible(true);
					String out = d.getSelected();
					if (!out.equals("Ignore")) {
						linkMapping.put(tabName,  out);
					}
				}
			}
			
			//  Report out results
			System.out.println("Widgets on all pages:");
			for (JSONNode n : globalLayouts.values()) {
				System.out.println("  -" + n.get("name").asString());
			}
			for (Entry<String, JSONArray> tab : tabWidgetList.entrySet()) {
				System.out.println("Widgets on tab: " + tab.getKey());
				for (JSONNode n : ((JSONNode)tab.getValue()).values()) {
					System.out.println("  -" + n.get("name").asString());
				}
			}
			
			//  Build Tabs
			int firstTabRow = firstContainerRow;
			for (String tabName : tabContainers.keySet()) {
				JSONNode newPage = master.clone();
				JSONArray layoutWidets = (JSONArray)newPage.get("widgets");
				JSONNode tabLayout = tabContainers.get(tabName);
				JSONArray widgetList = tabWidgetList.get(tabName);
				layoutWidets.clear();
				newPage.get("label").setString(tabName);
				newPage.get("name").setString(newPage.get("name").asString() + "-" + tabName);
				
				
				for (JSONNode n : globalLayouts.values()) {
					JSONNode newLayout = n.clone();
					//  Clone any collapsing link widgets
					String widgetName = newLayout.get("name").asString();
					JSONNode widget = widgets.get(widgetName);
					if (widget.get("type").asString().equals("link") 
							&& widget.get("parameters").get("text").asString().equals("<")) {
						JSONNode newWidget = widget.clone();
						newLayout.get("name").setString(widgetName + tabName);
						widgets.put(widgetName + tabName, newWidget);
					}
					layoutWidets.add(newLayout);
				}
				//layoutWidets.add(tabLayout);
				int tabRow = tabLayout.get("row").asInt();
				int rowDiff = tabRow - firstTabRow;
				for (JSONNode n : widgetList.values()) {
					JSONNode newWidget = n.clone();
					newWidget.get("row").setInt(n.get("row").asInt() - rowDiff);
					layoutWidets.add(newWidget);
				}
				newPages.add(newPage);
				System.out.println("Adding page " + newPage.get("label").asString());
			}
			System.out.println("Adding page Master");
			newPages.add(master);
			
			// Attach Links to new pages.
			for (JSONNode n : tabLinks.values()) {
				String nodeName = n.get("name").asString();
				JSONNode widget = widgets.get(nodeName);
				String linkLabel = widget.get("parameters").get("text").asString();
				if (linkLabel.equals("Matrix")) {
					System.out.println("Matrix");
				}
				if (linkMapping.containsKey(linkLabel)) {
					linkLabel = linkMapping.get(linkLabel);
				}
				widget.get("parameters").get("destinationType").setString("page");
				((JSONObject)widget.get("parameters")).addObject("destinationLink").addString("name", masterPageName + "-" + linkLabel);
				
				//  Set link format to Inactive;
//				n.put("widgetStyle", inactiveStyle);
			}
			
			//  Clone link widgets for active pages
			for (JSONNode n : tabLinks.values()) {
				String linkName = n.get("name").asString();
				JSONNode linkWidget = widgets.get(linkName);
				String linkLabel = linkWidget.get("parameters").get("text").asString();
				JSONNode newWidget = linkWidget.clone();
				String newLinkWidgetName = linkName + "Active";
				widgets.put(newLinkWidgetName, newWidget);
				for (JSONNode page : newPages.values()) {
					if (page.get("label").asString().equals(linkLabel)) {
						for (JSONNode layout : page.get("widgets").values()) {
							if (layout.get("name").asString().equals(linkName)) {
								layout.get("name").setString(newLinkWidgetName);
								//layoutWidget.put("widgetStyle", activeStyle);
							}
						}
					}
				}
			}

			
			
			// Check to see if any links needs to be wired to a tab
			Map<String, String> pageLinks = new TreeMap<String, String>();
			for (JSONNode page : newPages.values()) {
				String pageName = page.get("label").asString();
				pageLinks.put(pageName, page.get("name").asString());
				for (JSONNode layout : page.get("widgets").values()) {
					String widgetName = layout.get("name").asString();
					JSONNode widget = widgets.get(widgetName);
					if (widget.get("type").asString().equals("link")) {
						
						JSONNode dLink = widget.get("parameters").get("destinationLink");
						if (dLink != null) {
							String name = dLink.get("name").asString();
							if (pageLinks.containsKey(name)) {
								String pageLink = pageLinks.get(name);
								if (pageName.endsWith("NF")) {
									pageLink += "NF";
								}
								dLink.get("name").setString(pageLink);
							}
						}
					}
				}
			}
			
			//  Clone link widget for first tab (active in Master)
			JSONNode newWidget = widgets.get(firstTabWidgetName).clone();
			String newFirstTabWidgetName = firstTabWidgetName + "C";
			widgets.put(newFirstTabWidgetName, newWidget);
			
			//  Scan through all pages and format link layouts appropriately
			for (JSONNode page : newPages.values()) {
				String pageName = page.get("label").asString();
				for (JSONNode layout : page.get("widgets").values()) {
					String widgetName = layout.get("name").asString();
					if (widgetName.equals(firstTabWidgetName) && !pageName.equals("Master")) {
						widgetName = newFirstTabWidgetName;
						layout.get("name").setString(newFirstTabWidgetName);
					}
					
					JSONNode widget = widgets.get(widgetName);
					if (widget.get("type").asString().equals("link")) {
						boolean isActive = false;
						if (widgetName.endsWith("Active") || 
								(widgetName.equals(firstTabWidgetName) && pageName.equals("Master"))) {
							isActive = true;
						}
						String name = widget.get("parameters").get("text").asString();
						if (tabLinks.containsKey(name)) {
							JSONNode style = isActive ? activeStyle : inactiveStyle;
							layout.setObject("widgetStyle", style.clone());
						}
					}
				}
			}
			
		}
		
		return root.toString();
	}
}
