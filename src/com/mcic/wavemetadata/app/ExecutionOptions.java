package com.mcic.wavemetadata.app;

import java.awt.BorderLayout;
import java.awt.Container;
import java.awt.FlowLayout;
import java.io.File;
import java.util.List;

import javax.swing.JButton;
import javax.swing.JDialog;
import javax.swing.JPanel;
import javax.swing.SwingUtilities;
import javax.swing.SwingWorker;
import javax.swing.border.EmptyBorder;
import javax.swing.table.AbstractTableModel;

import com.mcic.util.json.JSONNode;
import com.mcic.wavemetadata.tool.WaveDataQuality;
import com.mcic.wavemetadata.tool.WaveMetadata;
import com.mcic.wavemetadata.tool.WaveMetadata.Dashboard;
import com.mcic.wavemetadata.ui.DataQualityDialog;

import java.awt.event.ActionListener;
import java.awt.event.ActionEvent;
import net.miginfocom.swing.MigLayout;
import javax.swing.JTable;
import javax.swing.JLabel;

public class ExecutionOptions extends JDialog {

	private static final long serialVersionUID = 1L;
	private final JPanel contentPanel = new JPanel();
	private static WaveMetadataReader reader;
	private JTable table;
	private JLabel lblDatasets;
	private JLabel lblRecipes;
	private JLabel lblDashboards;
	private JLabel lblFields;
	private JLabel lblApplications;
	private JLabel lblPWSF;
	private JButton btnSaveDatasets;
	private JButton btnSaveRecipes;
	private JButton btnSaveDashboards;
	private JButton btnSaveApplications;

	/**
	 * Launch the application.
	 */
	public static void main(String[] args) {
		File propFile = null;
		String curDir = "";
		int i = 0;
		while (i < args.length) {
			String cmd = args[i].toLowerCase();
			switch (cmd) {
			case "-p": 
				propFile = new File(curDir + args[++i]);
				break;
			case "-d":
				curDir = args[++i];
				char c = curDir.charAt(curDir.length() - 1);
				if (c != '/' && c != '\\') {
					curDir += "/";
				}
				break;
			}
			i++;
		}
		
		reader = new WaveMetadataReader(propFile);
		
		try {
			ExecutionOptions dialog = new ExecutionOptions();
			dialog.setDefaultCloseOperation(JDialog.DISPOSE_ON_CLOSE);
			dialog.setVisible(true);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	
	public void update() {
		lblDatasets.setText(reader.datasets.size() + "");
		lblRecipes.setText(reader.recipes.size() + "");
		lblDashboards.setText(reader.dashboards.size() + "");
		lblFields.setText(reader.dashfields.size() + "");
		lblApplications.setText(reader.apps.size() + "");
		lblPWSF.setText(reader.pageWidgetStepFields.size() + "");
		
		btnSaveDatasets.setEnabled(reader.datasets.size() > 0);
		btnSaveRecipes.setEnabled(reader.recipes.size() > 0);
		btnSaveDashboards.setEnabled(reader.dashboards.size() > 0);
		btnSaveApplications.setEnabled(reader.apps.size() > 0);
	}

	/**
	 * Create the dialog.
	 */
	public ExecutionOptions() {
		setBounds(100, 100, 618, 300);
		getContentPane().setLayout(new BorderLayout());
		contentPanel.setBorder(new EmptyBorder(5, 5, 5, 5));
		getContentPane().add(contentPanel, BorderLayout.CENTER);
		{
			JButton btnNewButton = new JButton("Complete Inventory");
			btnNewButton.addActionListener(new ActionListener() {
				public void actionPerformed(ActionEvent e) {
					reader.doInventory();
				}
			});
			contentPanel.setLayout(new MigLayout("", "[][grow]", "[grow][][][][]"));
			contentPanel.add(btnNewButton, "cell 0 0,growx,aligny center");
		}
		{
			JPanel panel = new JPanel();
			contentPanel.add(panel, "cell 1 0 1 4,grow");
			panel.setLayout(new MigLayout("", "[][][][][]", "[][][][][][]"));
			{
				JLabel lblNewLabel = new JLabel("Datasets");
				panel.add(lblNewLabel, "cell 0 0,growx");
			}
			{
				lblDatasets = new JLabel("0");
				panel.add(lblDatasets, "cell 1 0");
			}
			{
				JButton btnNewButton_10 = new JButton("Load");
				btnNewButton_10.addActionListener(new ActionListener() {
					public void actionPerformed(ActionEvent e) {
						reader.readDatasets();
						update();
					}
				});
				panel.add(btnNewButton_10, "cell 2 0,growx");
			}
			{
				btnSaveDatasets = new JButton("Save");
				btnSaveDatasets.addActionListener(new ActionListener() {
					public void actionPerformed(ActionEvent e) {
						reader.saveDatasets();
					}
				});
				panel.add(btnSaveDatasets, "cell 3 0,growx");
			}
			{
				JLabel lblNewLabel_1 = new JLabel("Recipes");
				panel.add(lblNewLabel_1, "cell 0 1");
			}
			{
				{
					lblRecipes = new JLabel("0");
					panel.add(lblRecipes, "cell 1 1");
				}
			}
			{
				JButton btnNewButton_8 = new JButton("Load");
				btnNewButton_8.addActionListener(new ActionListener() {
					public void actionPerformed(ActionEvent e) {
						SwingWorker task = new SwingWorker() {
							@Override
							protected Object doInBackground() throws Exception {
								btnNewButton_8.setEnabled(false);
								reader.readRecipes();
								update();
								btnNewButton_8.setEnabled(true);
								return "Success";
							}
						
						};
						task.execute();
					}
				});
				panel.add(btnNewButton_8, "cell 2 1");
			}
			btnSaveRecipes = new JButton("Save");
			btnSaveRecipes.addActionListener(new ActionListener() {
				public void actionPerformed(ActionEvent e) {
					reader.saveRecipes();
				}
			});
			panel.add(btnSaveRecipes, "cell 3 1");
			{
				JLabel lblNewLabel_2 = new JLabel("Dashboards");
				panel.add(lblNewLabel_2, "cell 0 2");
			}
			{
				{
					{
						lblDashboards = new JLabel("0");
						panel.add(lblDashboards, "cell 1 2");
					}
				}
				JButton btnNewButton_4 = new JButton("Load");
				btnNewButton_4.addActionListener(new ActionListener() {
					public void actionPerformed(ActionEvent arg0) {
						btnNewButton_4.setEnabled(false);
						btnNewButton_4.setText("Loading...");
						SwingWorker task = new SwingWorker() {

							@Override
							protected Object doInBackground() throws Exception {
								reader.readDashboards();
								update();
								btnNewButton_4.setEnabled(true);
								btnNewButton_4.setText("Load");
								return "Success";
							}
							
						};
						task.execute();
					}
				});
				panel.add(btnNewButton_4, "cell 2 2 1 3");
			}
			btnSaveDashboards = new JButton("Save");
			btnSaveDashboards.addActionListener(new ActionListener() {
				public void actionPerformed(ActionEvent e) {
					btnSaveDashboards.setEnabled(false);
					SwingWorker task = new SwingWorker() {
						@Override
						protected Object doInBackground() throws Exception {
							reader.saveDashboards();
							btnSaveDashboards.setEnabled(true);
							return "Success";
						}
					};
					task.execute();
				}
			});
			panel.add(btnSaveDashboards, "cell 3 2 1 3");
			{
				JLabel lblNewLabel_3 = new JLabel("Dashboard Fields");
				panel.add(lblNewLabel_3, "cell 0 3");
			}
			{
				lblFields = new JLabel("0");
				panel.add(lblFields, "cell 1 3");
			}
			{
				JLabel lblNewLabel_5 = new JLabel("Page Widget Step Fields");
				panel.add(lblNewLabel_5, "cell 0 4");
			}
			{
				lblPWSF = new JLabel("0");
				panel.add(lblPWSF, "cell 1 4");
			}
			{
				JLabel lblNewLabel_4 = new JLabel("Applications");
				panel.add(lblNewLabel_4, "cell 0 5");
			}
			{
				{
					lblApplications = new JLabel("0");
					panel.add(lblApplications, "cell 1 5");
				}
			}
			{
				JButton btnNewButton_6 = new JButton("Load");
				btnNewButton_6.addActionListener(new ActionListener() {
					public void actionPerformed(ActionEvent e) {
						reader.readApplications();
						update();
					}
				});
				panel.add(btnNewButton_6, "cell 2 5");
			}
			btnSaveApplications = new JButton("Save");
			btnSaveApplications.addActionListener(new ActionListener() {
				public void actionPerformed(ActionEvent e) {
					reader.saveApplications();
				}
			});
			panel.add(btnSaveApplications, "cell 3 5");
		}
		{
			JButton btnNewButton_1 = new JButton("Update Prod Replication Fields");
			contentPanel.add(btnNewButton_1, "cell 0 1,growx");
		}
		{
			JButton btnNewButton_2 = new JButton("Point Recipes to Prod");
			btnNewButton_2.addActionListener(new ActionListener() {
				public void actionPerformed(ActionEvent e) {
					reader.setRecipesToProd();
				}
			});
			contentPanel.add(btnNewButton_2, "cell 0 2,growx");
		}
		{
			JButton btnNewButton_3 = new JButton("Write Recipe Datasets");
			btnNewButton_3.addActionListener(new ActionListener() {
				public void actionPerformed(ActionEvent e) {
					SwingUtilities.invokeLater(new Runnable() {
						@Override
						public void run() {
							reader.readRecipes();
							update();
						}
					});
				}
			});
			contentPanel.add(btnNewButton_3, "cell 0 3,growx");
		}
		{
			JButton btnNewButton_5 = new JButton("Create Data Quality Dataflow");
			btnNewButton_5.addActionListener(new ActionListener() {
				public void actionPerformed(ActionEvent e) {
						SwingUtilities.invokeLater(new Runnable() {
						@Override
						public void run() {
							WaveMetadata meta = reader.meta;
							WaveDataQuality dq = new WaveDataQuality(meta);
							JSONNode root = dq.buildDataflow();
							DataQualityDialog dialog = new DataQualityDialog(dq);
							dialog.setDefaultCloseOperation(JDialog.DISPOSE_ON_CLOSE);
							dialog.setVisible(true);
							dialog.setText(root.asString());
						}				
					});
				}
			});
			contentPanel.add(btnNewButton_5, "cell 0 4,growx");
		}
		{
			JPanel buttonPane = new JPanel();
			buttonPane.setLayout(new FlowLayout(FlowLayout.RIGHT));
			getContentPane().add(buttonPane, BorderLayout.SOUTH);
			{
				JButton cancelButton = new JButton("Close");
				cancelButton.addActionListener(new ActionListener() {
					public void actionPerformed(ActionEvent e) {
						JButton cancel = ((JButton)e.getSource());
						Container c = cancel.getParent();
						JDialog d = (JDialog)c.getParent().getParent().getParent().getParent();
						d.dispose();
					}
				});
				cancelButton.setActionCommand("Cancel");
				buttonPane.add(cancelButton);
			}
		}
		update();
	}

}
