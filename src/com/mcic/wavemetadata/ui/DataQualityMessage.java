package com.mcic.wavemetadata.ui;

import java.awt.BorderLayout;
import java.awt.FlowLayout;

import javax.swing.JButton;
import javax.swing.JComponent;
import javax.swing.JDialog;
import javax.swing.JPanel;
import javax.swing.border.EmptyBorder;

import com.mcic.util.FuzzyScore;
import com.mcic.util.json.Pair;
import com.mcic.wavemetadata.tool.WaveDataQuality;
import com.mcic.wavemetadata.tool.WaveMetadata;
import com.mcic.wavemetadata.tool.WaveMetadata.Source;
import com.mcic.wavemetadata.tool.WaveMetadata.Instance;

import javax.swing.JLabel;
import javax.swing.JTextField;
import java.awt.event.ActionListener;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;
import java.awt.event.ActionEvent;
import java.awt.Font;
import javax.swing.JTextPane;
import javax.swing.JTable;
import javax.swing.table.DefaultTableModel;
import javax.swing.JScrollPane;
import net.miginfocom.swing.MigLayout;
import javax.swing.ListSelectionModel;

public class DataQualityMessage extends JDialog {

	private static final long serialVersionUID = 1L;
	private final JPanel contentPanel = new JPanel();
	private JTextField textInstanceId;
	private DataQualityMessage dialog;
	private JLabel lblNewLabel;
	private JPanel panel;
	private JTextPane txtMessage;
	private JLabel lblMatches;
	private Instance instance;
	private WaveMetadata meta;
	private List<Pair<Integer, String>> matches;
	private JTable table;
	private JButton btnNewButton;
	private JButton btnConnect;
	private JButton btnDelete;
	private JButton ignoreAllButton;
	private boolean isIgnoreAll;
	private JScrollPane scrollPane;
	private JPanel panel_1;
	private JScrollPane scrollPane_1;

	/**
	 * Launch the application.
	 */
	public static void main(String[] args) {
		try {
			DataQualityMessage dialog = new DataQualityMessage(null, null);
			dialog.addMatches("Policy_Layer_Code");
			dialog.addMatches("Policy_Name");
			dialog.addMatches("Claimant");
			dialog.setDefaultCloseOperation(JDialog.DISPOSE_ON_CLOSE);
			dialog.setVisible(true);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Create the dialog.
	 */
	
	public DataQualityMessage(WaveMetadata meta, Instance inst) {
		instance = inst == null ? new Instance("0x437474", "0x3gdd", "Policy_Layer_Group", false, new Source("0x3gdd", "CRMA", "Test_AB", "", "", "", "", null, null, null, null)) : inst;
		this.meta = meta;
		this.dialog = this;
		matches = new LinkedList<Pair<Integer, String>>();
		isIgnoreAll = false;
		
		setBounds(100, 100, 820, 559);
		getContentPane().setLayout(new MigLayout("", "[]", "[200][300]"));
		contentPanel.setBorder(new EmptyBorder(5, 5, 5, 5));
		getContentPane().add(contentPanel, "cell 0 0,grow");
		contentPanel.setLayout(new BorderLayout(0, 0));
		{
			panel = new JPanel();
			contentPanel.add(panel, BorderLayout.SOUTH);
		}
		panel.setLayout(new BorderLayout(0, 0));
		{
			lblNewLabel = new JLabel("Instance Id");
			lblNewLabel.setFont(new Font("Tahoma", Font.PLAIN, 11));
			panel.add(lblNewLabel, BorderLayout.WEST);
		}
		{
			textInstanceId = new JTextField();
			panel.add(textInstanceId);
			textInstanceId.setText(instance.id);
			textInstanceId.setColumns(20);
		}
		{
			txtMessage = new JTextPane();
			txtMessage.setEditable(false);
			txtMessage.setText("");
			contentPanel.add(txtMessage, BorderLayout.CENTER);
		}
		{
			JPanel buttonPane = new JPanel();
			getContentPane().add(buttonPane, "cell 0 1,growx,aligny top");
			{
				JButton ignoreButton = new JButton("Ignore");
				ignoreButton.addActionListener(new ActionListener() {
					public void actionPerformed(ActionEvent e) {
						dialog.setVisible(false);
						dialog.dispose();
					}
				});
				buttonPane.setLayout(new MigLayout("", "[1px][466px][73px][63px][65px][79px]", "[441px]"));
				{
					lblMatches = new JLabel("");
					buttonPane.add(lblMatches, "cell 0 0,alignx left,aligny center");
				}
				{
					{
						table = new JTable();
						table.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
						table.setModel(new DefaultTableModel(
							new Object[][] {
								{"(no matches available)", "0"},
							},
							new String[] {
								"API Name", "Score"
							}
						) {
							Class[] columnTypes = new Class[] {
								String.class, Object.class
							};
							public Class getColumnClass(int columnIndex) {
								return columnTypes[columnIndex];
							}
						});
					}
					{
						panel_1 = new JPanel();
						buttonPane.add(panel_1, "cell 1 0,alignx left,aligny top");
						panel_1.setLayout(new MigLayout("", "[]", "[]"));
						{
							scrollPane_1 = new JScrollPane(table);
							panel_1.add(scrollPane_1, "cell 0 0");
						}
					}
					//scrollPane = new JScrollPane(table);
					//buttonPane.add(scrollPane);
					//buttonPane.add(table);
				}
//				{
//					scrollPane = new JScrollPane(table);
//					buttonPane.add(scrollPane);
//				}
				{
					{
						btnDelete = new JButton("Delete");
						btnDelete.addActionListener(new ActionListener() {
							public void actionPerformed(ActionEvent e) {
								meta.deleteInstance(instance);
								dialog.setVisible(false);
								dialog.dispose();
							}
						});
						btnConnect = new JButton("Connect");
						btnConnect.addActionListener(new ActionListener() {
							public void actionPerformed(ActionEvent e) {
								String match = (String) table.getModel().getValueAt(table.getSelectedRow(), 0);
								meta.updateInstanceAPIName(instance, match);
								dialog.setVisible(false);
								dialog.dispose();
							}
						});
						buttonPane.add(btnConnect, "cell 2 0,alignx left,aligny center");
						buttonPane.add(btnDelete, "cell 3 0,alignx left,aligny center");
					}
				}
				ignoreButton.setActionCommand("OK");
				buttonPane.add(ignoreButton, "cell 4 0,alignx left,aligny center");
				getRootPane().setDefaultButton(ignoreButton);
			}
			{
				ignoreAllButton = new JButton("Ignore All");
				ignoreAllButton.addActionListener(new ActionListener() {
					public void actionPerformed(ActionEvent e) {
						isIgnoreAll = true;
						dialog.setVisible(false);
						dialog.dispose();
					}
				});
				buttonPane.add(ignoreAllButton, "cell 5 0,alignx left,aligny center");
			}
		}
		this.setModalityType(ModalityType.DOCUMENT_MODAL);
	}
	
	public int showTableMessage() {
		textInstanceId.setText("Cannot find dataset '" + instance.source.table + "' within CRMA");
		setVisible(true);
		if (isIgnoreAll) {
			return 1;
		} else {
			return 0;
		}
	}
	
	public void addMatches(String matchesAPIName) {
		FuzzyScore fs = new FuzzyScore(Locale.ENGLISH);
		int dist = fs.fuzzyScore(instance.fieldAPIName, matchesAPIName);
		matches.add(new Pair<Integer, String>(dist, matchesAPIName));

		List<Object[]> rows = matches.stream().sorted(Collections.reverseOrder()).map(p -> {
			return new Object[] {p.second, p.first};
		}).collect(Collectors.toList());
		Object[][] rowsArray = rows.toArray(new Object[rows.size()][]);
		
		table.setModel(new DefaultTableModel(
			
				rowsArray,
			new String[] {
				"API Name", "Score"
			}
		));
	}
	
	public int showFieldMessage() {
		txtMessage.setText("Cannot find field '" + instance.fieldAPIName + "' within CRMA dataset " + instance.source.table);
		setVisible(true);
		if (isIgnoreAll) {
			return 1;
		} else {
			return 0;
		}
	}
}
