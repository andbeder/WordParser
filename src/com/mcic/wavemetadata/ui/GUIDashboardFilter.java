package com.mcic.wavemetadata.ui;

import java.awt.BorderLayout;
import java.awt.Container;
import java.awt.FlowLayout;

import javax.swing.JButton;
import javax.swing.JDialog;
import javax.swing.JPanel;
import javax.swing.border.EmptyBorder;

import com.mcic.wavemetadata.tool.WaveDashboardFilter;
import com.mcic.wavemetadata.tool.WaveDashboardTabs;

import javax.swing.JTextArea;
import java.awt.event.ActionListener;
import java.io.File;
import java.awt.event.ActionEvent;
import java.awt.Font;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;

public class GUIDashboardFilter extends JDialog {

	private static final long serialVersionUID = 1L;
	private final JPanel contentPanel = new JPanel();
	private JTextArea txtJSON;
	private boolean isFirstClick;
	

	/**
	 * Launch the application.
	 */
	public static void main(String[] args) {
		File propFile = null;
		if (args.length > 0) {
			propFile = new File(args[0]);
		}

		try {
			GUIDashboardFilter dialog = new GUIDashboardFilter();
			dialog.setDefaultCloseOperation(JDialog.DISPOSE_ON_CLOSE);
			dialog.setVisible(true);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Create the dialog.
	 */
	public GUIDashboardFilter() {
		isFirstClick = true;
		
		setBounds(100, 100, 1106, 801);
		getContentPane().setLayout(new BorderLayout());
		contentPanel.setBorder(new EmptyBorder(5, 5, 5, 5));
		getContentPane().add(contentPanel, BorderLayout.CENTER);
		contentPanel.setLayout(new BorderLayout(0, 0));
		{
			txtJSON = new JTextArea();
			txtJSON.addMouseListener(new MouseAdapter() {
				@Override
				public void mouseClicked(MouseEvent e) {
					if (isFirstClick) {
						isFirstClick = false;
						txtJSON.setText("");
					}
				}
			});
			txtJSON.setFont(new Font("Courier New", Font.PLAIN, 13));
			txtJSON.setText("Drop JSON here...");
			contentPanel.add(txtJSON, BorderLayout.CENTER);
		}
		{
			JPanel buttonPane = new JPanel();
			buttonPane.setLayout(new FlowLayout(FlowLayout.RIGHT));
			getContentPane().add(buttonPane, BorderLayout.SOUTH);
			{
				JButton okButton = new JButton("Collapse Filters");
				okButton.addActionListener(new ActionListener() {
					public void actionPerformed(ActionEvent e) {
						String json = txtJSON.getText();
						WaveDashboardFilter tool = new WaveDashboardFilter();
						String output = tool.addFilters(json);
						txtJSON.setText(output);
					}
				});
				{
					JButton btnNewButton = new JButton("Create Tabs");
					btnNewButton.addActionListener(new ActionListener() {
						public void actionPerformed(ActionEvent e) {
							String json = txtJSON.getText();
							WaveDashboardTabs tool = new WaveDashboardTabs();
							String output = tool.buildTabs(json);
							txtJSON.setText(output);
						}
					});
					{
						JButton btnNewButton_1 = new JButton("Sort Layout JSON");
						btnNewButton_1.addActionListener(new ActionListener() {
							public void actionPerformed(ActionEvent e) {
								String json = txtJSON.getText();
								WaveDashboardFilter tool = new WaveDashboardFilter();
								String output = tool.sortLayouts(json);
								txtJSON.setText(output);
							}
						});
						buttonPane.add(btnNewButton_1);
					}
					{
						JButton btnNewButton_2 = new JButton("Clean Master");
						btnNewButton_2.addActionListener(new ActionListener() {
							public void actionPerformed(ActionEvent e) {
								String json = txtJSON.getText();
								WaveDashboardTabs tool = new WaveDashboardTabs();
								String output = tool.keepMaster(json);
								txtJSON.setText(output);
							}
						});
						buttonPane.add(btnNewButton_2);
					}
					buttonPane.add(btnNewButton);
				}
				okButton.setActionCommand("OK");
				buttonPane.add(okButton);
				getRootPane().setDefaultButton(okButton);
			}
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
				{
					JButton btnUnusedWidgets = new JButton("Remove Unused Widgets");
					btnUnusedWidgets.addActionListener(new ActionListener() {
						public void actionPerformed(ActionEvent e) {
							String json = txtJSON.getText();
							WaveDashboardFilter tool = new WaveDashboardFilter();
							String output = tool.removeUnusedWidgets(json);
							txtJSON.setText(output);
						}
					});
					buttonPane.add(btnUnusedWidgets);
				}
				cancelButton.setActionCommand("Cancel");
				buttonPane.add(cancelButton);
			}
		}
	}

}
