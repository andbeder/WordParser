package com.mcic.wavemetadata.ui;

import java.awt.BorderLayout;
import java.awt.Container;
import java.awt.FlowLayout;

import javax.swing.JButton;
import javax.swing.JDialog;
import javax.swing.JPanel;
import javax.swing.border.EmptyBorder;

import com.mcic.wavemetadata.tool.WaveDataQuality;

import javax.swing.JTextArea;
import javax.swing.SwingUtilities;

import net.miginfocom.swing.MigLayout;
import java.awt.event.ActionListener;
import java.awt.event.ActionEvent;
import javax.swing.JScrollPane;

public class DataQualityDialog extends JDialog {

	private static final long serialVersionUID = 1L;
	private final JPanel contentPanel = new JPanel();
	private JTextArea textArea;
	private JScrollPane scrollPane;
	private JButton btnSave;
	private WaveDataQuality wdq;
	private JButton btnNewButton;

	/**
	 * Launch the application.
	 */
	public static void main(String[] args) {
		try {
			DataQualityDialog dialog = new DataQualityDialog(null);
			dialog.setDefaultCloseOperation(JDialog.DISPOSE_ON_CLOSE);
			dialog.setVisible(true);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void setText(String text) {
		textArea.setText(text);
	}

	/**
	 * Create the dialog.
	 */
	public DataQualityDialog(WaveDataQuality wdq) {
		this.wdq = wdq;
		DataQualityDialog dialog = this;
		setBounds(100, 100, 838, 783);
		getContentPane().setLayout(new BorderLayout());
		contentPanel.setBorder(new EmptyBorder(5, 5, 5, 5));
		getContentPane().add(contentPanel, BorderLayout.CENTER);
		contentPanel.setLayout(new BorderLayout(0, 0));
		{
			scrollPane = new JScrollPane();
			getContentPane().add(scrollPane, BorderLayout.CENTER);
		}
		{
			textArea = new JTextArea();
			scrollPane.setViewportView(textArea);
		}
		{
			JPanel buttonPane = new JPanel();
			buttonPane.setLayout(new FlowLayout(FlowLayout.RIGHT));
			getContentPane().add(buttonPane, BorderLayout.SOUTH);
			{
				JButton cancelButton = new JButton("Close");
				cancelButton.addActionListener(new ActionListener() {
					public void actionPerformed(ActionEvent e) {
						dialog.setVisible(false);
						dialog.dispose();
					}
				});
				{
					btnSave = new JButton("Save");
					btnSave.addActionListener(new ActionListener() {
						public void actionPerformed(ActionEvent e) {
							SwingUtilities.invokeLater(new Runnable() {
								@Override
								public void run() {
									wdq.saveDataflow(textArea.getText(), false);
								}
							});
						}
					});
					buttonPane.add(btnSave);
				}
				{
					btnNewButton = new JButton("Save and Run");
					btnNewButton.addActionListener(new ActionListener() {
						public void actionPerformed(ActionEvent e) {
							wdq.saveDataflow(textArea.getText(), true);
						}
					});
					buttonPane.add(btnNewButton);
				}
				cancelButton.setActionCommand("Cancel");
				buttonPane.add(cancelButton);
			}
		}
	}

}
