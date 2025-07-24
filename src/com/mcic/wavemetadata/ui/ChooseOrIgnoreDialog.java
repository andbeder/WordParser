package com.mcic.wavemetadata.ui;

import java.awt.BorderLayout;
import java.awt.Frame;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.swing.JButton;
import javax.swing.JDialog;
import javax.swing.JLabel;
import javax.swing.JList;
import javax.swing.JPanel;
import javax.swing.JTable;
import javax.swing.border.EmptyBorder;

import com.mcic.wavemetadata.tool.WaveDataQuality;

public class ChooseOrIgnoreDialog extends JDialog {
	private final JPanel contentPanel = new JPanel();
	private JList list;
	private String[] options;
	private boolean doIgnore;
	

	public static void main(String[] args) {
		try {
			String[] options = {"One", "Two", "Three"};
			ChooseOrIgnoreDialog dialog = new ChooseOrIgnoreDialog("This is an example", options);
			dialog.setDefaultCloseOperation(JDialog.DISPOSE_ON_CLOSE);
			dialog.setVisible(true);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public ChooseOrIgnoreDialog(String title, String[] options) {
		super((Frame)null, title, true);
		ChooseOrIgnoreDialog dialog = this;
		this.options = options;
		setBounds(100, 100, 838, 783);
		getContentPane().setLayout(new BorderLayout());
		contentPanel.setBorder(new EmptyBorder(5, 5, 5, 5));
		getContentPane().add(contentPanel, BorderLayout.CENTER);
		contentPanel.setLayout(new BorderLayout(0, 0));
		{
			JLabel label = new JLabel(title);
			list = new JList(options);
			contentPanel.add(label, BorderLayout.NORTH);
			contentPanel.add(list, BorderLayout.CENTER);
		}
		{
			JButton ignore = new JButton("Ignore");
			JButton ok = new JButton("OK");
			JPanel buttonPanel = new JPanel();
			buttonPanel.add(ignore);
			buttonPanel.add(ok);
			contentPanel.add(buttonPanel, BorderLayout.SOUTH);

			ignore.addActionListener(new ActionListener() {
				public void actionPerformed(ActionEvent e) {
					doIgnore = true;
					dialog.setVisible(false);
				}
			});

			ok.addActionListener(new ActionListener() {
				public void actionPerformed(ActionEvent e) {
					doIgnore = false;
					dialog.setVisible(false);
				}
			});

		}
	}
	
	public String getSelected() {
		int i = list.getSelectedIndex();
		if (i == -1) {
			return "Ignore";
		}
		return options[i];
	}
}
