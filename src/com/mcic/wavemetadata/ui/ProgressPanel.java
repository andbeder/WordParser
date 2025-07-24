package com.mcic.wavemetadata.ui;

import java.util.LinkedList;
import java.util.List;
import java.util.Vector;

import javax.swing.JPanel;
import javax.swing.JProgressBar;
import javax.swing.JScrollPane;

import net.miginfocom.swing.MigLayout;
import javax.swing.JTextArea;
import javax.swing.JButton;
import javax.swing.JDialog;
import javax.swing.JLabel;
import javax.swing.SwingConstants;
import java.awt.Font;
import java.awt.event.ActionListener;
import java.awt.event.ActionEvent;

public class ProgressPanel extends JPanel {
	private int steps;
	private Vector<ProgressPanelStep> stepNames;
	private static final long serialVersionUID = 1L;
	private JTextArea textArea;
	private JProgressBar progressBar;
	private JButton btnNewButton;
	
	public class ProgressPanelStep {
		String message;
		boolean complete;
		ProgressPanel parent;
		boolean noComplete;
		
		public ProgressPanelStep(ProgressPanel parent, String message, boolean noComplete){
			this.message = message;
			this.parent = parent;
			this.noComplete = noComplete;
			complete = false;
		}
		
		
		public void complete() {
			complete = true;
			parent.refresh();
		}
		
		public void setText(String text) {
			message = text;
			parent.refresh();
		}
		
		public void addNote(String note) {
			message += "..." + note;
			parent.refresh();
		}
		
		public String toString() {
			String out =  message;
			if (!noComplete){
				out += "...";
				out += complete ? "done" : "";
			}
			return out;
		}
	}

	/**
	 * Create the panel.
	 */
	public ProgressPanel(int steps) {
		this.steps = steps;
		stepNames = new Vector<ProgressPanelStep>();
		setLayout(new MigLayout("", "[146px,grow]", "[][14px][grow][]"));
		
		JLabel lblNewLabel = new JLabel("Word Frequency App");
		lblNewLabel.setFont(new Font("Tahoma", Font.BOLD, 16));
		lblNewLabel.setHorizontalAlignment(SwingConstants.CENTER);
		add(lblNewLabel, "cell 0 0,alignx center");
		
		progressBar = new JProgressBar();
		progressBar.setMaximum(steps);
		add(progressBar, "cell 0 1,growx,aligny top");
		
		textArea = new JTextArea();
		JScrollPane scrollPane = new JScrollPane(textArea);
		add(scrollPane, "cell 0 2,grow");
		
		btnNewButton = new JButton("Cancel");
		btnNewButton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				System.exit(0);
			}
		});
		add(btnNewButton, "cell 0 3,alignx right");
	}

	public void show() {
		JDialog dialog = new JDialog();
		dialog.setBounds(100, 100, 450, 800);
		dialog.add(this);
		dialog.setVisible(true);
	}
	
	
	public ProgressPanelStep nextStep(String message, boolean noComplete) {
		ProgressPanelStep out = new ProgressPanelStep(this, message, noComplete);
		stepNames.add(out);
		progressBar.setValue(stepNames.size());
		refresh();
		return out;
	}
	
	public void refresh() {
		StringBuilder b = new StringBuilder();
		for (int i = 0;i < stepNames.size();i++) {
			String s = stepNames.elementAt(i).toString();
			b.append(s);
			b.append("\n");
		}
		textArea.setText(b.toString());
	}
	
	public void setClose(boolean close) {
		btnNewButton.setText(close ? "Close" : "Cancel");
	}
}
