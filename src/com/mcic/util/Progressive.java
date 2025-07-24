package com.mcic.util;

import javax.swing.JDialog;

import com.mcic.wavemetadata.ui.ProgressPanel;
import com.mcic.wavemetadata.ui.ProgressPanel.ProgressPanelStep;

public class Progressive {
	private static ProgressPanel panel = null;
	
	public static final void setPanel(ProgressPanel p) {
		panel = p;
	}
	
	public ProgressPanelStep nextStep(String message) {
		return nextStep(message, false);
	}
	
	public ProgressPanelStep nextStep(String message, boolean noComplete) {
		if (panel == null) {
			panel = new ProgressPanel(10);
			JDialog dialog = new JDialog();
			dialog.setBounds(100, 100, 450, 800);
			dialog.add(panel);
			dialog.setVisible(true);
		}
		return panel.nextStep(message, noComplete);
	}
	
	public void setClose(boolean close) {
		panel.setClose(close);
	}
}