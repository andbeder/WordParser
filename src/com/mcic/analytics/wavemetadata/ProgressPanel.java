package com.mcic.analytics.wavemetadata;

import java.util.Vector;
import javax.swing.JPanel;
import javax.swing.JProgressBar;
import javax.swing.JScrollPane;
import net.miginfocom.swing.MigLayout;
import javax.swing.JTextArea;
import javax.swing.JButton;
import javax.swing.JDialog;
import javax.swing.JEditorPane;
import javax.swing.JLabel;
import javax.swing.SwingConstants;
import java.awt.Font;
import java.awt.event.ActionListener;
import java.awt.event.ActionEvent;

public class ProgressPanel extends JPanel {
    private int totalSteps;
    private int tickers;
    private Vector<ProgressPanelStep> stepNames;
    private static final long serialVersionUID = 1L;
    private JEditorPane textArea;
    private JProgressBar progressBar;
    private JButton btnAction;
    private JDialog dialog;  // to allow closing
    private JLabel ticker;


    public class ProgressPanelStep {
        String message;
        boolean complete;
        ProgressPanel parent;
        boolean noComplete;

        public ProgressPanelStep(ProgressPanel parent, String message, boolean noComplete) {
            this.message = message;
            this.parent = parent;
            this.noComplete = noComplete;
            this.complete = false;

        }

        public void complete() {
            this.complete = true;
            parent.refresh();
        }

        public void setText(String text) {
            this.message = text;
            parent.refresh();
        }

        public void addNote(String note) {
            this.message += "..." + note;
            parent.refresh();
        }

        @Override
        public String toString() {
            String out = message;
            if (!noComplete) {
                out += "...";
                out += complete ? "done" : "";
            }
            return out;
        }
    }

    /**
     * Create the panel with known total number of steps.
     * @param totalSteps number of steps to expect
     */
    public ProgressPanel(int totalSteps) {
        this.totalSteps = totalSteps;
        this.stepNames = new Vector<>();
        tickers = 0;
        
        setLayout(new MigLayout("", "[grow]", "[][14px][grow][]"));

        JLabel lblTitle = new JLabel("Wave Metadata Progress");
        lblTitle.setFont(new Font("Tahoma", Font.BOLD, 16));
        lblTitle.setHorizontalAlignment(SwingConstants.CENTER);
        add(lblTitle, "cell 0 0,alignx center");

        // Initialize and add the ticker label
        ticker = new JLabel("Ticker message here");
        ticker.setFont(new Font("Tahoma", Font.PLAIN, 12));
        ticker.setHorizontalAlignment(SwingConstants.CENTER);
        add(ticker, "cell 0 1,alignx center");
        progressBar = new JProgressBar(0, totalSteps);
        add(progressBar, "cell 0 1,growx,aligny top");

        textArea = new JEditorPane("text/html", "");
        textArea.setEditable(false);
        JScrollPane scrollPane = new JScrollPane(textArea);
        add(scrollPane, "cell 0 2,grow");

        btnAction = new JButton("Cancel");
        btnAction.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
                if ("Close".equals(btnAction.getText())) {
                    if (dialog != null) {
                        dialog.dispose();
                    }
                } else {
                    System.exit(0);
                }
            }
        });
        add(btnAction, "cell 0 3,alignx right");
    }

    /**
     * Show the panel in a dialog.
     */
    public void show() {
        dialog = new JDialog();
        dialog.setTitle("Progress");
        dialog.setBounds(100, 100, 450, 800);
        dialog.setModal(false);
        dialog.add(this);
        dialog.setVisible(true);
    }

    /**
     * Advance to the next step: adds a new step entry and increments progress.
     * @param message description of the step
     * @param noComplete if true, this step will not show completion status
     */
    public ProgressPanelStep newCompleted(String message) {
    		return  nextStep(message, false);
    }
    
    public ProgressPanelStep newUncompleted(String message) {
			return  nextStep(message, true);
	}
	
   
    private ProgressPanelStep nextStep(String message, boolean noComplete) {
        ProgressPanelStep step = new ProgressPanelStep(this, message, noComplete);
        stepNames.add(step);
        progressBar.setValue(stepNames.size());
        refresh();
        return step;
    }

    public void newTicker(String message) {
    			ticker.setText(message);
    			refresh();
	}
    
    /**
     * Refresh the display: update textArea and toggle button label if finished.
     */
    public void refresh() {
        StringBuilder builder = new StringBuilder();
        for (ProgressPanelStep step : stepNames) {
            builder.append(step.toString()).append("\n");
        }
        textArea.setText(builder.toString());
        // If all steps reached, switch button to Close
        progressBar.setValue(tickers);
        if (progressBar.getValue() >= progressBar.getMaximum()) {
            btnAction.setText("Close");
        }
    }

    /**
     * (Optional) Manually set button text for final state.
     */
    public void setClose(boolean close) {
        btnAction.setText(close ? "Close" : "Cancel");
    }
}
