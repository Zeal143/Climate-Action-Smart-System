package com.climate.gui;

import com.climate.client.ClimateActionClient;
import com.climate.grpc.*;

import io.grpc.StatusRuntimeException;

import javax.swing.*;
import javax.swing.border.EmptyBorder;
import java.awt.*;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

public class ClimateActionGUI extends JFrame {

    private static final Logger logger = Logger.getLogger(ClimateActionGUI.class.getName());

    private static final Color BG = new Color(185, 180, 170);

    //values for dropdown
    private static final String[] LOCATIONS = {"DUBLIN", "LONDON", "BEIJING", "PARIS", "BERLIN"};
    private static final String[] PANEL_IDS = {"PANEL_DUBLIN_01", "PANEL_DUBLIN_02", "PANEL_CORK_01"};
    private static final String[] FR_SENSORS = {"SENSOR_LIFFEY_01", "SENSOR_LIFFEY_02", "SENSOR_LIFFEY_03"};
    private static final String[] WL_SENSORS = {"WL_SENSOR_WICKLOW_01", "WL_SENSOR_LIFFEY_01", "WL_SENSOR_DUBLIN_01"};
    private static final String[] FR_LOCS = {"DUBLIN", "WICKLOW", "CORK", "GALWAY", "LIMERICK"};

    // Client
    private final ClimateActionClient client = new ClimateActionClient();

    //Shared result area
    private JTextArea resultArea;

    // Tab 1 : Air Quality 
    private JComboBox<String> aqUnaryLocation;
    private JComboBox<String> aqStreamLocation;

    // Tab 2 : Flood Risk
    private JComboBox<String> frSensor1, frSensor2, frSensor3;
    private JTextField frRain1, frRain2, frRain3;
    private JComboBox<String> frLocation;
    private JComboBox<String> frBidiSensor, frBidiLocation;
    private JTextField frBidiLevel;

    // Tab 3 : Solar Panel 
    private JComboBox<String> spStatusPanelId;
    private JComboBox<String> spStreamPanelId;
    private JComboBox<String> spConfigPanelId;
    private JTextField spConfigAngle;
    private JTextField spBatchPanelIds, spBatchEnergies;

    // Constructor
    public ClimateActionGUI() {
        super("Climate Action Smart System – gRPC Client");
        setDefaultCloseOperation(DO_NOTHING_ON_CLOSE);
        addWindowListener(new WindowAdapter() {
            @Override
            public void windowClosing(WindowEvent e) {
                onExit();
            }
        });
        buildUI();
        discoverServicesAsync();
    }

    // UI Construction
    private void buildUI() {
        setExtendedState(JFrame.MAXIMIZED_BOTH);   // full screen
        setLayout(new BorderLayout());

        JTabbedPane tabs = new JTabbedPane();
        tabs.addTab("Air Quality Service", buildAirQualityTab());
        tabs.addTab("Flood Risk Service", buildFloodRiskTab());
        tabs.addTab("Solar Panel Service", buildSolarPanelTab());

        add(tabs, BorderLayout.CENTER);
        add(buildResultPanel(), BorderLayout.SOUTH);

        setLocationRelativeTo(null);
    }

    // Tab 1 – Air Quality Service
    private JPanel buildAirQualityTab() {
        JPanel panel = tabPanel();

        // Row 0 – Unary: Get Current Air Quality
        JPanel row1 = rowPanel();
        row1.add(fixedLabel("Unary Service – Get Current Air Quality", 290));
        row1.add(new JLabel("Location"));
        row1.add(hgap(6));
        aqUnaryLocation = combo(LOCATIONS);
        row1.add(aqUnaryLocation);
        row1.add(hgap(8));
        JButton getAqiBtn = new JButton("Get Air Quality");
        getAqiBtn.addActionListener(e -> doGetCurrentAirQuality());
        row1.add(getAqiBtn);
        panel.add(row1);
        panel.add(sep());

        // Row 1 – Server Streaming: Stream Air Quality Readings
        JPanel row2 = rowPanel();
        row2.add(fixedLabel("Server Streaming – Stream Air Quality Readings", 290));
        row2.add(new JLabel("Location"));
        row2.add(hgap(6));
        aqStreamLocation = combo(LOCATIONS);
        row2.add(aqStreamLocation);
        row2.add(hgap(8));
        JButton streamBtn = new JButton("Stream Readings");
        streamBtn.addActionListener(e -> doStreamAirQuality());
        row2.add(streamBtn);
        panel.add(row2);

        panel.add(Box.createVerticalGlue());
        return panel;
    }

    // Tab 2 – Flood Risk Service
    private JPanel buildFloodRiskTab() {
        JPanel panel = tabPanel();

        // Row 1 – Client Streaming: Report Rainfall Readings
        JPanel row1a = rowPanel();
        row1a.add(fixedLabel("Client Streaming – Report Rainfall Readings", 290));
        JButton prepareBtn = new JButton("Prepare Client Stream");
        prepareBtn.addActionListener(e -> doReportRainfall());
        row1a.add(prepareBtn);
        row1a.add(hgap(6));
        row1a.add(new JLabel("S1"));
        row1a.add(hgap(2));
        frSensor1 = combo(FR_SENSORS);
        row1a.add(frSensor1);
        row1a.add(hgap(2));
        row1a.add(new JLabel("mm"));
        frRain1 = new JTextField("12.5", 5);
        row1a.add(frRain1);
        row1a.add(hgap(4));
        row1a.add(new JLabel("S2"));
        frSensor2 = combo(FR_SENSORS);
        frSensor2.setSelectedIndex(1);
        row1a.add(frSensor2);
        row1a.add(hgap(2));
        row1a.add(new JLabel("mm"));
        frRain2 = new JTextField("45.0", 5);
        row1a.add(frRain2);
        row1a.add(hgap(4));
        row1a.add(new JLabel("S3"));
        frSensor3 = combo(FR_SENSORS);
        frSensor3.setSelectedIndex(2);
        row1a.add(frSensor3);
        row1a.add(hgap(2));
        row1a.add(new JLabel("mm"));
        frRain3 = new JTextField("78.3", 5);
        row1a.add(frRain3);
        panel.add(row1a);

        // Row 1b – Location (on its own line)
        JPanel row1b = rowPanel();
        row1b.add(hgap(171));
        row1b.add(fixedLabel("", 290)); // indent to align
        row1b.add(new JLabel("Location:"));
        row1b.add(hgap(6));
        frLocation = combo(FR_LOCS);
        row1b.add(frLocation);
        panel.add(row1b);
        panel.add(sep());

        // Row 1 – BiDi Streaming: Monitor Flood Alerts
        JPanel row2 = rowPanel();
        row2.add(fixedLabel("Bi-directional Streaming – Monitor Flood Alerts", 290));
        JButton biDiBtn = new JButton("Prepare BiDi Stream");
        biDiBtn.addActionListener(e -> doMonitorFlood());
        row2.add(biDiBtn);
        row2.add(hgap(15));
        row2.add(new JLabel("Sensor"));
        row2.add(hgap(4));
        frBidiSensor = combo(WL_SENSORS);
        row2.add(frBidiSensor);
        row2.add(hgap(8));
        row2.add(new JLabel("Water Level (m)"));
        row2.add(hgap(4));
        frBidiLevel = new JTextField("3.5", 6);
        row2.add(frBidiLevel);
        row2.add(hgap(8));
        row2.add(new JLabel("Location"));
        row2.add(hgap(4));
        frBidiLocation = combo(FR_LOCS);
        frBidiLocation.setSelectedItem("WICKLOW");
        row2.add(frBidiLocation);
        panel.add(row2);

        panel.add(Box.createVerticalGlue());
        return panel;
    }

    // Tab 3 – Solar Panel Service
    private JPanel buildSolarPanelTab() {
        JPanel panel = tabPanel();

        // Row 0 – Get Panel Status
        JPanel row1 = rowPanel();
        row1.add(fixedLabel("Unary Service – Get Panel Status", 290));
        row1.add(new JLabel("Panel ID"));
        row1.add(hgap(6));
        spStatusPanelId = combo(PANEL_IDS);
        row1.add(spStatusPanelId);
        row1.add(hgap(8));
        JButton statusBtn = new JButton("Get Status");
        statusBtn.addActionListener(e -> doGetPanelStatus());
        row1.add(statusBtn);
        panel.add(row1);
        panel.add(sep());

        // Row 1 – Stream Energy Production
        JPanel row2 = rowPanel();
        row2.add(fixedLabel("Server Streaming – Stream Energy Production", 290));
        row2.add(new JLabel("Panel ID"));
        row2.add(hgap(6));
        spStreamPanelId = combo(PANEL_IDS);
        row2.add(spStreamPanelId);
        row2.add(hgap(8));
        JButton streamBtn = new JButton("Stream Energy");
        streamBtn.addActionListener(e -> doStreamEnergy());
        row2.add(streamBtn);
        panel.add(row2);
        panel.add(sep());

        // Row 2 – Adjust Panel Configuration
        JPanel row3 = rowPanel();
        row3.add(fixedLabel("Unary Service – Adjust Panel Configuration", 290));
        row3.add(new JLabel("Panel ID"));
        row3.add(hgap(6));
        spConfigPanelId = combo(PANEL_IDS);
        row3.add(spConfigPanelId);
        row3.add(hgap(8));
        row3.add(new JLabel("Angle (0–90°)"));
        row3.add(hgap(4));
        spConfigAngle = new JTextField("42.0", 6);
        row3.add(spConfigAngle);
        row3.add(hgap(8));
        JButton adjustBtn = new JButton("Adjust Panel");
        adjustBtn.addActionListener(e -> doAdjustPanel());
        row3.add(adjustBtn);
        panel.add(row3);
        panel.add(sep());

        // Row 3 – Upload Energy Batch
        JPanel row4 = rowPanel();
        row4.add(fixedLabel("Client Streaming – Upload Energy Batch", 290));
        JButton batchBtn = new JButton("Prepare Client Stream");
        batchBtn.addActionListener(e -> doUploadBatch());
        row4.add(batchBtn);
        row4.add(hgap(8));
        row4.add(new JLabel("Panel IDs (csv)"));
        row4.add(hgap(4));
        spBatchPanelIds = new JTextField("PANEL_DUBLIN_01,PANEL_DUBLIN_02,PANEL_CORK_01", 24);
        row4.add(spBatchPanelIds);
        row4.add(hgap(8));
        row4.add(new JLabel("Energy kWh (csv)"));
        row4.add(hgap(4));
        spBatchEnergies = new JTextField("2.3,1.8,3.1", 12);
        row4.add(spBatchEnergies);
        panel.add(row4);

        panel.add(Box.createVerticalGlue());
        return panel;
    }

    // Result Panel
    private JPanel buildResultPanel() {
        resultArea = new JTextArea(30, 80);
        resultArea.setEditable(false);
        resultArea.setFont(new Font("Monospaced", Font.PLAIN, 12));
        resultArea.setBackground(new Color(240, 240, 240));

        JScrollPane scroll = new JScrollPane(resultArea);
        scroll.setBorder(BorderFactory.createTitledBorder("Result"));
        scroll.setPreferredSize(new Dimension(0, 350));

        JPanel p = new JPanel(new BorderLayout());
        p.add(scroll, BorderLayout.CENTER);
        return p;
    }

    // Service Discovery
    private void discoverServicesAsync() {
        appendResult("Discovering services via mDNS...");
        new SwingWorker<Void, String>() {
            @Override
            protected Void doInBackground() throws Exception {
                client.init();
                return null;
            }

            @Override
            protected void done() {
                try {
                    get();
                    appendResult("All services discovered. Ready.");
                } catch (Exception ex) {
                    appendResult("Discovery failed: " + ex.getMessage()
                            + "\nMake sure all servers are running.");
                }
            }
        }.execute();
    }

    // gRPC Actions – Tab 1 : Air Quality
    private void doGetCurrentAirQuality() {
        String location = selectedValue(aqUnaryLocation);
        new SwingWorker<AirQualityResponse, Void>() {
            @Override
            protected AirQualityResponse doInBackground() {
                return client.getCurrentAirQuality(location);
            }

            @Override
            protected void done() {
                try {
                    AirQualityResponse r = get();
                    appendResult("[Unary] Location: " + r.getLocation()
                            + " | AQI: " + String.format("%.1f", r.getAqi())
                            + " | CO2: " + String.format("%.1f ppm", r.getCo2Level())
                            + " | Status: " + r.getStatus()
                            + " | " + r.getTimestamp());
                } catch (Exception ex) {
                    handleError(ex);
                }
            }
        }.execute();
    }

    private void doStreamAirQuality() {
        String location = selectedValue(aqStreamLocation);
        appendResult("[Server Stream] Starting for: " + location);
        client.streamAirQualityReadings(location,
                new ClimateActionClient.AirQualityStreamListener() {
            int count = 0;

            @Override
            public void onReading(AirQualityReading r) {
                count++;
                appendResult(String.format("  [%2d] AQI=%.1f | CO2=%.1f ppm | %s",
                        count, r.getAqi(), r.getCo2Level(), r.getTimestamp()));
            }

            @Override
            public void onError(Throwable t) {
                appendResult("[Server Stream] Error: " + t.getMessage());
            }

            @Override
            public void onCompleted() {
                appendResult("[Server Stream] Complete.");
            }
        });
    }

    // gRPC Actions – Tab 2 : Flood Risk
    private void doReportRainfall() {
        List<RainfallRequest> readings = new ArrayList<>();
        try {
            String loc = selectedValue(frLocation);
            readings.add(RainfallRequest.newBuilder()
                    .setSensorId(selectedValue(frSensor1))
                    .setRainfallMM(Float.parseFloat(frRain1.getText().trim()))
                    .setLocation(loc).build());
            readings.add(RainfallRequest.newBuilder()
                    .setSensorId(selectedValue(frSensor2))
                    .setRainfallMM(Float.parseFloat(frRain2.getText().trim()))
                    .setLocation(loc).build());
            readings.add(RainfallRequest.newBuilder()
                    .setSensorId(selectedValue(frSensor3))
                    .setRainfallMM(Float.parseFloat(frRain3.getText().trim()))
                    .setLocation(loc).build());
        } catch (NumberFormatException e) {
            appendResult("Error: Rainfall values must be valid numbers.");
            return;
        }

        appendResult("[Client Stream] Uploading " + readings.size() + " rainfall readings...");
        new SwingWorker<FloodRiskResponse, Void>() {
            @Override
            protected FloodRiskResponse doInBackground() throws Exception {
                return client.reportRainfallReadings(readings);
            }

            @Override
            protected void done() {
                try {
                    FloodRiskResponse r = get();
                    appendResult("[Client Stream] Summary:"
                            + " Readings: " + r.getReadingsCount()
                            + " | Avg: " + String.format("%.2f mm/hr", r.getAverageRainfall())
                            + " | Max: " + String.format("%.2f mm/hr", r.getMaxRainfall())
                            + " | Risk: " + r.getRiskLevel()
                            + " | " + r.getMessage());
                } catch (Exception ex) {
                    handleError(ex);
                }
            }
        }.execute();
    }

    private void doMonitorFlood() {
        float waterLevel;
        try {
            waterLevel = Float.parseFloat(frBidiLevel.getText().trim());
        } catch (NumberFormatException e) {
            appendResult("Error: Water level must be a valid number.");
            return;
        }

        FloodUpdate update = FloodUpdate.newBuilder()
                .setSensorId(selectedValue(frBidiSensor))
                .setWaterLevel(waterLevel)
                .setLocation(selectedValue(frBidiLocation))
                .build();

        appendResult("[BiDi Stream] Sending update: sensor="
                + update.getSensorId() + " level=" + waterLevel + "m"
                + " loc=" + update.getLocation());

        new SwingWorker<Void, FloodAlert>() {
            @Override
            protected Void doInBackground() throws Exception {
                client.monitorFloodAlerts(List.of(update),
                        new ClimateActionClient.FloodAlertListener() {
                    @Override
                    public void onAlert(FloodAlert a) {
                        publish(a);
                    }

                    @Override
                    public void onError(Throwable t) {
                        appendResult("[BiDi Stream] Error: " + t.getMessage());
                    }

                    @Override
                    public void onCompleted() {
                        appendResult("[BiDi Stream] Complete.");
                    }
                });
                return null;
            }

            @Override
            protected void process(List<FloodAlert> alerts) {
                for (FloodAlert a : alerts) {
                    appendResult("[BiDi Stream] Alert -> sensor=" + a.getSensorId()
                            + " | Risk: " + a.getRiskLevel()
                            + " | " + a.getAdvisoryMessage()
                            + " | " + a.getTimestamp());
                }
            }
        }.execute();
    }

    // gRPC Actions – Tab 3 : Solar Panel
    private void doGetPanelStatus() {
        String panelId = selectedValue(spStatusPanelId);
        new SwingWorker<PanelStatusResponse, Void>() {
            @Override
            protected PanelStatusResponse doInBackground() {
                return client.getPanelStatus(panelId);
            }

            @Override
            protected void done() {
                try {
                    PanelStatusResponse r = get();
                    appendResult("[Unary] Panel: " + r.getPanelId()
                            + " | Status: " + r.getStatus()
                            + " | Power: " + String.format("%.1f W", r.getPowerOutput())
                            + " | Efficiency: " + String.format("%.1f%%", r.getEfficiency())
                            + " | Temp: " + String.format("%.1f C", r.getTemperature())
                            + " | " + r.getLastUpdated());
                } catch (Exception ex) {
                    handleError(ex);
                }
            }
        }.execute();
    }

    private void doStreamEnergy() {
        String panelId = selectedValue(spStreamPanelId);
        appendResult("[Server Stream] Starting energy stream for: " + panelId);
        client.streamEnergyProduction(panelId,
                new ClimateActionClient.EnergyStreamListener() {
            int count = 0;

            @Override
            public void onReading(EnergyReading r) {
                count++;
                appendResult(String.format("  [%2d] Panel: %s | %.2f kWh | Sunlight: %.0f W/m2 | %s",
                        count, r.getPanelId(), r.getEnergyKwh(),
                        r.getSunlightLevel(), r.getTimestamp()));
            }

            @Override
            public void onError(Throwable t) {
                appendResult("[Server Stream] Error: " + t.getMessage());
            }

            @Override
            public void onCompleted() {
                appendResult("[Server Stream] Energy stream complete.");
            }
        });
    }

    private void doAdjustPanel() {
        String panelId = selectedValue(spConfigPanelId);
        float angle;
        try {
            angle = Float.parseFloat(spConfigAngle.getText().trim());
        } catch (NumberFormatException e) {
            appendResult("Error: Invalid angle value.");
            return;
        }

        float finalAngle = angle;
        new SwingWorker<ConfigResponse, Void>() {
            @Override
            protected ConfigResponse doInBackground() {
                return client.adjustPanel(panelId, finalAngle);
            }

            @Override
            protected void done() {
                try {
                    ConfigResponse r = get();
                    appendResult("[Unary] Adjust Config: Success=" + r.getSuccess()
                            + " | Applied Angle: " + String.format("%.1f deg", r.getAppliedAngle())
                            + " | " + r.getMessage());
                } catch (Exception ex) {
                    handleError(ex);
                }
            }
        }.execute();
    }

    private void doUploadBatch() {
        String[] panelIds = spBatchPanelIds.getText().split(",");
        String[] energies = spBatchEnergies.getText().split(",");
        if (panelIds.length != energies.length) {
            appendResult("Error: Number of Panel IDs must match number of Energy values.");
            return;
        }
        List<EnergyBatchEntry> entries = new ArrayList<>();
        try {
            for (int i = 0; i < panelIds.length; i++) {
                entries.add(EnergyBatchEntry.newBuilder()
                        .setPanelId(panelIds[i].trim())
                        .setEnergyKwh(Float.parseFloat(energies[i].trim()))
                        .setTimestamp(java.time.LocalDateTime.now().toString())
                        .build());
            }
        } catch (NumberFormatException e) {
            appendResult("Error: Energy values must be valid numbers.");
            return;
        }

        appendResult("[Client Stream] Uploading energy batch: " + entries.size() + " entries...");
        new SwingWorker<EnergyBatchSummary, Void>() {
            @Override
            protected EnergyBatchSummary doInBackground() throws Exception {
                return client.uploadEnergyBatch(entries);
            }

            @Override
            protected void done() {
                try {
                    EnergyBatchSummary s = get();
                    appendResult("[Client Stream] Batch Summary:"
                            + " Records: " + s.getRecordsReceived()
                            + " | Total: " + String.format("%.2f kWh", s.getTotalEnergyKwh())
                            + " | Avg: " + String.format("%.2f kWh", s.getAverageEnergyKwh())
                            + " | Peak: " + String.format("%.2f kWh", s.getPeakEnergyKwh())
                            + " | " + s.getMessage());
                } catch (Exception ex) {
                    handleError(ex);
                }
            }
        }.execute();
    }

    // Layout Helpers
    private JPanel tabPanel() {
        JPanel p = new JPanel();
        p.setLayout(new BoxLayout(p, BoxLayout.Y_AXIS));
        p.setBackground(BG);
        p.setBorder(new EmptyBorder(10, 15, 10, 15));
        return p;
    }

    private JPanel rowPanel() {
        JPanel row = new JPanel(new FlowLayout(FlowLayout.LEFT, 4, 10));
        row.setBackground(BG);
        row.setMaximumSize(new Dimension(Integer.MAX_VALUE, 55));
        return row;
    }

    private JLabel fixedLabel(String text, int width) {
        JLabel lbl = new JLabel(text);
        lbl.setPreferredSize(new Dimension(width, 25));
        return lbl;
    }

    private JComboBox<String> combo(String[] options) {
        JComboBox<String> cb = new JComboBox<>(options);
        cb.setEditable(true);
        cb.setPreferredSize(new Dimension(160, 25));
        return cb;
    }

    // Gets the current value from a combo box (handles both selected and typed values).
    private String selectedValue(JComboBox<String> cb) {
        Object val = cb.getEditor().getItem();
        return val != null ? val.toString().trim().toUpperCase() : "";
    }

    private Component hgap(int w) {
        return Box.createHorizontalStrut(w);
    }

    private JSeparator sep() {
        JSeparator s = new JSeparator();
        s.setMaximumSize(new Dimension(Integer.MAX_VALUE, 2));
        return s;
    }

    // Utilities
    private void appendResult(String text) {
        SwingUtilities.invokeLater(() -> {
            resultArea.append(text + "\n");
            resultArea.setCaretPosition(resultArea.getDocument().getLength());
        });
    }

    private void handleError(Exception ex) {
        String msg;
        if (ex.getCause() instanceof StatusRuntimeException sre) {
            msg = "gRPC Error [" + sre.getStatus().getCode() + "]: "
                    + sre.getStatus().getDescription();
        } else {
            msg = ex.getMessage() != null ? ex.getMessage() : ex.getClass().getSimpleName();
        }
        appendResult("ERROR: " + msg);
        logger.warning("GUI error: " + msg);
    }

    // Exit & Main
    private void onExit() {
        try {
            client.shutdown();
        } catch (Exception ignored) {
        }
        dispose();
        System.exit(0);
    }

    public static void main(String[] args) {
        SwingUtilities.invokeLater(() -> new ClimateActionGUI().setVisible(true));
    }
}
