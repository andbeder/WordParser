package com.mcic.analytics.wavemetadata;

import java.io.*;
import java.util.Arrays;
import java.util.zip.GZIPOutputStream;
import java.util.Base64;

public class DatasetBuilder {
    private final String[] headers;
    private final File tempFile;
    private BufferedWriter writer;
    private int encodedSize;

    public DatasetBuilder(String... headers) throws IOException {
        this.headers = headers;
        tempFile = File.createTempFile("dataset", ".csv");
        writer = new BufferedWriter(new FileWriter(tempFile));
        writeRow(headers);
        encodedSize = 0;
    }

    public void addRecord(String... row) throws IOException {
        if (row.length != headers.length) {
            throw new IllegalArgumentException("Row length does not match headers");
        }
        writeRow(row);
    }

    public void finish() throws IOException {
        if (writer != null) {
            writer.close();
            writer = null;
        }
    }

    public BufferedReader build() throws IOException {
        finish();

        // Compress CSV
        File compressed = File.createTempFile("dataset_compressed", ".gz");
        try (FileInputStream in = new FileInputStream(tempFile);
             GZIPOutputStream out = new GZIPOutputStream(new FileOutputStream(compressed))) {
            byte[] buf = new byte[8192];
            int len;
            while ((len = in.read(buf)) != -1) {
                out.write(buf, 0, len);
            }
        }

        // Base64-encode compressed file
        Base64.Encoder encoder = Base64.getEncoder();
        File encoded = File.createTempFile("dataset_encoded", ".txt");
        try (FileInputStream in2 = new FileInputStream(compressed);
             BufferedWriter bw = new BufferedWriter(new FileWriter(encoded))) {
            byte[] buf = new byte[8192];
            int len;
            while ((len = in2.read(buf)) != -1) {
                String data = encoder.encodeToString(Arrays.copyOf(buf, len));
                bw.write(data);
                encodedSize += data.length();
            }
        }

        return new BufferedReader(new FileReader(encoded));
    }

    public int getEncodedSize() {
        return encodedSize;
    }

    private void writeRow(String... row) throws IOException {
        writer.write(String.join(",", row));
        writer.newLine();
    }
}
