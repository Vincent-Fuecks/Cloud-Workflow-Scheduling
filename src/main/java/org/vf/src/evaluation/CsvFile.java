package org.vf.src.evaluation;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

public class CsvFile {

    private BufferedWriter writer;

    public CsvFile(String csvFilePath, List<String> headers) throws IOException {
        this.writer = new BufferedWriter(new FileWriter(csvFilePath, true));
        writeHeader(headers);
    }

    public CsvFile(String csvFilePath) throws IOException {
        this.writer = new BufferedWriter(new FileWriter(csvFilePath, true));
    }

    private void writeHeader(List<String> headers) throws IOException {
        if (headers != null && !headers.isEmpty()) {
            this.writer.write(String.join(",", headers));
            this.writer.newLine();
        }
    }

    public void addNewLine() throws IOException {
        this.writer.newLine();
    }

    public <T> void addRow(List<T> row) throws IOException {
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < row.size(); i++) {
            sb.append(row.get(i));
            if (i < row.size() - 1) {
                sb.append(",");
            }
        }

        this.writer.write(sb.toString());
        writer.newLine();
    }

    public void close() throws IOException {
        if (writer != null) {
            writer.close();
        }
    }
}
