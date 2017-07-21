package utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class InputStreamReaderRunnable implements Runnable {

    private BufferedReader reader;

    private StringBuilder output;

    private String name;

    public InputStreamReaderRunnable(InputStream is, String name) {
        this.reader = new BufferedReader(new InputStreamReader(is));
        this.name = name;
        this.output = new StringBuilder();
    }

    public String getOutput() {
        return this.output.toString();
    }

    public void run() {
        try {
            String line = reader.readLine();
            while (line != null) {
                // TODO: Uncomment it if you want output on console
                // System.out.println(line);
                this.output.append(line).append("\n");
                line = reader.readLine();
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}