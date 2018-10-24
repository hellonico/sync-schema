import java.io.FileReader;
import java.util.ArrayList;
import java.util.Map;
import java.util.Properties;

import com.opencsv.CSVReaderHeaderAware;

/**
 * Business class to get headers and values from a csv file
 */
public class CSV {
    String csvFile;

    public CSV(String csvFile) {
        this.csvFile = csvFile;
    }

    String[] headers;
    ArrayList<Map<String, String>> list = new ArrayList<Map<String, String>>();

    public String[] getHeaders() {
        if (this.headers == null) {
            readHeaders();
        }
        return this.headers;
    }

    public ArrayList<Map<String, String>> getValues() {
        return list;
    }

    // public void loadCSV(SQL sql) throws Exception {
    // CSVReaderHeaderAware reader = new CSVReaderHeaderAware(new
    // FileReader(csvFile));
    // Map<String, String> values = reader.readMap();
    // this.headers = readHeaders(values);
    // list.add(values);
    // while ((values = reader.readMap()) != null) {
    // list.add(values);
    // }
    // reader.close();
    // }

    public void stream(SQL sql) throws Exception {
        CSVReaderHeaderAware reader = new CSVReaderHeaderAware(new FileReader(csvFile));
        Map<String, String> values;
        while ((values = reader.readMap()) != null) {
            // list.add(values);
            sql.insertOne(values);
        }
        reader.close();
    }

    public String[] readHeaders() {
        try {
            CSVReaderHeaderAware reader = new CSVReaderHeaderAware(new FileReader(csvFile));
            Map<String, String> values = reader.readMap();
            String[] headers = values.entrySet().stream().map(e -> Util.toHeader(e.getKey())).toArray(String[]::new);
            // this.headers = readHeaders(values);
            this.headers = headers;
            reader.close();
            return this.headers;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}