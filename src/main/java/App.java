import java.util.Properties;
import java.io.FileInputStream;
import java.io.InputStream;

/**
 * Entry point of the package. Get the settings from sync.properties and then
 * run the sync from CSV to SQL
 */
public class App {

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.out.printf("Usage: App <path_to_sync_properties_file> %n");
            System.exit(1);
        }
        Properties props = new Properties();
        String propFile = args[0];
        System.out.printf("Using sync file: %s %n", propFile);
        InputStream as = new FileInputStream(propFile);
        props.load(as);

        App app = new App();
        if (args.length == 2)
            app.clean(props);
        else
            app.execute(props);
    }

    public void execute(Properties props) throws Exception {
        CSV csv = new CSV(props.getProperty("inputFile"));
        SQL sql = new SQL(props.getProperty("dbURL"), props);
        String tableName = props.getProperty("tableName");
        Sync sync = new Sync();
        sync.exec(tableName, csv, sql, props);
    }

    public void clean(Properties props) throws Exception {
        SQL sql = new SQL(props.getProperty("dbURL"), props);
        sql.drop();
    }
}
