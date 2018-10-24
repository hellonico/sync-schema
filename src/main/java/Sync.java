import java.util.Properties;

import com.google.common.collect.Sets;

/**
 * Sync between the given CSV and SQL on the tableName
 */
public class Sync {

    public Sync() {

    }

    public void sync(SQL sql, CSV csv) {
        System.out.printf("Sync data from csv [%s] to table [%s] %n", csv.csvFile, sql.tableName);
        try {
            csv.stream(sql);
        } catch (Exception e) {
            System.out.printf("ERROR: [%s]%n", e.getMessage());
            // throw new RuntimeException(e);
        }
    }

    public void exec(String tableName, CSV csv, SQL sql, Properties props) {

        if (props.containsKey("clean")) {
            sql.drop();
            // return;
        }
        boolean exist = sql.tableExist();
        String[] headers = csv.getHeaders();

        if (!exist) {
            System.out.printf("Creating table %s\n", tableName);
            sql.createTable(headers);
            this.sync(sql, csv);
        } else {

            System.out.printf("Skip creating table %s\n", tableName);

            String[] sqlHeaders = sql.extractHeaders();
            if (props.containsKey("debug")) {
                Util.prettyPrint(sqlHeaders);
                Util.prettyPrint(headers);
            }

            Sets.SetView<String> diff = Util.compareHeaders(headers, sqlHeaders);

            if (diff.size() == 0) {
                System.out.printf("No need for schema update on table [%s] %n", tableName);
            } else {
                System.out.printf("Header diff: %s %n", diff);
                System.out.printf("Running schema sync%n");
                sql.syncSchema(diff);
            }

            this.sync(sql, csv);
        }

        sql.countRows();

    }
}