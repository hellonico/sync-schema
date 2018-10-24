import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import com.google.common.collect.Sets;

/**
 * Business class to manage schema and insert values into a database table
 */
public class SQL {
    private Connection connection;
    private Properties properties;
    String tableName;
    private PreparedStatement prepared;
    boolean debug;

    public SQL(String url, Properties props) {
        try {
            System.out.printf("Connection to DB [%s]%n", url);
            this.connection = DriverManager.getConnection(url, props.getProperty("username"),
                    props.getProperty("password"));
            this.properties = props;
            this.debug = properties.containsKey("debug");
            this.tableName = props.getProperty("tableName");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void drop() {
        if (!tableExist())
            return;

        System.out.printf("Drop table [%s]%n", this.tableName);
        Statement statement = null;
        try {
            statement = connection.createStatement();
            statement.executeUpdate(String.format("drop table %s", tableName));
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            closeSilently(statement);
        }
    }

    String getColumnType() {
        return (String) properties.getOrDefault("columnType", "string");
        // return JDBCType.VARCHAR.getName() + " (50)";
    }

    public void createTable(String[] headers) {
        Statement statement = null;
        try {
            statement = connection.createStatement();
            String colType = getColumnType();
            String columns = Arrays.stream(headers).map(e -> {
                if (e.length() > 30) {
                    // TODO: debug on oracle
                    System.out.println("[" + e.length() + "]:" + e);
                }
                return sqlHeader(e) + " " + colType + " ";
            }).collect(Collectors.joining(", "));
            String createStatement = String.format("create table %s ( %s )", tableName, columns);
            System.out.printf("SQL:\t %s%n", createStatement);
            statement.executeUpdate(createStatement);
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            closeSilently(statement);
        }
    }

    static String sqlHeader(String e) {
        return "\"" + Util.toHeader(e) + "\"";
    }

    public void syncSchema(Sets.SetView<String> missings) {
        try {
            final Statement statement = connection.createStatement();
            missings.forEach(m -> {
                String sql = String.format("ALTER TABLE %s ADD %s %s ", tableName, sqlHeader(m), getColumnType());
                try {
                    statement.executeUpdate(sql);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        } catch (Exception e) {

        } finally {

        }

    }

    public void insertOne(Map<String, String> vals) {
        String columnsAsString = vals.keySet().stream().map(e -> sqlHeader(e)).collect(Collectors.joining(","));
        String preparedAsString = vals.values().stream().map(e -> "?").collect(Collectors.joining(","));
        String sql = String.format("insert into %s ( %s ) values ( %s )", tableName, columnsAsString, preparedAsString);
        int i = 0;
        try {
            if (prepared == null) {
                this.prepared = connection.prepareStatement(sql);
            }
            String[] toset = vals.values().toArray(new String[vals.size()]);
            for (i = 0; i < toset.length; i++) {
                prepared.setString(i + 1, toset[i]);
            }
            // prepared.addBatch();
            prepared.executeUpdate();
        } catch (Exception e) {
            System.out.printf("Failed SQL: %n [%s] %n [%s] [i=%d] %n", sql, e.getMessage(), i);
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    // public void batch() {
    // prepared.executeUpdate();
    // }

    // public void sync(String[] headers, ArrayList<Map<String, String>> values) {
    // Statement statement = null;
    // String sql = "";
    // try {
    // Iterator<Map<String, String>> iter = values.iterator();
    // while (iter.hasNext()) {
    // Map<String, String> vals = iter.next();
    // insertOne(vals);
    // }
    // } catch (Exception e) {
    // System.out.println(sql);
    // e.printStackTrace();
    // } finally {
    // closeSilently(statement);
    // }
    // }

    public String[] extractHeaders() {
        ResultSet rs = null;
        try {
            rs = connection.createStatement().executeQuery(String.format("select * from %s", tableName));
            ResultSetMetaData rsm = rs.getMetaData();
            int colCount = rsm.getColumnCount();
            ArrayList<String> list = new ArrayList<String>(colCount);
            for (int i = 1; i <= colCount; i++) {
                list.add(rsm.getColumnName(i));
            }
            String[] hh = list.toArray(new String[colCount]);
            return hh;
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            closeSilently(rs);
        }
    }

    private void closeSilently(ResultSet rs) {
        if (rs != null)
            try {
                rs.close();
            } catch (Exception e) {

            }
    }

    private void closeSilently(Statement s) {
        if (s != null)
            try {
                s.close();
            } catch (Exception e) {

            }
    }

    /**
     * https://docs.microsoft.com/en-us/sql/connect/jdbc/reference/gettables-method-sqlserverdatabasemetadata?view=sql-server-2017
     */
    public boolean tableExist() {
        ResultSet rs = null;
        System.out.printf("Looking for table [%s] %n", tableName);
        try {
            rs = connection.getMetaData().getTables(null, null, tableName, null);
            if (rs.next()) {
                System.out.printf("Table [%s] exist: %b \n", tableName, true);
                return true;
            }
            closeSilently(rs);
            rs = connection.getMetaData().getTables(null, null, tableName.toUpperCase(), null);
            if (rs.next()) {
                System.out.printf("Table [%s] exist: %b \n", tableName, true);
                return true;
            }
            System.out.printf("Table [%s] exist: %b\n", tableName, false);
            return false;
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            closeSilently(rs);
        }
    }

    public void countRows() {
        ResultSet rs = null;
        try {
            rs = connection.createStatement().executeQuery(String.format("select count(*) from %s", tableName));
            if (rs.next()) {
                int rowCount = rs.getInt(1);
                System.out.printf("ROWS COUNT: %d", rowCount);
            } else {
                System.out.printf("NO ROWS COUNT INFO");
            }
        } catch (Exception e) {
            e.printStackTrace();
            closeSilently(rs);
        }
    }

}