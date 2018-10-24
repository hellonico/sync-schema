
# goal

Import data from csv files to SQL database via JDBC.
On first import, the table will be created as needed. Columns are all the same with datatype string.
On successive imports, the table will be added columns (not removed) to reflect CSV file update.
Column names are stripped of ' ' and '_' and lowercase-d.
Data is streamed from the CSV file, so constant rather low (for java) memory usage.

# to run the basic test

```
./demo.sh
```

Which calls:
```
./gradlew run --args src/test/resources/sync.properties
```

With the sync properties file below:
```
inputFile=src/test/resources/sales.csv
dbURL=jdbc:sqlite:sample.db
tableName=sales
tableSchema=null
password=
username=
```

This will read values from the file 
```
src/test/java/resources/sales.csv
```
and store data in table named **sales** in a newly (or existing) sqllite database: __sample.db__

# programmatically 

```
Properties props = new Properties();
props.load(App.class.getClassLoader().getResourceAsStream("sync.properties"));

App app = new App();
app.execute(props);
```

with a similar property file with the following content
```
inputFile=src/test/resources/sales.csv
dbURL=jdbc:sqlite:sample.db
tableName=sales
# debug=true
```

Will execute the same.
App is not thread safe, but can be reused for a new import.

## oracle 

this runs ok on oracle.  Here is a sample oracle.properties file to use with the sync-er

```
inputFile=src/test/resources/sales.csv
dbURL=jdbc:oracle:thin:@localhost:1521:orcl
tableName=sales
username=username
password=password
columnType=varchar2 (50)
```