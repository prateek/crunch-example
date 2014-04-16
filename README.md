# Crunch-Example #

This is a sample project demonstrating the use of the `Kite SDK`, and `Apache Crunch`.

## Sample Input ##
CSV input, compressed using gzip, with the following structure

```csv
id, name, age, salary, years_spent, title, department
```

## Example 1: Find average salary per department ##
This is a facetious example. It is created with to demonstrate the scaffolding required around getting the two frameworks (`Kite` and `Crunch` rolling along).

The equivalent SQL query for the computation being performed -

```sql
SELECT
    department
  , AVERAGE( salary)
FROM
  table
GROUP BY
  department
```



#### Notes:####
* Need to add the lines below to the mvn kite pom to connect to the cluster

```xml
<hadoopConfiguration>
  <property>
    <name>fs.default.name</name>
    <value>hdfs://localhost</value>
  </property>
  <property>
    <name>hive.metastore.uris</name>
    <value>thrift://localhost:9083</value>
  </property>
</hadoopConfiguration>
```

* Generate the datasets

```sh
mvn kite:create-dataset                                         \
-Dkite.datasetName=employee_records                             \
-Dkite.avroSchemaFile=src/resources/schema/employee_record.avsc \
-Dkite.rootDirectory=/tmp/employee_records
```

* Generate the schema classes -
```sh
java -jar avro-tools-1.7.6.jar compile schema \
  src/resources/schema/employee_summary.avsc src/java
```

* FIXME: needed to add `src/main/resources/core-site.xml` to get the Kite tool to pickup HDFS locations
