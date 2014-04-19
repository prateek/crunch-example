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
