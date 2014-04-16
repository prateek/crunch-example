#!/bin/bash

hdfs dfs -rm -r /tmp/employee_test &&                                       \
mvn kite:run-tool -Dkite.args="repo:hdfs:/tmp/employee_test" &&             \
hdfs dfs -put input-sample/input.csv /tmp/employee_test/employee_records && \
mvn kite:run-tool -Dkite.args="repo:hdfs:/tmp/employee_test"

