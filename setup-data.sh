#!/bin/bash

hdfs dfs -rm -r /tmp/employee_test 
hdfs dfs -mkdir -p /tmp/employee_test/employee_records
hdfs dfs -put input-sample/employee_record.csv /tmp/employee_test/employee_records 
