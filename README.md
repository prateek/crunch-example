# Crunch-Example #

This is a sample project demonstrating the use of the `Morphline SDK`, and `Apache Crunch`.

## Sample Input ##
CSV input, compressed using gzip, with the following structure

```csv
id, name, age, salary, years_spent, title, department
```

## Example 1: Find average salary per department ##
This is a facetious example. It is created with to demonstrate the scaffolding required around getting the two frameworks (`Morphlines` and `Crunch` rolling along).

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

## Usage: ##

```sh

$ git clone github.com/prateek/crunch-example/crunch-example.git
$ cd crunch-example
$ mvn package
$ ./setup-data.sh
$ ./run.sh
```
