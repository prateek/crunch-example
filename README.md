# Crunch-Example #

This is a sample project demonstrating the use of the `Kite Morphline SDK`, and `Apache Crunch`.

## Sample Input ##
CSV input, compressed using gzip, with the following structure:

```csv
id, name, age, salary, years_spent, title, department
```

## Example 1: Find average salary per department ##
This is a facetious example. It is created to demonstrate the scaffolding required around getting the two frameworks (`Morphline` and `Crunch`) rolling along.

The first example is a the Crunch equivalent of the following SQL query -

```sql
SELECT
    department
  , AVERAGE( salary)
FROM
  table
GROUP BY
  department
```

The equivalent Crunch code (*sans* the scaffolding Morphline assisted text->Avro conversion, which can be found in `MorphlineDoFn.java`) is provided below. It has minimal boilerplate code, especially when compared to MapReduce code, while still maintaining static typing and extensibility. Sufficed to say, I'm a fan.

```java
    PCollection<String> lines = getPipeline().read( From.textFile( args[0] ) );
    PTable<String, Double> summaries = lines
        .parallelDo( "GenerateAvroRecords",
            new MorphlineDoFn(), Avros.reflects(EmployeeRecord.class) )
        .by("ExtractDepartment",
            new MapFn< EmployeeRecord, String > () {
              public String map( EmployeeRecord record ) {
                return record.getDepartment();
              }
            }, Avros.strings())
        .mapValues( "ExtractSalaries",
            new MapFn< EmployeeRecord, Pair< Double, Long > >() {
              public Pair<Double, Long> map( EmployeeRecord e ) {
                return Pair.of( e.getSalary(), 1L);
              }
            },
            Avros.pairs(Avros.doubles(), Avros.longs()))
        .groupByKey()
        .combineValues(
            Aggregators.pairAggregator( Aggregators.SUM_DOUBLES(), Aggregators.SUM_LONGS()))
        .mapValues( "ComputeAverage",
            new MapFn< Pair< Double, Long >, Double >() {
              public Double map( Pair< Double, Long > p ) {
                return p.first() / p.second();
              }
            },
            Avros.doubles());

    summaries.write(
        At.avroFile(args[1], EmployeeSummary.class), Target.WriteMode.APPEND );
```

## Usage: ##

```sh

$ git clone github.com/prateek/crunch-example.git
$ cd crunch-example
$ mvn package
$ ./setup-data.sh
$ ./run.sh
```
