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



