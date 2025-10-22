### Getting Started with Scala Spark and Dataframes

This demo is just to get some hands on experience working with scala and dataframes.
#### 1. Run Spark Shell

To run the Spark shell in Scala, use the following command:

```bash
$SPARK_HOME/bin/spark-shell
```

#### 2. Download Dataset

Download the CSV file from the DataSF Open Data Portal:
[Fire Incidents Dataset](https://data.sfgov.org/Public-Safety/Fire-Incidents/wr8u-xric/about_data)

#### 3. Load Script to Spark Shell

* To load the `scripts.scala` file into the Spark shell, use:

```bash
:load path/to/scripts.scala
```

* The `scripts.scala` file currently runs the following queries:

  1. **Query 1**: Get the number of different types of fire calls in 2018.
  2. **Query 2**: Find the month(s) that had the maximum number of fire calls.

#### 4. Further Experimentation

You can further experiment with additional queries, such as:

1. **Query 3**: Find the neighborhood district with the least number of fire calls.
2. **Query 4**: Identify the neighborhood with the worst response time to fire calls in 2019.


