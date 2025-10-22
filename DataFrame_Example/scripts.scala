val df=spark.read.option("delimiter",",").option("path/to/csv/Fire_Incidents_20251022.csv").withColumn("Incident Date",to_date(col("Incident Date"),"yyyy/MM/dd")).withColumn("year",year(col("Incident Date"))).withColumn("month",month(col("Incident Date")));

val df_cache=df.where(col("year")===2018);
df_cache.cache();// since we are reusing for multiple queries.

// Query to get types of fire calls in 2018
val types_of_fire_calls_2018=df_cache.select("Primary Situation").distinct().count();

// Query to get the month(s) with max fire calls in 2018
val count_fire_call_per_month_in_2018 = df_cache.groupBy("month").agg(count("Call Number").alias("Count of calls"));

val max_agg_df = count_fire_call_per_month_in_2018.agg(max(col("Count of calls")).alias("max_calls")).select("max_calls");

val max_fire_call_in_a_month_2018 = max_agg_df.collect()(0).getAs[Long]("max_calls");


val months_with_highest_calls_in_2018 = count_fire_call_per_month_in_2018.filter(col("Count of calls") === max_fire_call_in_a_month_2018).select("month").collect();
val monthMapping = Map(
  1 -> "January", 2 -> "February", 3 -> "March",4 -> "April",
  5 -> "May", 6 -> "June", "7" -> "July", 8 -> "August",
  9 -> "September", 10 -> "October", 11 -> "November", 12 -> "December"
);


// print statements
println(s"Number of different fire calls in 2018: $types_of_fire_calls_2018");
println(s"Month(s) with $max_fire_call_in_a_month_2018 (max value) fire calls in 2018: "+months_with_highest_calls_in_2018.map(m=>monthMapping(m.getAs[Int]("month"))).mkString(", "));