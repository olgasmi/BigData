// Databricks notebook source
import org.apache.spark.sql.functions._
import java.time.temporal.ChronoUnit._
import org.apache.spark.sql.types._


val filePath = "dbfs:/FileStore/tables/names.csv"
val namesDf = spark.read.format("csv").option("header","true").option("inferSchema","true").load(filePath)

//adding new columns
namesDf.withColumn("time", unix_timestamp()).withColumn("height_in_feet",col("height")/30.48).withColumn("age", year(current_date()) - year(date_format(to_date(col("date_of_birth"), "dd.mm.yyyy"), "yyyy-MM-dd"))).explain()

//removing columns
namesDf.drop("bio", "death_details").explain()

//most popular name
namesDf.withColumn("names", substring_index(col("name"), " ", 1)).groupBy("names").count().orderBy(desc("count")) //John 873

//sorting 
namesDf.orderBy(column("name").asc).explain()

//how to make df out of array of columns ???
val colNames = namesDf.columns.map(name => col(name.replace("_", "").capitalize)) 


// COMMAND ----------

import org.apache.spark.sql.Column

val filePath = "dbfs:/FileStore/tables/movies.csv"
val movies = spark.read.format("csv").option("header","true").option("inferSchema","true").load(filePath)

//years
movies.withColumn("time", unix_timestamp()).withColumn("how_old", abs(col("year")-year(current_date())))
                                                                     
//removing null values
val withoutNull = movies.na.drop()//.explain()

//budget
withoutNull.withColumn("cleanBudget", regexp_replace($"budget", "[^0-9.]","")).explain()



// COMMAND ----------

val filePath = "dbfs:/FileStore/tables/ratings.csv"
val ratings = spark.read.format("csv").option("header","true").option("inferSchema","true").load(filePath)

val df = ratings.na.fill(0)
                        
//subtraction
df.withColumn("subtraction_median", abs($"weighted_average_vote"-$"median_vote")).withColumn("subtraction_mean", abs($"weighted_average_vote"-$"mean_vote"));

//who gives better votes + ex3
val better = ratings.select($"males_allages_avg_vote", $"females_allages_avg_vote")

better.filter(($"males_allages_avg_vote" > $"females_allages_avg_vote")).count()//25117
better.filter(($"males_allages_avg_vote" < $"females_allages_avg_vote")).count() //53656
better.filter(($"males_allages_avg_vote" === $"females_allages_avg_vote")).count() //7000

//females

//casting to long type
ratings.withColumn("mean_vote",col("mean_vote").cast(LongType))//.explain()


//median and mean
display(df.withColumn("mean", $"votes_10"+$"votes_9"+$"votes_8"+$"votes_7"+$"votes_6"+$"votes_5"+$"votes_4"+$"votes_3"+$"votes_2"+$"votes_1"/10))


// COMMAND ----------

//ex2
/*
Jobs Tab 
Wy??wietla stron?? wszytskich job'??w + strony z detalami dla tych job??w. Strona podsumowania - informacje np. status, czas trwania, post??py job'??w, og??lny timeline wydarze??. Klikni??cie na joba umo??liwia wy??wietlenie jego detaili.

Stages Tab, Stage detail
Strona podsumowania wy??wietla stan wszystkich etap??w job??w. Strona zawiera takie elementy jak szczeg????y zdarze?? wzgl??dem statusu, linki umo??liwiaj??ce zako??czenie aktywych zdarze??, je??li zdarzenie nie powiod??o si?? to widoczna jest tego przyczyna.

Stage detail - podsumowanie wszystkich task??w w tabeli i na osi czasu.

Storage Tab - wy??wietla RDDs i DataFrames. Wyr????nia si?? ???summary page??? i ???details page???, kt??re dostarczaj?? podstawowe informacje, takie jak poziom pami??ci, liczba partycji i narzut pami??ci.

Environment Tab - warto??ci dla r????nych ??rodowisk i konfiguracji zmiennych (JVM, Spark, ustawienia systemowe).
Umo??liwia sprawdzenie poprawno????i w??a??ciwo??ci. Podzielone na 5 cz????ci:
1: informacje np. wersja Javy/Scali,
2: w??a??ciowi??ci Spark'a,
3: w??a??ciwo??ci Hadoop'a i YARN'a,
4: pokazuje wi??cej szczego????w JVM,
5: klasy za??adowane z innych ??r??de?? (przydatne przy rozwi??zywaniu konfilkt??w klas)

Executors Tab - dostarcza podsumowanie o executorach (m.in. wykorzystanie pami??ci/dysku/rdzeni, informacje o zadaniach/losowaniu, wydajno????). Za pomoc?? poni??szych element??w mo??na wy??wieli??:
- ????cze 'stderr' executora 0 ?????standardowy dziennik b????d??w,
- link ???Thread Dump??? executora 0 ???  zrzut w??tku JVM na executorze 0

SQL Tab - informacje takie jak czas trwania zapytania, job'y, fizyczne/logiczne plany zapyta??. Wylistowane operatory dataframe/SQL, po naci??nieciu odpowiedniego linku mo??na zobaczy?? DAG i szczeg????y wykonania zapytania.
Query's details' page - informacje na temat czasu wykonania zapytania, jego trwania, lista powi??zanych job'??w i DAG wykoniania zapytania.

Structured Streaming Tab - wy??wietla zwi??z??e statystki dla zapyta?? (tych sko??czonych oraz w trakcie). Mo??na sprawdzi?? ostatni wyj??tek jaki zosta?? wyrzucony przez zapytanie, kt??re si?? nie powiod??o. Strona statystyk zawiera metryki, kt??re pozwalaj?? na sprwadzenie statusu zapytania (np. czas trwania operacji, wiersze wej??ciowe).

Streaming (DStreams) Tab - wy??wietla op????nienie planowania i czas przetwarzania dla ka??dej mikropartii w strumieniu danych.

JDBC/ODBC Server Tab - widoczna podczas dzia??ania Sparka; zawiera informacje o sesjach i przes??anych operacjach SQL. Wyr????nia si?? 3 sekcje:
1: og??lne informacje o serwerze JDBC/ODBC,
2: informacje o aktywnych i zako??czonych sesjach,
3: statystyki SQL przes??anych operacji.*/

// COMMAND ----------

//ex3
//groupBy() - collect the identical data into groups and perform aggregate functions on the grouped data

val filePath = "dbfs:/FileStore/tables/ratings.csv"
val ratings = spark.read.format("csv").option("header","true").option("inferSchema","true").load(filePath)

ratings.select($"males_allages_avg_vote", $"females_allages_avg_vote").explain()
ratings.select($"males_allages_avg_vote", $"females_allages_avg_vote").groupBy("females_allages_avg_vote").count().explain()



// COMMAND ----------

//ex4
var username = "sqladmin"
var password = "$3bFHs56&o123$" 

val dataFromSqlServer = sqlContext.read
      .format("jdbc")
      .option("driver" , "com.microsoft.sqlserver.jdbc.SQLServerDriver")
      .option("url", "jdbc:sqlserver://bidevtestserver.database.windows.net:1433;database=testdb")
      .option("dbtable", "(SELECT * FROM information_schema.tables) tmp")
      .option("user", username)
      .option("password",password)
      .load()

display(dataFromSqlServer)
