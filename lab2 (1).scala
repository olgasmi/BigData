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
Wyświetla stronę wszytskich job'ów + strony z detalami dla tych jobów. Strona podsumowania - informacje np. status, czas trwania, postępy job'ów, ogólny timeline wydarzeń. Kliknięcie na joba umożliwia wyświetlenie jego detaili.

Stages Tab, Stage detail
Strona podsumowania wyświetla stan wszystkich etapów jobów. Strona zawiera takie elementy jak szczegóły zdarzeń względem statusu, linki umożliwiające zakończenie aktywych zdarzeń, jeśli zdarzenie nie powiodło się to widoczna jest tego przyczyna.

Stage detail - podsumowanie wszystkich tasków w tabeli i na osi czasu.

Storage Tab - wyświetla RDDs i DataFrames. Wyróżnia się “summary page” i “details page”, które dostarczają podstawowe informacje, takie jak poziom pamięci, liczba partycji i narzut pamięci.

Environment Tab - wartości dla różnych środowisk i konfiguracji zmiennych (JVM, Spark, ustawienia systemowe).
Umożliwia sprawdzenie poprawnośći właściwości. Podzielone na 5 części:
1: informacje np. wersja Javy/Scali,
2: właściowiści Spark'a,
3: właściwości Hadoop'a i YARN'a,
4: pokazuje więcej szczegołów JVM,
5: klasy załadowane z innych źródeł (przydatne przy rozwiązywaniu konfilktów klas)

Executors Tab - dostarcza podsumowanie o executorach (m.in. wykorzystanie pamięci/dysku/rdzeni, informacje o zadaniach/losowaniu, wydajność). Za pomocą poniższych elementów można wyświelić:
- łącze 'stderr' executora 0 → standardowy dziennik błędów,
- link „Thread Dump” executora 0 →  zrzut wątku JVM na executorze 0

SQL Tab - informacje takie jak czas trwania zapytania, job'y, fizyczne/logiczne plany zapytań. Wylistowane operatory dataframe/SQL, po naciśnieciu odpowiedniego linku można zobaczyć DAG i szczegóły wykonania zapytania.
Query's details' page - informacje na temat czasu wykonania zapytania, jego trwania, lista powiązanych job'ów i DAG wykoniania zapytania.

Structured Streaming Tab - wyświetla zwięzłe statystki dla zapytań (tych skończonych oraz w trakcie). Można sprawdzić ostatni wyjątek jaki został wyrzucony przez zapytanie, które się nie powiodło. Strona statystyk zawiera metryki, które pozwalają na sprwadzenie statusu zapytania (np. czas trwania operacji, wiersze wejściowe).

Streaming (DStreams) Tab - wyświetla opóźnienie planowania i czas przetwarzania dla każdej mikropartii w strumieniu danych.

JDBC/ODBC Server Tab - widoczna podczas działania Sparka; zawiera informacje o sesjach i przesłanych operacjach SQL. Wyróżnia się 3 sekcje:
1: ogólne informacje o serwerze JDBC/ODBC,
2: informacje o aktywnych i zakończonych sesjach,
3: statystyki SQL przesłanych operacji.*/

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
