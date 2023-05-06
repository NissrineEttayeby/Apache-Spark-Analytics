// Databricks notebook source
// MAGIC %md ## World Development Indicators Project

// COMMAND ----------

// DBTITLE 1,Country Data - Data frame Definition
// MAGIC %scala 
// MAGIC
// MAGIC val country = sqlContext.read.format("csv")
// MAGIC   .option("header", "true")
// MAGIC   .option("inferSchema", "true")
// MAGIC   .load("/FileStore/tables/Country-3.csv")
// MAGIC
// MAGIC display(country)

// COMMAND ----------

// DBTITLE 1,Create Temp View of Country Data
// MAGIC %scala
// MAGIC
// MAGIC country.createOrReplaceTempView("country")

// COMMAND ----------

// MAGIC %scala
// MAGIC country.printSchema()

// COMMAND ----------

// MAGIC %sql 
// MAGIC select * from country;

// COMMAND ----------

// DBTITLE 1,Indicators Data - Data frame Definition
// MAGIC %scala 
// MAGIC
// MAGIC val Indicators = sqlContext.read.format("csv")
// MAGIC   .option("header", "true")
// MAGIC   .option("inferSchema", "true")
// MAGIC   .load("/FileStore/tables/Indicators.csv")
// MAGIC
// MAGIC display(Indicators)

// COMMAND ----------

// DBTITLE 1,Create Temp View on Indicators Data
// MAGIC %scala
// MAGIC Indicators.createOrReplaceTempView("Indicators")

// COMMAND ----------

// DBTITLE 1,What is the Gini Index?
// MAGIC %md
// MAGIC
// MAGIC The Gini index is a simple measure of the distribution of income across income percentiles in a population.
// MAGIC A higher Gini index indicates greater inequality, with high income individuals receiving much larger percentages of the total income of the population.

// COMMAND ----------

// DBTITLE 1,GINI Index of China
// MAGIC %sql
// MAGIC
// MAGIC   select Year,Value from Indicators where IndicatorCode  = "SI.POV.GINI" and CountryCode = "CHN" order by Year asc;

// COMMAND ----------

// DBTITLE 1,GINI Index of Argentina
// MAGIC %sql
// MAGIC
// MAGIC select Year,Value from Indicators where IndicatorCode  = "SI.POV.GINI" and CountryCode = "ARG" order by Year asc;

// COMMAND ----------

// DBTITLE 1,GINI Index of Uganda
// MAGIC %sql
// MAGIC
// MAGIC select Year,Value from Indicators where IndicatorCode  = "SI.POV.GINI" and CountryCode = "UGA" order by Year asc;

// COMMAND ----------

// DBTITLE 1,GINI Index of USA
// MAGIC %sql
// MAGIC
// MAGIC select Year,Value from Indicators where IndicatorCode  = "SI.POV.GINI" and CountryCode = "USA" order by Year asc;

// COMMAND ----------

// DBTITLE 1,GINI Index of Colombia
// MAGIC %sql 
// MAGIC
// MAGIC select Year,Value from Indicators where IndicatorCode  = "SI.POV.GINI" and CountryCode = "COL" order by Year asc;

// COMMAND ----------

// DBTITLE 1,GINI Index of Rwanda
// MAGIC %sql
// MAGIC
// MAGIC select Year,Value from Indicators where IndicatorCode  = "SI.POV.GINI" and CountryCode = "RWA" order by Year asc;

// COMMAND ----------

// DBTITLE 1,GINI Index of Russia
// MAGIC %sql
// MAGIC
// MAGIC select Year,Value from Indicators where IndicatorCode  = "SI.POV.GINI" and CountryCode = "RUS" order by Year asc;

// COMMAND ----------

// DBTITLE 1,GINI Index of Ecuador
// MAGIC %sql
// MAGIC
// MAGIC select Year,Value from Indicators where IndicatorCode  = "SI.POV.GINI" and CountryCode = "ECU" order by Year asc;

// COMMAND ----------

// DBTITLE 1,GINI Index of Central African Republic
// MAGIC %sql
// MAGIC
// MAGIC select Year,Value from Indicators where IndicatorCode  = "SI.POV.GINI" and CountryCode = "CAF" order by Year asc;

// COMMAND ----------

// DBTITLE 1,Barplot of Youth Literacy Rate in 1990
// MAGIC %sql 
// MAGIC
// MAGIC select value as Youth_Literacy_Rate,ShortName from Indicators A JOIN Country N ON A.CountryCode = N.CountryCode  where IndicatorCode  = "SE.ADT.1524.LT.ZS" and Year = 1990 order by Youth_Literacy_Rate Desc; 

// COMMAND ----------

// DBTITLE 1,Barplot of Youth Literacy Rate in 2010
// MAGIC %sql 
// MAGIC
// MAGIC select value as Youth_Literacy_Rate,ShortName from Indicators A JOIN Country N ON A.CountryCode = N.CountryCode  where IndicatorCode  = "SE.ADT.1524.LT.ZS" and Year = 2010 order by Youth_Literacy_Rate Desc; 

// COMMAND ----------

// DBTITLE 1,Trade as a percentage of GDP for China and India
// MAGIC %sql
// MAGIC
// MAGIC select Value,Year,CountryCode  from Indicators where IndicatorCode = "NE.TRD.GNFS.ZS" and CountryCode in ("IND", "CHN") order by Year;

// COMMAND ----------

// DBTITLE 1,Exports of goods and services (constant 2005 US$)
// MAGIC %sql
// MAGIC
// MAGIC select Value,Year,CountryCode  from Indicators where IndicatorCode = "NE.EXP.GNFS.KD" and CountryCode in ("IND", "CHN") order by Year;

// COMMAND ----------

// DBTITLE 1,Import of goods and services (constant 2005 US$)
// MAGIC %sql
// MAGIC
// MAGIC select Value,Year,CountryCode  from Indicators where IndicatorCode = "NE.IMP.GNFS.KD" and CountryCode in ("IND", "CHN") order by Year;

// COMMAND ----------

// DBTITLE 1,GDP per capita (adjusted by purchasing power parity)
// MAGIC %sql
// MAGIC
// MAGIC select Value,Year,CountryCode  from Indicators where IndicatorCode = "NY.GDP.PCAP.PP.KD" and CountryCode in ("IND", "CHN") order by Year;

// COMMAND ----------

// DBTITLE 1,Poverty Alleviation
// MAGIC %sql
// MAGIC
// MAGIC select Value,Year,CountryCode  from Indicators where IndicatorCode = "SI.POV.2DAY" and CountryCode in ("IND", "CHN") order by Year;

// COMMAND ----------

// DBTITLE 1,Life Expectancy at birth, total (years)
// MAGIC %sql
// MAGIC
// MAGIC select Value,Year,CountryCode  from Indicators where IndicatorCode = "SP.DYN.LE00.IN" and CountryCode in ("IND", "CHN") order by Year;

// COMMAND ----------

// DBTITLE 1,Urban Population growth
// MAGIC %sql
// MAGIC
// MAGIC select Value,Year,CountryCode  from Indicators where IndicatorCode = "SP.URB.TOTL.IN.ZS" and CountryCode in ("IND", "CHN") order by Year;

// COMMAND ----------

// DBTITLE 1,Infant Mortality - as a measure of health care
// MAGIC %sql
// MAGIC
// MAGIC select Value,Year,CountryCode  from Indicators where IndicatorCode = "SH.DTH.IMRT" and CountryCode in ("IND", "CHN") order by Year;

// COMMAND ----------

// DBTITLE 1,The 10 Countries with Lowest Average Income in 1962
// MAGIC %sql
// MAGIC
// MAGIC select CountryName,Value from Indicators where IndicatorCode in ("NY.GNP.PCAP.CD") and Year = 1962 order by Value asc limit 10;

// COMMAND ----------

// DBTITLE 1,The 10 Countries with Lowest Average Income in 2014
// MAGIC %sql
// MAGIC
// MAGIC select CountryName,Value from Indicators where IndicatorCode in ("NY.GNP.PCAP.CD") and Year = 2014 order by Value asc limit 10;

// COMMAND ----------

// DBTITLE 1,The 10 Countries with Highest Average Income in 1962
// MAGIC %sql
// MAGIC
// MAGIC select CountryName,Value from Indicators where IndicatorCode in ("NY.GNP.PCAP.CD") and Year = 1962 order by Value desc limit 10;

// COMMAND ----------

// DBTITLE 1,The 10 Countries with Highest Average Income in 2014
// MAGIC %sql
// MAGIC
// MAGIC select CountryName,Value from Indicators where IndicatorCode in ("NY.GNP.PCAP.CD") and Year = 2014 order by Value desc limit 10;

// COMMAND ----------

// DBTITLE 1,Average Income from 1960-2014 in Rich Countries
// MAGIC %sql
// MAGIC
// MAGIC select CountryName,Value,Year from Indicators where IndicatorCode in ("NY.GNP.PCAP.CD") and CountryName in("Australia","Austria","Canada", "Luxembourg", "Netherlands","Norway","United States") 

// COMMAND ----------

// DBTITLE 1,Average Income from 1960-2014 in Poor Countries
// MAGIC %sql
// MAGIC
// MAGIC select CountryName,Value,Year from Indicators where IndicatorCode in ("NY.GNP.PCAP.CD") and CountryName in("Burundi","Togo","Malawi","Central African Republic") 

// COMMAND ----------

// DBTITLE 1,Average Income in 1962
// MAGIC %sql
// MAGIC
// MAGIC select CountryName,Value,Year from Indicators where IndicatorCode in ("NY.GNP.PCAP.CD") and Year = 1962 and CountryName in ("Malawi","China","Luxembourg","United States") order by Value asc;

// COMMAND ----------

// DBTITLE 1,Average Income in 2014
// MAGIC %sql
// MAGIC
// MAGIC select CountryName,Value,Year from Indicators where IndicatorCode in ("NY.GNP.PCAP.CD") and Year = 2014 and CountryName in ("Malawi","China","Luxembourg","United States") order by Value asc;

// COMMAND ----------

// DBTITLE 1,Life Expectancy in France 1960-2013
// MAGIC %sql 
// MAGIC SELECT year,Value
// MAGIC     FROM Indicators
// MAGIC     WHERE CountryName = "France"
// MAGIC     AND IndicatorCode = "SP.DYN.LE00.IN";

// COMMAND ----------

// DBTITLE 1,G-7 Country Birth Rates 1960-2013
// MAGIC %sql
// MAGIC
// MAGIC SELECT CountryName,IndicatorName,IndicatorCode,Year,Value FROM Indicators WHERE IndicatorCode = 'SP.DYN.CBRT.IN' AND CountryName IN ('Canada','France','Germany', 'United Kingdom', 'Italy', 'Japan','United States')

// COMMAND ----------

// DBTITLE 1,World Per Capita Income in 2013
// MAGIC %sql 
// MAGIC SELECT countrycode,int(value) FROM Indicators WHERE Year = 2013 AND IndicatorCode = 'NY.ADJ.NNTY.PC.KD' order by value desc limit 250;

// COMMAND ----------


