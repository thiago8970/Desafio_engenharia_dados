package handlers


import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, regexp_extract, sum}


class DataProcessing ()   {

  def structureDataFrame(df: DataFrame): DataFrame = {


    df.withColumn("Host", regexp_extract(df("value"), "^([^\\s]+\\s)", 1))
      .withColumn("TimeStamp", regexp_extract(df("value"), "^.*\\[(\\d\\d/\\w{3}/\\d{4}:\\d{2}:\\d{2}:\\d{2} -\\d{4})]", 1))
      .withColumn("Requisicao", regexp_extract(df("value"), "^.*\"\\w+\\s+([^\\s]+)\\s+HTTP.*\"", 1))
      .withColumn("Codigo", regexp_extract(df("value"), "^.*\"\\s+([^\\s]+)", 1))
      .withColumn("Tamanho", regexp_extract(df("value"), "^.*\\s+(\\d+)$", 1))
      .withColumn("Date", regexp_extract(df("value"), ".*\\[(\\d\\d/\\w{3}/\\d{4}:\\d{2}:\\d{2}:\\d{2})", 1))

  }

  def getUniqueHosts (df:DataFrame): Long = {

   val hosts = df.select( col("Host"))
      .distinct()

    hosts.count()
  }

  def errorsCount (df: DataFrame, errorCode: Int): Long = {

    val errors = df.where(col("Codigo") === errorCode)

   errors.count()

  }

  def mostCausedErrors (df: DataFrame, errorCode: Int): DataFrame = {
    df.filter(col("Codigo") === errorCode)
      .groupBy("Host")
      .count()
      .sort(col("count").desc)

  }

  def numberErrorsDay (df: DataFrame, file: DataFrame, errorCode: Int): DataFrame = {

    df.filter(col("Codigo") === errorCode)
      .withColumn("Dia", regexp_extract(file("value"), ".*\\[(\\d\\d)", 1))
      .groupBy(col("Dia")).count().sort(col("Dia").asc)
  }

  def totalBytes (df: DataFrame): DataFrame = {

    df.select(col("Tamanho"))
      .agg(sum("Tamanho"))

  }
}
