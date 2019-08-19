package apps.batch

import handlers.DataProcessing
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import spark.Spark

object LogsNasa {

  def main(args: Array[String]): Unit = {

    val errorCode = 404

    Spark.setLogLevel(Level.WARN)

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val ss: SparkSession = Spark.getSession
    val sc = Spark.getContext
    val sqlContext = ss.sqlContext

    val file1 = sqlContext.read.text("logs\\NASA_access_log_Jul95\\access_log_Jul95") // DataFrame contendo Logs de Julho

    val file2 = sqlContext.read.text("logs\\NASA_access_log_Aug95\\access_log_Aug95") // DataFrame contendo Logs de Agosto


    val DataProcessing: DataProcessing = new DataProcessing()

    val df1 = DataProcessing.structureDataFrame(file1).cache()
    println("July Dataframe")
    df1.show(10,truncate = false)

    val df2 = DataProcessing.structureDataFrame(file2).cache()
    println("August Dataframe")
    df2.show(10,truncate = false)



    //Exercicio 1 Hosts unicos
    val uniqueHostsJuly =
      DataProcessing.getUniqueHosts(df1)

    print("July Unique Hosts: ")
    println(uniqueHostsJuly)

    val uniqueHostsAugust =
      DataProcessing.getUniqueHosts(df2)

    print("August Unique Hosts: ")
    println(uniqueHostsAugust)


    //Exercicio 2 - Total de erros 404

    val errorsJuly = DataProcessing.errorsCount(df1, errorCode)
    print("Number of 404 errors - July: ")
    println(errorsJuly)

    val errorsAugust= DataProcessing.errorsCount(df2, errorCode)
    print("Number of 404 errors - August: ")
    println(errorsAugust)


    //Exercicio 3 - 5 URLS que mais causaram erro 404

    val most404errorsJuly = DataProcessing.mostCausedErrors(df1, errorCode)
    println("URLs most caused 404 errors - July: ")
    most404errorsJuly.show(5, truncate = false)

    val most404errorsAugust = DataProcessing.mostCausedErrors(df2, errorCode)
    println("URLs most caused 404 errors - August: ")
    most404errorsAugust.show(5, truncate = false)


    //Exercicio 4 - Quantidade de erros 404 por dia

    val errorsperDayJuly = DataProcessing.numberErrorsDay(df1, file1, errorCode)
    println("errors per day - July: ")
    errorsperDayJuly.show(40, truncate = false)

    val errorsperDayAugust = DataProcessing.numberErrorsDay(df2, file2, errorCode)
    println("errors per day - August: ")
    errorsperDayAugust.show(40, truncate = false)



    //Exercicio 5 - Total de bytes retornados

    val totalBytesJuly = DataProcessing.totalBytes(df1)
    println("total of Bytes - July: ")
    totalBytesJuly.show(truncate = false)

    val totalBytesAugust = DataProcessing.totalBytes(df2)
    println("total of Bytes - August: ")
    totalBytesAugust.show(truncate = false)



  }
}
