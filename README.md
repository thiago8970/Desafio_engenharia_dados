# Desafio_engenharia_dados
Desafio Engenharia de Dados - Spark Scala

O código foi desenvolvido na linguagem Scala, com as dependências geradas utilizando o arquivo pom.xml geradas através do Maven.

Os arquivos de logs devem estar no mesmo diretório do projeto dentro de uma pasta chamada "logs".

Bibliotecas utilizadas no projeto:

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, regexp_extract, sum}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

