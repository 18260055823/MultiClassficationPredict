package TreeDecision

import java.io.{FileNotFoundException, IOException}
import java.sql.SQLException
import java.util.Properties

import org.apache.spark.sql
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}


private[TreeDecision] class InputAndOutput() {

  private[this] var spark: SparkSession = null

  def setSpark(spark: SparkSession) = {
    if (spark.isInstanceOf[SparkSession]) {
      this.spark = spark
    } else {
      println("环境错误")
    }
  }

  def getSpark() = {
    this.spark
  }

  def getEnvironment(appName: String, master: String) = {
    val spark = SparkSession.builder().appName(appName).master(master).getOrCreate()
    setSpark(spark)
  }

  def getCsvFile(url: String) = {
    var cf: sql.DataFrame = null
    try {
      cf = spark.read.option("header", "true").csv(url)
    } catch {
      case e: FileNotFoundException => println("file not found")
      case ex: IOException => println("IO Exception")
    }
    cf
  }

  def getTextFile(url: String) = {
    var tf: sql.Dataset[String] = null
    try {
      tf = spark.read.option("header", "true").textFile(url)
    } catch {
      case e: FileNotFoundException => println("file not found")
      case ex: IOException => println("found IO Exception")
    }
    tf
  }

  def dataToMysql(spark: SparkSession, predictResult: Any) = {
    val myEnm = new MyEnm()
    val properties = new Properties()
    properties.put("user", myEnm.user)
    properties.put("password", myEnm.password)
    properties.put("driver", myEnm.driver)
    val url = myEnm.mysqlUrl
    val table = myEnm.tableName
    try {
      val schema = StructType {
        List(
          StructField("predictR", DoubleType, true)
        )
      }
      val pRdd = spark.sparkContext.makeRDD(Array(predictResult.asInstanceOf[Double]))
      val pRdd1 = pRdd.map(row => {
        Row(row)
      })
      spark.createDataFrame(pRdd1, schema).write.mode(SaveMode.Overwrite).jdbc(url, table, properties)
    } catch {
      case e: SQLException => e.printStackTrace()
    }
  }
}
