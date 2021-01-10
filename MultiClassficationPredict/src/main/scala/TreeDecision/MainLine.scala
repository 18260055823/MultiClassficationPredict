package TreeDecision

import org.apache.log4j.{Level, Logger}

object MainLine {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    val spark = new InputAndOutput()
    spark.getEnvironment("deviceAnalyse", "spark://***:7077")
    val csf1 = spark.getCsvFile(new MyEnm().csvFileUrl)
    val csf2 = csf1.select("等级", "青瓦斯", "重瓦斯", "油温", "油位", "风冷停", "风机启动", "压力释放")
    val labeledPointRdd = new DataProcess().packageToLabeledpointed(csf2)
    new PersistenceLevel().persistence(labeledPointRdd, 1)
    val Array(trainRDD, testRDD) = labeledPointRdd.randomSplit(Array(0.8, 0.2)) //折叠交叉验证
    val modelSelection = new ModelSelection()
    modelSelection.modelSelection(trainRDD, testRDD)
    val sgModel = new SaveAndGetModel()
    sgModel.saveModel(modelSelection.getBestModel(), spark.getSpark())
    val result = sgModel.getModelAndPredict(spark.getSpark())
    spark.dataToMysql(spark.getSpark(), result)

  }
}
