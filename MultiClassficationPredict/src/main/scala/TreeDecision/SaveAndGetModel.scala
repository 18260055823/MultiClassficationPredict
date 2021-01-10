package TreeDecision

import java.io.IOException

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.sql.SparkSession

class SaveAndGetModel {
  def saveModel(bestMode: DecisionTreeModel, spark: SparkSession) = {
    try {
      bestMode.save(spark.sparkContext, new MyEnm().storeUrl)
    } catch {
      case e: IOException => println("model save exception")
    }

  }

  def getModelAndPredict(spark: SparkSession) = {
    var predictResult: Double = null
    try {
      val getBestModel: DecisionTreeModel = DecisionTreeModel.load(spark.sparkContext, new MyEnm().storeUrl)
      val features = Vectors.dense(1, 1, 1, 1, 1, 1, 1)
      predictResult = getBestModel.predict(features)
    } catch {
      case e: IOException => println("load model exception")
    }
    predictResult
  }
}
