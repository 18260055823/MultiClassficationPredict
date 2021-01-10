package TreeDecision

import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.rdd.RDD


class ModelSelection {
  private[this] var bestModel: DecisionTreeModel = null

  def setBestModel(model:DecisionTreeModel) = {
    if (model.isInstanceOf[DecisionTreeModel]) {
      this.bestModel = model
    } else {
      println("模型错误")
    }
  }

  def getBestModel(): DecisionTreeModel = {
    this.bestModel
  }

  def modelSelection(trainRDD: RDD[LabeledPoint], testRDD: RDD[LabeledPoint]) = {
    val tuple7Array1 = for {
      maxDepth <- Array(1, 2, 3, 4, 5, 10, 20)
      maxBins <- Array(2, 4, 8, 16, 32, 64)
    } yield {
      val model: DecisionTreeModel = DecisionTree.trainClassifier(trainRDD, 5, Map[Int, Int](), "gini", maxDepth, maxBins)
      val metric = new Metric()
      val tuple4 = metric.dtMultiMetric(model, testRDD)
      (tuple4._1, tuple4._2, tuple4._3, tuple4._4, maxDepth, maxBins, model)
    }

    if (tuple7Array1.sortBy(_._1).reverse.take(2)(0)._1 - tuple7Array1.sortBy(_._1).reverse.take(2)(1)._1 >= 0.005) {
      setBestModel(tuple7Array1.sortBy(_._1).reverse.take(2)(0)._7)
    } else if (tuple7Array1.sortBy(_._1).reverse.take(2)(0)._4 > tuple7Array1.sortBy(_._1).reverse.take(2)(1)._4) {
      setBestModel(tuple7Array1.sortBy(_._1).reverse.take(2)(0)._7)
    } else {
      setBestModel(tuple7Array1.sortBy(_._1).reverse.take(2)(1)._7)
    }
  }
}
