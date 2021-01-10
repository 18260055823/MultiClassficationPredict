package TreeDecision

import org.apache.spark.mllib.evaluation.{BinaryClassificationMetrics, MulticlassMetrics}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.rdd.RDD

class Metric {
  def dtMultiMetric(model: DecisionTreeModel, testRDD: RDD[LabeledPoint]) = {
    val dtMPredictAndActualRDD: RDD[(Double, Double)] = testRDD.map({ case LabeledPoint(label, features) => (model.predict(features), label) })
    val dtMetrics = new MulticlassMetrics(dtMPredictAndActualRDD)
    (dtMetrics.accuracy, dtMetrics.weightedPrecision, dtMetrics.weightedRecall, dtMetrics.weightedFMeasure) //准确率+加权精确率+加权召回率+加权综合指标
  }

  def dtBinaryMetric(model: DecisionTreeModel, testRDD: RDD[LabeledPoint]) = {
    val dtBPredictAndActualRDD = testRDD.map({ case LabeledPoint(label, features) => (model.predict(features), label) })
    val dtMetrics = new BinaryClassificationMetrics(dtBPredictAndActualRDD)
    val dtROC = dtMetrics.areaUnderROC() //AUC指标
    dtROC
  }

}
