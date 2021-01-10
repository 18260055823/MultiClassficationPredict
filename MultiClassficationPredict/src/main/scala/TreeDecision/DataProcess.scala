package TreeDecision

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql

class DataProcess {
  def packageToLabeledpointed(file: sql.DataFrame) = {
    val labeledpointRDD = file.rdd.map(row => {
      //标签
      val grade = if ("无事故".equals(row.getAs[String]("等级"))) 0 else if ("轻微事故".equals(row.getAs[String]("等级"))) 1
      else if ("一般事故".equals(row.getAs[String]("等级"))) 2 else if ("严重事故".equals(row.getAs[String]("等级"))) 3
      else 4
      //特征
      val qws = row.getAs[String]("青瓦斯").toDouble
      val zws = row.getAs[String]("重瓦斯").toDouble
      val ywen = row.getAs[String]("油温").toDouble
      val ywei = row.getAs[String]("油位").toDouble
      val flt = row.getAs[String]("风冷停").toDouble
      val fjqd = row.getAs[String]("风机启动").toDouble
      val ylsf = row.getAs[String]("压力释放").toDouble
      /*
       @Since("0.8.0") label: Double,
       @Since("1.0.0") features: Vector
       */
      //封装到labeledpoint中
      LabeledPoint(grade, Vectors.dense(qws, zws, ywen, ywei, flt, fjqd, ylsf))
    })
    labeledpointRDD
  }

}
