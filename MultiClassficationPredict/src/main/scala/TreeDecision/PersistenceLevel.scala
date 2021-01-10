package TreeDecision

import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

class PersistenceLevel {
  def persistence(rdd: RDD[LabeledPoint], level: Int) = level match {
    case 1 => rdd.persist(StorageLevel.MEMORY_ONLY)
    case 2 => rdd.persist(StorageLevel.MEMORY_ONLY_SER)
    case 3 => rdd.persist(StorageLevel.MEMORY_AND_DISK_SER)
    case _ => println("持久化等级错误")
  }
}
