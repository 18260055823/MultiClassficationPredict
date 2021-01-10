./spark-submit \ (进入bin目录下，使用spark-submit进程提交)

--master spark://master:7077 \
--class com.daoke360.classification.MainLine \（指定程序入口）
hdfs://master:9000/******（从hdfs中文件路径）

//spark性能调优-资源调优
--num-executors 100 \
--executor-memory 6G \
--executor-cores 4 \
--driver-memory 1G \
--conf spark.default.parallelism=1000 \
--conf spark.storage.memoryFraction=0.5 \
--conf spark.shuffle.memoryFraction=0.3 \

