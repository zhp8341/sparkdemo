# sparkdemo

1:SparkStreamFile  本地运行（数据源本地文件，结果输出到本地） 本地直接运行即可

2:SparkStreamHDFS  集群下运行（数据来源hdfs，输出结果保存在hdfs上）
运行操作登录hadoop1主机 进入/home/hadoop/zhp目录 
执行：  spark-submit --class com.demo.spark.sparkdemo.SparkStreamHDFS  --master spark://hadoop1:7077  sparkdemo-0.0.1-SNAPSHOT.jar 
说明：监听hdfs://hadoop1:8020/spark/sparkwordCounts/input 目录下文件 
     结果输出到 hdfs://hadoop1:8020/spark/sparkwordCounts/

3:SparkStreamingPollDataFromFlume   集群下运行（数据来源socket，输出到控制台）
运行操作登录hadoop1主机 进入/home/hadoop/zhp目录
执行：  spark-submit --class com.demo.spark.sparkdemo.SparkStreamingPollDataFromFlume  --master spark://hadoop1:7077  sparkdemo-0.0.1-SNAPSHOT.jar 
运行后 执行  nc  -lk 9999 命令 输入相关内容 控制台就能实时计算结果显示


4:SparkSteamingKafka 集群下运行 (数据来源kafka）还未调试