package cn.sheep.users.test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2017/4/19.
  */
object ContextTagsAndUserId {
  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      println(
        """
          |com.dj.dmp.tags.ContextTagsAndUserId
          |dataInputPath log标签日志的输入文件
          |dataInputPath2 用户关系输入文件
          |outputPath 结果的输出路劲
        """.stripMargin)
      System.exit(0)
    }
    var Array(dataInputPath, dataInputPath2, outputPath) = args
    val sparkConf = new SparkConf().setAppName(s"${this.getClass.getSimpleName}")
      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sparkContext = new SparkContext(sparkConf)

    val userid: RDD[(String, String)] = sparkContext.textFile(dataInputPath).flatMap {
      line =>
        val fields = line.split("\t")
        val s = fields(1).split(",")
        s.map((_, fields(0)))
    }
	  val tags: RDD[(String, List[(String, Int)])] = sparkContext.textFile(dataInputPath2).map {
		  line =>
			  val fields = line.split(",")
			  val split = fields(1).split("\t")
			  val map: Array[(String, Int)] = split.map(t => {
				  val s = t.split(":")
				  (s(0), s(1).toInt)
			  })
			  map
			  (fields(0), map.toList)

	  }
	  tags

    //（imei,(line,1001）
    val useridAndlineAndId: RDD[(String, (List[(String, Int)], String))] = tags.join(userid)

    val result: RDD[(String, List[(String, Int)])] = useridAndlineAndId.map {
      case (userId, (tagList, hashcode)) => (hashcode, tagList)
    }.reduceByKey {
      case (list1, list2) => {
        (list1 ++ list2).groupBy(_._1)
		  .map {
			  case (m, n) => (m, n.map(_._2).sum)
		  }.toList
      }
    }
    result.map { case (k, v) => k + "," + v.map(t => t._1 + ":" + t._2).mkString("\t") }.saveAsTextFile(outputPath)
    sparkContext.stop()

  }

}
