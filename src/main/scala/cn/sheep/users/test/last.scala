package cn.sheep.users.test

import cn.sheep.beans.Logs
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Created by Administrator on 2017/4/18.
  */
object last {
  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      println(
        """
          |sheep.report.oddline.AreaAnalyse <inputPath><outputPath>
          |inputPath:数据输入目录 follower
          |inputPath2:数据输入目录 user
          |outputPath1:输出目录
        """.stripMargin)
      System.exit(0)
    }
    val Array(inputPath, inputPath2, outputPath) = args
    //val clazz = new AdTagsClass
    val conf = new SparkConf().setAppName(s"${this.getClass.getSimpleName}").setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(classOf[Logs]))
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    //将标签广播
    val brdd = sc.textFile(inputPath).map(line => {
      val fields = line.split("\t")
      val map = new mutable.HashMap[String, Long]()
      for (i <- 1 to fields.length - 1) {
        val split = fields(i).split(":")
        map += ((split(0) -> split(1).toLong))
      }
      //IMEI:245823536472167	LC01:1	D00020005:1	ZC武汉市:1	ZC湖北省:1	D00030004:1	D00010001:1	App汽车-and:1	CN100016:1
      (fields(0), map)
    })
    val collect = brdd.collect().toMap
    val broadcast: Map[String, mutable.HashMap[String, Long]] = sc.broadcast(collect).value
    val lines = sc.textFile(inputPath2)
    val map: RDD[(String, ListBuffer[(String, Long)])] = lines.map(line => {
      val lst1 = ListBuffer[(String, Long)]()
      //IMEI:866568029220182	ANDROIDID:FEB8F79353D7519D
      val split = line.split("\t")
      try {
        for (i <- split) {
          val list: mutable.HashMap[String, Long] = broadcast(i)
          //电影:1  电视:2
          lst1 ++= list
        }
      }catch {
        case _: Exception => None
      }
      (line, lst1)
    })filter(_._2.length>0)
    map.mapValues(m => {
      m.groupBy {
        case (k, v) => k
      }.map {
        case (k1, v1) => (k1, v1.map(t => t._2).sum)
      }.toList
    }).map(t => t._1 +"\t"+ t._2.map(x => x._1 + ":" + x._2).mkString ("\t")).coalesce(1).saveAsTextFile(outputPath)
    sc.stop()
  }
}
