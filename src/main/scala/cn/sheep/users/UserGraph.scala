package cn.sheep.users

import java.util.UUID

import cn.sheep.beans.Logs
import org.apache.spark.graphx.{GraphLoader, VertexId, VertexRDD}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 从日志文件中提取出用户关系
  * Created by ThinkPad on 2017/4/19.
  */
object UserGraph {

	def main(args: Array[String]): Unit = {

		if (args.length != 3){
			println(
				"""
				  |cn.sheep.users.UserGraph
				  |参数：<datapath> <usersship> <outputPath>
				""".stripMargin)
			System.exit(0)
		}

		val Array(datapath, usersship, outputpath) = args

		val sparkConf = new SparkConf()
		  .setAppName(s"${this.getClass.getSimpleName}")
		  .setMaster("local")
		  .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
		  .registerKryoClasses(Array(classOf[Logs]))

		val sc = new SparkContext(sparkConf)

		// 读取日志文件
		val basedata = sc.textFile(datapath).map {
			line =>
				val log = Logs.line2Log(line)
				val userIds = getUserIds(log)
//			  	val userIds = line.split(",").toSet
				(math.abs(UUID.randomUUID().hashCode()), userIds) // (hashId, Set[string])
		}.filter(_._2.nonEmpty).cache()

		// 找数据之间的关系
		basedata.flatMap{
			t => t._2.map(x=>(x, t._1.toString)) // (imei, 1) (idfa, 2)
		}
		  .reduceByKey((a, b) => a.concat(",").concat(b)) // (1, 2, 3) => 1 => 1 2 \n 1	3
		  .map{
			t =>
				val hashIds = t._2.split(",")
				var ships = Map[String, String]()
				if (hashIds.size == 1) {
					ships += (hashIds(0) -> hashIds(0))
				} else {
					// (1,2,3,4,5) => (1->2)(1->3)(1->4)(1->5)
					hashIds.map(x => ships += (hashIds(0) -> x))
				}
				ships.map(x=>x._1+"\t"+x._2).toSeq.mkString("\t")
		}
//		  .foreach(println _)
		  .saveAsTextFile(usersship)

		val graph = GraphLoader.edgeListFile(sc, usersship)
		val cc: VertexRDD[VertexId] = graph.connectedComponents().vertices


		basedata.map(x=>(x._1.toLong, x._2)).join(cc).map{
			case (hashId, (uIds, cc)) => (cc, uIds)
		}.reduceByKey(_ ++ _).map{
			x => math.abs(x._2.hashCode()).toString +"\t" + x._2.mkString("\t")
		}.saveAsTextFile(outputpath)

		sc.stop()

	}

	/**
	  * 获取用户不为空的ID 集合
	  * @param log
	  * @return
	  */
	def getUserIds(log: Logs): Set[String] = {
		var ids = Set[String]()
		// 取出用户不为空的id属性字段，并放入到set中
		if (log.imei.nonEmpty)  ids ++= Set("IMEI:"+log.imei.toUpperCase)
		if (log.imeimd5.nonEmpty) ids ++= Set("IMEIMD5:"+log.imeimd5.toUpperCase)
		if (log.imeisha1.nonEmpty) ids ++= Set("IMEISHA1:"+log.imeisha1.toUpperCase)

		if (log.androidid.nonEmpty) ids ++= Set("ANDROIDID:"+log.androidid.toUpperCase)
		if (log.androididmd5.nonEmpty) ids ++= Set("ANDROIDIDMD5:"+log.androididmd5.toUpperCase)
		if (log.androididsha1.nonEmpty) ids ++= Set("ANDROIDIDSHA1:"+log.androididsha1.toUpperCase)

		if (log.idfa.nonEmpty) ids ++= Set("IDFA:"+log.idfa.toUpperCase)
		if (log.idfamd5.nonEmpty) ids ++= Set("IDFAMD5:"+log.idfamd5.toUpperCase)
		if (log.idfasha1.nonEmpty) ids ++= Set("IDFASHA1:"+log.idfasha1.toUpperCase)

		if (log.mac.nonEmpty) ids ++= Set("MAC:"+log.mac.toUpperCase)
		if (log.macmd5.nonEmpty) ids ++= Set("MACMD5:"+log.macmd5.toUpperCase)
		if (log.macsha1.nonEmpty) ids ++= Set("MACSHA1:"+log.macsha1.toUpperCase)

		if (log.openudid.nonEmpty) ids ++= Set("OPENUDID:"+log.openudid.toUpperCase)
		if (log.openudidmd5.nonEmpty) ids ++= Set("OPENUDIDMD5:"+log.openudidmd5.toUpperCase)
		if (log.openudidsha1.nonEmpty) ids ++= Set("OPENUDIDSHA1:"+log.openudidsha1.toUpperCase)

		ids
	}

}
