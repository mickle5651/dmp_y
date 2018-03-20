package cn.sheep.report.offline

import cn.sheep.beans.Logs
import cn.sheep.utils.ReportUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 媒体报表分析
  * Created by ThinkPad on 2017/4/15.
  */
object AppsAnalyse {

	def main(args: Array[String]): Unit = {

		// 判断参数
		if(args.length < 2) {
			println(
				"""
				  |cn.sheep.report.offline.AppsAnalyse
				  |参数：
				  |	datapath
				  | appmapping
				  | outputpath
				""".stripMargin)
			System.exit(0)
		}


		// 接收参数
		val Array(datapath, appmapping, outputpath) = args

		// sparkconf
		val sparkConf = new SparkConf()
		  .setAppName(s"${this.getClass.getSimpleName}")
		  .setMaster("local")
		  .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
		  .registerKryoClasses(Array(classOf[Logs]))

		// sparkContext
		val sc = new SparkContext(sparkConf)

//		val hashMap = new  scala.collection.mutable.HashMap[String, String]()

		// 读取字典app_mapping中的数据，并转化成map【String, String】
		val appMap: Map[String, String] = sc.textFile(appmapping).map {
			line =>
				var map = Map[String, String]()
				val fields = line.split("\t", line.length)
				map += (fields(4) -> fields(1))
				map
		}
		  // Array[Map, map, map] -> map[]
		  .collect()
		  // Array(() ,())
		  .flatten
		  // map[（）（）]
		  .toMap


		// appMap广播出去
		val broadcast: Broadcast[Map[String, String]] = sc.broadcast(appMap)

		// 读取数据，进行计算
		sc.textFile(datapath).map{
			line =>
				println(line)
				val log = Logs.line2Log(line)
//				println(log.requestmode + "\\t"+log.processnode)
				// 计算指标
				val adRequest = ReportUtils.calculateAdRequest(log) // 3个元素
				val adResponse = ReportUtils.calulateAdResponse(log) // 2个元素
				val adShowClick = ReportUtils.calulateAdShowClick(log) //2个元素
				val adCost = ReportUtils.calulateAdCost(log) // 2个元素
				/*优先取用应用Id,如果没有应用id,则取应用名称，请求、响应、点击、消费中list元素合并*/
				(broadcast.value.getOrElse(log.appid, log.appname), adRequest ++ adResponse ++ adShowClick ++ adCost)
		}.filter(_._1.nonEmpty).reduceByKey{
			case (list1, list2) =>
				list1.zip(list2).map{
					case (v1, v2) => v1 + v2
				}
		}.map(t=> t._1+"\t"+t._2.mkString(",")).saveAsTextFile(outputpath)

		sc.stop()

	}

}
