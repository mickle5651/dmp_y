package cn.sheep.report.offline

import cn.sheep.beans.Logs
import cn.sheep.utils.ReportUtils
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ThinkPad on 2017/4/13.
  */
object AreaAnalyse {


	def main(args: Array[String]): Unit = {
		if (args.length < 2) {
			println(
				"""
				  |cn.sheep.report.offline.AreaAnalyse inputdataPath outputPath
				""".stripMargin)
			System.exit(0)
		}

		val Array(inputdataPath, outputPath) = args

		val sparkConf = new SparkConf()
		  .setAppName(s"${this.getClass.getSimpleName}")
		  .setMaster("local")
		  .registerKryoClasses(Array(classOf[Logs]))


		val sc = new SparkContext(sparkConf)


		val rdd = sc.textFile(inputdataPath).map {
			line =>
				val log = Logs.line2Log(line)
				val adRequest = ReportUtils.calculateAdRequest(log)
				val adResponse = ReportUtils.calulateAdResponse(log)
				val adShowClick = ReportUtils.calulateAdShowClick(log)
				val adCost = ReportUtils.calulateAdCost(log)

				(log.provincename, log.cityname, adRequest ++ adResponse ++ adShowClick ++ adCost)
		}

		// 计算省的指标情况
		rdd.map {
//			tp => (tp._1, tp._3)
			case (p, c, list) => (p, list)
		}.reduceByKey {
			case (list1, list2) =>
				list1.zip(list2).map{ // 将list1 和list2对应的索引位置求和
					case (v1, v2) => v1 + v2
				}
		}.map(t => t._1+"\t" + t._2.mkString(",")).saveAsTextFile(outputPath)// 格式化数据格式，存储到指定目录

		// 计算省市的指标情况
		rdd.map{
			case (p, c, list) => (p+"|"+c, list)
		}.reduceByKey {
			case (list1, list2) =>
				list1.zip(list2).map{
					case (v1, v2) => v1 + v2
				}
		}.map(t => t._1+"\t" + t._2.mkString(",")).saveAsTextFile(outputPath)

	}

}
