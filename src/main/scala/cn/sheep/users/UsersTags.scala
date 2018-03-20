package cn.sheep.users

import cn.sheep.beans.Logs
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ThinkPad on 2017/4/19.
  */
object UsersTags {

	def main(args: Array[String]): Unit = {
		if (args.length != 3) {
			println(
				"""
				  |cn.sheep.users.UsersTags
				  |参数：
				  |	datapath 上下文标签所在路径
				  | usershippath 用户关系数据所在路径
				  | outputpath 结果存储路径
				""".stripMargin)
			System.exit(0)
		}

		// 接受参数
		val Array(datapath, usershippath, outputpath) = args

		val sparkConf = new SparkConf()
		  .setAppName(s"${this.getClass.getSimpleName}")
		  .setMaster("local")
		  .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
		  .registerKryoClasses(Array(classOf[Logs]))

		val sc = new SparkContext(sparkConf)

		// 读取上下文标签数据
		val ut = sc.textFile(datapath).map {
			line =>
				val fields = line.split("\t")
				val tags = fields.slice(1, fields.length).flatMap {
					// tv: ZP广东省：1
					tv =>
						var tmap = Map[String, Int]()
						val tkv = tv.split(":")
						tmap += (tkv(0) -> tkv(1).toInt)
						tmap
				}
				(fields(0), tags.toSeq)
		}

		// 读取关系数据
		val us = sc.textFile(usershippath).flatMap {
			line =>
				val uIds = line.split("\t")
				uIds.slice(1, uIds.length).map(t => (t, uIds(0)))
		}
		// (u15?, hashCodeId)  (u15?, Seq) => (u15?, (hashCodeId, seq))
		us.join(ut).map{
			case (uId, (hashCodeId, tags)) => (hashCodeId, tags ++ Seq((uId, 0)))
		}.reduceByKey{
			case (s1, s2) =>
				(s1 ++ s2).groupBy(_._1).map{
					case (tk, stk) =>
						val sum = stk.map(_._2).sum
						(tk, sum)
				}.toSeq
		}.map{
			t =>
				t._1 + "\t" + t._2.map{
					x =>
						if (x._2 > 0) {
							x._1+":"+x._2
						} else {
							x._1
						}

				}.toList.mkString("\t")
		}.saveAsTextFile(outputpath)

		sc.stop()
	}

}
