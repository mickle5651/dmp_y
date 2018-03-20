package cn.sheep.tags

import cn.sheep.beans.Logs
import cn.sheep.utils.Utils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 读取日志文件，并将数据打上标签，产生用户的上下文标签
  * Created by ThinkPad on 2017/4/16.
  */
object TagsContext {

	def main(args: Array[String]): Unit = {

		if (args.length < 4) {
			println(
				"""
				  |cn.sheep.tags.TagsContext
				  |参数：
				  |	datapath
				  | appdict
				  | devicedict
				  | outputpath
				""".stripMargin)
			System.exit(0)
		}

		// 接收参数
		val Array(datapath, appdictpath, devicepath, outputpath) = args

		val conf = new SparkConf()
		  .setAppName(s"${this.getClass.getSimpleName}")
		  .setMaster("local[*]")
		  .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
		  .registerKryoClasses(Array(classOf[Logs]))

		val sc = new SparkContext(conf)

		// 读取appdict字典文件并广播
		val appdict: Map[String, String] = sc.textFile(appdictpath).map {
			line =>
				val fields = line.split("\t", line.length)
				var appdictMap = Map[String, String]()
				if (fields.length > 4) {
					appdictMap += (fields(4) -> fields(1))
				}
				appdictMap
		}.collect().flatten.toMap
		// 将app字典内容广播到每个计算节点上
		val appdictBroadcast: Broadcast[Map[String, String]] = sc.broadcast(appdict)

		// 读取设备的字典文件并广播
		val devicedict: Map[String, String] = sc.textFile(devicepath).flatMap {
			line =>
				val fields = line.split("\t")
				var deviceMap = Map[String, String]()
				deviceMap += (fields(0) -> fields(1))
				deviceMap
		}.collect().toMap

		// 将设备字典广播到每个计算节点
		val deveicedictBroadcast: Broadcast[Map[String, String]] = sc.broadcast(devicedict)


		sc.textFile(datapath).map {
			line =>
				val log = Logs.line2Log(line)

				// 开始将数据转换成标签
				val adLocalTag = Tags4AdLocal.makeTags(log)
				val appTag = Tags4Apps.makeTags(log, appdictBroadcast.value)
				val areaTag = Tags4Area.makeTags(log)
				val channelTag = Tags4Channel.makeTags(log)
				val deviceTag = Tags4Device.makeTags(log, deveicedictBroadcast.value)
				val keysTag = Tags4Keyword.makeTags(log)

				// 用户的ID
				val uIdOption = getNotEmptyID(log)
				// (UID, List[(String, Int)]
				(uIdOption.getOrElse(""), (adLocalTag ++ appTag ++ areaTag ++ channelTag ++ deviceTag ++ keysTag).toList)
		}.filter(_._1.nonEmpty).reduceByKey{
			case (list1, list2) =>
				(list1 ++ list2).groupBy{ case (k, v) => k}.map{
					case (k1, v1) => (k1, v1.map(t=>t._2).sum)
				}.toList
		}.map(t=>t._1+"\t"+t._2.map(x=>x._1+":"+x._2).mkString("\t")).saveAsTextFile(outputpath)


		sc.stop()
	}


	// 获取用户唯一不为空的ID
	def getNotEmptyID(log: Logs): Option[String] = {
		log match {
			case v if v.imei.nonEmpty => Some("IMEI:" + Utils.formatIMEID(v.imei))
			case v if v.imeimd5.nonEmpty => Some("IMEIMD5:" + v.imeimd5.toUpperCase)
			case v if v.imeisha1.nonEmpty => Some("IMEISHA1:" + v.imeisha1.toUpperCase)

			case v if v.androidid.nonEmpty => Some("ANDROIDID:" + v.androidid.toUpperCase)
			case v if v.androididmd5.nonEmpty => Some("ANDROIDIDMD5:" + v.androididmd5.toUpperCase)
			case v if v.androididsha1.nonEmpty => Some("ANDROIDIDSHA1:" + v.androididsha1.toUpperCase)

			case v if v.mac.nonEmpty => Some("MAC:" + v.mac.replaceAll(":|-", "").toUpperCase)
			case v if v.macmd5.nonEmpty => Some("MACMD5:" + v.macmd5.toUpperCase)
			case v if v.macsha1.nonEmpty => Some("MACSHA1:" + v.macsha1.toUpperCase)

			case v if v.idfa.nonEmpty => Some("IDFA:" + v.idfa.replaceAll(":|-", "").toUpperCase)
			case v if v.idfamd5.nonEmpty => Some("IDFAMD5:" + v.idfamd5.toUpperCase)
			case v if v.idfasha1.nonEmpty => Some("IDFASHA1:" + v.idfasha1.toUpperCase)

			case v if v.openudid.nonEmpty => Some("OPENUDID:" + v.openudid.toUpperCase)
			case v if v.openudidmd5.nonEmpty => Some("OPENDUIDMD5:" + v.openudidmd5.toUpperCase)
			case v if v.openudidsha1.nonEmpty => Some("OPENUDIDSHA1:" + v.openudidsha1.toUpperCase)

			case _ => None
		}


	}
}
