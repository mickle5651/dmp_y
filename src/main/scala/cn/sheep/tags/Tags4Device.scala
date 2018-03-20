package cn.sheep.tags

import cn.sheep.beans.Logs

/**
  * 标签
  * 	-- 操作系统
  * 	-- 联网方式
  * 	-- 运营商
  * Created by ThinkPad on 2017/4/16.
  */
object Tags4Device extends Tags{
	/**
	  * 打标签的接口方法
	  *
	  * @param args
	  * @return Map[String, Int]
	  */
	override def makeTags(args: Any*): Map[String, Int] = {

		var deviceMap = Map[String, Int]()

		if (args.length > 1) {
			val log = args(0).asInstanceOf[Logs]
			// 设备字典
			val devicedict = args(1).asInstanceOf[Map[String, String]]
			// 操作系统

			val osOption: Option[String] = devicedict.get(log.client.toString)
			if (osOption.nonEmpty) deviceMap += (osOption.get -> 1)
			else {
				deviceMap += (deviceMap.get("4").get.toString -> 1)
			}

			// 联网方式
			val network = devicedict.getOrElse(log.networkmannername, devicedict.get("NETWORKOTHER").get)
			deviceMap += (network -> 1)


			// 运营商
			val isp = devicedict.getOrElse(log.ispname, devicedict.get("OPERATOROTHER").get)

			deviceMap += (isp -> 1)
		}

		deviceMap
	}
}
