package cn.sheep.tags

import cn.sheep.beans.Logs

/**
  * 标签 --
  * 	地域
  * Created by ThinkPad on 2017/4/16.
  */
object Tags4Area extends Tags{
	/**
	  * 打标签的接口方法
	  *
	  * @param args
	  * @return Map[String, Int]
	  */
	override def makeTags(args: Any*): Map[String, Int] = {
		var areaMap = Map[String, Int]()

		if (args.length > 0) {
			val log = args(0).asInstanceOf[Logs]

			if (log.provincename.nonEmpty) {
				areaMap += ("ZP"+log.provincename -> 1)
			}

			if (log.cityname.nonEmpty) {
				areaMap += ("ZC"+log.cityname -> 1)
			}

		}
		areaMap
	}
}
