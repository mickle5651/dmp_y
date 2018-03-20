package cn.sheep.tags

import cn.sheep.beans.Logs
import org.apache.commons.lang.StringUtils

/**
  * 应用名称标签
  * Created by ThinkPad on 2017/4/16.
  */
object Tags4Apps extends Tags{
	/**
	  * 打标签的接口方法
	  *
	  * @param args
	  * @return Map[String, Int]
	  */
	override def makeTags(args: Any*): Map[String, Int] = {
		var appNameMap = Map[String, Int]()

		if (args.length > 1) {
			val log = args(0).asInstanceOf[Logs]
			val appdict = args(1).asInstanceOf[Map[String, String]]
			// 如果appdict中找不到log.appid, 则返回log.appname
			// 否则 adddict.appname
			val appName = appdict.getOrElse(log.appid, log.appname)

			if (StringUtils.isNotEmpty(appName)) {
				appNameMap += ("APP"+ appName -> 1)
			}

		}
		appNameMap
	}
}
