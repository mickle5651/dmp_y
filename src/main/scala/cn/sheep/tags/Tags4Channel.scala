package cn.sheep.tags

import cn.sheep.beans.Logs
import org.apache.commons.lang.StringUtils

/**
  * 标签 ---  渠道 （ADX）
  * Created by ThinkPad on 2017/4/16.
  */
object Tags4Channel extends Tags{
	/**
	  * 打标签的接口方法
	  *
	  * @param args
	  * @return Map[String, Int]
	  */
	override def makeTags(args: Any*): Map[String, Int] = {
		var channelMap = Map[String, Int]()

		if (args.length > 0) {
			val log = args(0).asInstanceOf[Logs]

			if (StringUtils.isNotEmpty(log.adplatformproviderid.toString)) {
				channelMap += ("CN"+log.adplatformproviderid -> 1)
			}
		}

		channelMap
	}
}
