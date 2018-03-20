package cn.sheep.tags

import cn.sheep.beans.Logs
import org.apache.commons.lang.StringUtils

/**
  * 标签 -- 广告位
  * Created by ThinkPad on 2017/4/16.
  */
object Tags4AdLocal extends Tags{
	/**
	  * 打标签的接口方法
	  *
	  * @param args
	  * @return Map[String, Int]
	  */
	override def makeTags(args: Any*): Map[String, Int] = {
		var adLocalTagMap = Map[String, Int]()

		// 校验参数
		if (args.length > 0) {
			// 将arg(0)转成Logs对象
			val log = args(0).asInstanceOf[Logs]

			// 广告位ID 标签
			log.adspacetype match {
				case x if x < 10 => adLocalTagMap += ("LC0".concat(x.toString) -> 1)
				case x if x > 9 => adLocalTagMap += ("LC".concat(x.toString) -> 1)
			}

			// 广告位的名称 名称
			if(StringUtils.isNotEmpty(log.adspacetypename)) {
				adLocalTagMap += ("LN".concat(log.adspacetypename) -> 1)
			}
		}
		adLocalTagMap
	}
}
