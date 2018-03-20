package cn.sheep.tags

import cn.sheep.beans.Logs
import org.apache.commons.lang.StringUtils

/**
  * 标签 --
  * 	关键字标签
  * Created by ThinkPad on 2017/4/16.
  */
object Tags4Keyword extends Tags{
	/**
	  * 打标签的接口方法
	  *
	  * @param args
	  * @return Map[String, Int]
	  */
	override def makeTags(args: Any*): Map[String, Int] = {

		var keywordsMap = Map[String, Int]()
		if (args.length > 0) {
			val log = args(0).asInstanceOf[Logs]

			if (StringUtils.isNotEmpty(log.keywords)) {
				val keys = log.keywords.split("\\|")
				keys.filter(e=> e.length >=3 && e.length <=8).map(t=>keywordsMap+=("K"+t.replace(":","") -> 1))
			}
		}
		keywordsMap
	}
}
