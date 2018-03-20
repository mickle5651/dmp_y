package cn.sheep.tags

/**
  * 标签接口
  * Created by ThinkPad on 2017/4/16.
  */
trait Tags {

	/**
	  * 打标签的接口方法
	  * @param args
	  * @return Map[String, Int]
	  */
	def makeTags(args: Any*): Map[String, Int]

}
