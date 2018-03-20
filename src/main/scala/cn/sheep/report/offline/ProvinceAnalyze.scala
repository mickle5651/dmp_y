package cn.sheep.report.offline

import cn.sheep.beans.Logs
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 统计各省市数据量分布情况
  * Created by ThinkPad on 2017/4/13.
  */
object ProvinceAnalyze {

	def main(args: Array[String]): Unit = {

		//1. 判断参数个数
		if (args.length < 2) {
			println(
				"""
				  |cn.sheep.report.offline.ProvinceAnalyze <dataInputPath> <outputPath>
				  |参数：
				  |	dataInputPath： 数据所在路径
				  | outputPath: 结果存储路径
				""".stripMargin)
			System.exit(0)
		}

		//2. 接受参数
		val Array(inputPath, outputPath) = args

		//3. 创建一个sparkconf对象，设置运行时的相关参数
		val sparkConf = new SparkConf()
		  .setAppName(s"${this.getClass.getSimpleName}")
		  .setMaster("local[2]")
		  .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
		  .registerKryoClasses(Array(classOf[Logs]))
		//4. 创建一个sparkContext对象
		val sc = new SparkContext(sparkConf)
		val sQLContext = new SQLContext(sc)
		//5. 读取数据文件，进行业务逻辑处理
		val dataFrame = sQLContext.read.parquet(inputPath)
		// 注册临时表
		dataFrame.registerTempTable("logs")

		//6. 存储结果
		sQLContext.sql("select count(*) ct, provincename, cityname from logs group by provincename, cityname order by provincename").coalesce(1)
		  .write.json(outputPath)


		//7. 关闭sparkcontext
		sc.stop()
	}

}
