package cn.sheep.tools

import cn.sheep.beans.Logs
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 将原始日志bzi2格式转换成parquet文件
  * Created by ThinkPad on 2017/4/13.
  */
object Biz2parquet {

	def main(args: Array[String]): Unit = {

		// 1. 判断参数个数是否符合要求
		if (args.length < 3) {
			println(
				"""
				  |cn.sheep.tools.Biz2parquet <dataPath> <outputPath> <compressionCode>
				  |<dataPath> ：日志文件所在的路径
				  |<outputPath>：结果输出路径
				  |<compressionCode>: 存储数据所采用的压缩编码格式
				""".stripMargin)
			System.exit(0)
		}

		// 2. 接受参数
		val Array(dataPath, outputPath, compressionCode) = args
		// 3. 创建一个sparkConf对象，并设置相关的运行时参数
		/**
		  * sparkconf参数为运行时的全局参数
		  */
		val sparkConf = new SparkConf()
		sparkConf.setAppName(s"${this.getClass.getSimpleName}")
		sparkConf.setMaster("local[*]")
		sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
		sparkConf.set("spark.sql.parquet.compression.codec", compressionCode)
		sparkConf.registerKryoClasses(Array(classOf[Logs]))

		// 4. 创建一个sparkContext对象
		val sparkContext = new SparkContext(sparkConf)

		val sQLContext = new SQLContext(sparkContext)
//		sQLContext.setConf("", compressionCode)

		// 5. 读取日志文件，进行响应的业务逻辑处理
		val rdd: RDD[Logs] = sparkContext.textFile(dataPath).map {
			line => Logs.line2Log(line)
		}
		// 6. 将处理完的结果存储xxx位置
		sQLContext.createDataFrame(rdd).write.parquet(outputPath)

		// 7. 关闭sparkContext对象
		sparkContext.stop()
	}

}
