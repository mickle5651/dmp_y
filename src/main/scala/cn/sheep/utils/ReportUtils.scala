package cn.sheep.utils

import cn.sheep.beans.Logs

/**
  * Created by ThinkPad on 2017/4/13.
  */
object ReportUtils {

	/**
	  * 总请求，有效请求，广告请求
	  * 返回 List<Double> ，对应 位置打标签 1
	  * @param log
	  * @return
	  */
	def calculateAdRequest(log: Logs): List[Double] = {
		if (log.requestmode == 1) {//如果是请求
			if (log.processnode == 1) {//请求量kpi
				List(1, 0, 0) // 总请求
			} else if (log.processnode == 2) {//有效请求
				List(1, 1, 0)
			} else if (log.processnode == 3) {//广告请求
				println("========收到的是请求广告========")
				List(1, 1, 1)
			} else {//其他
				List(0, 0, 0)
			}
		} else {//展示和点击
			List(0, 0, 0)
		}
	}

	/**
	  * 参与竞价数，竞价成功数
	  *
	  * @param log
	  * @return
	  */
	def calulateAdResponse(log: Logs): List[Double] = {
		// rtb 、有效、是否收费
		if (log.adplatformproviderid >= 100000 && log.iseffective == 1 && log.isbilling == 1) {
			// 是rtb , 广告id 不等于0
			if (log.isbid == 1 && log.adorderid != 0) {
				List(1, 0)
				//总价成功
			} else if (log.iswin == 1) {
				List(0, 1)
			} else {
				List(0, 0)
			}
		} else {
			List(0, 0)
		}
	}


	/**
	  * 展示量 ， 点击量
	  *
	  * @param log
	  * @return
	  */
	def calulateAdShowClick(log: Logs): List[Double] = {
		/*数据请求方式为展示且有效*/
		if (log.requestmode == 2 && log.iseffective == 1) {
			List(1, 0)
			/*数据请求方式为点击且有效*/
		} else if (log.requestmode == 3 && log.iseffective == 1) {
			List(0, 1)
		} else {
			List(0, 0)
		}
	}

	/**
	  * 消费， 成本
	  * @param log
	  * @return
	  */
	def calulateAdCost(log: Logs): List[Double] = {
		if (log.adplatformproviderid >= 100000 //rtb
		  && log.iseffective == 1 //有效:表示可以正常计费
		  && log.isbilling == 1 //是否收费：1,表示收费
		  && log.iswin == 1 //是否总价成功
		  && log.adcreativeid > 200000 // 广告创意id
		  && log.adorderid > 200000) { // 广告id
			List(log.winprice /1000, log.adpayment / 1000) //rtb 总价成功价格，和转换后的消费
		} else {
			List(0.0, 0.0)
		}
	}


}
