import scala.collection.mutable

/**
  * Created by Administrator on 2018/3/21.
  */
object TestMap {

  def main(args:Array[String]):Unit = {

      var A :Map[String,Int] = Map()

      A += ("zhang3" -> 24)
      A += ("li4" -> 25)

      A += ("zhang3" -> 26)
//
//    A.keys.foreach{
//      x=>println(x)
//        println(A(x))
//    }


    val map: mutable.Map[String,Int] = mutable.Map()

    map += ("li4" -> 27)
    map += ("meichen" -> 28)

    val map1 : Map[String, Int] = A.++(map)
    val map2 : mutable.Map[String, Int] = map.++(A)
    var map3 = A ++ map
    var map4 = map ++ A
//    map1.keys.foreach{i => println(i + "==>" + map1(i))}
//    map2.keys.foreach{i => println(i + "==>" + map2(i))}
//    map3.keys.foreach{i => println(i + "==>" + map3(i))}
//  1  map4.keys.foreach{i => println(i + "==>" + map4(i))}

//    println(map.contains("meichen"))


//    Source.fromFile(new File("E:\\BaiduNetdiskDownload\\DMP精准推荐（小牛学堂）\\spark项目\\app_mapping.txt")).foreach(print)





  }




}
