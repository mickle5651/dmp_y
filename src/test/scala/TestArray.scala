/**
  * Created by Administrator on 2018/3/21.
  */
object TestArray {

  def main(args:Array[String]):Unit={

    var arr : Array[String] = new Array[String](3)
    var arr1 = new Array[String](3)
    var arr2 = Array("java","python","scala")

    var arr3 = Range(10,20,2)
    for(x <- arr3){
      println(x)
    }

  }

}
