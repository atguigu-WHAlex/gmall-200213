package com.atguigu.app

object TestSplitAt {

  def main(args: Array[String]): Unit = {

    val tuple: (String, String) = "13412349876".splitAt(3)
    println(tuple._1)
    println(tuple._2)

  }

}
