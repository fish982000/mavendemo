package com.yn.graph

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import scala.math.random

object graphxexample {
  def main(args:Array[String]): Unit ={
    val conf = new SparkConf().setAppName("Graphx Example").setMaster("spark://192.168.116.101:7077").setJars(Seq("D:\\demo\\mavendemo\\out\\artifacts\\mavendemo_jar\\mavendemo.jar"))
    val sc = new  SparkContext(conf)

    println("Time:" + sc.startTime)
    val n = math.min(2,Int.MaxValue).toInt
    val count = sc.parallelize(1 until n,2).map{ i=>
      val x = random * 2 - 1
      val y = random * 2 - 1
      if (x*x + y*y < 1) 1 else 0
    }.reduce(_ + _)
    println("Pi is roughly " + 4.0 * count / n)
    sc.stop()
  }
}
