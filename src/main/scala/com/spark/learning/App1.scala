package com.spark.learning

import org.apache.spark.{SparkConf, SparkContext}

object App1 {
      def main(args:Array[String]):Unit={
        val conf=new SparkConf().setMaster("local")
          .setAppName("First App")
        val sc=new SparkContext(conf)

        val rd1=sc.makeRDD(List(1,2,4,5))
        rd1.collect().foreach(println)
        sc.stop
      }
}
