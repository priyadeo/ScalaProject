package com.spark.learning

import org.apache.spark.{SparkConf, SparkContext}

object Assignment {
  def main(args:Array[String]):Unit={
    val conf=new SparkConf().setMaster("local").setAppName("Assignment")
    val sc=new SparkContext(conf)
     val rd=sc.textFile("src/main/resources/Spark-_Assignment_Data-File.txt");
    val mp=rd.flatMap(word=>word.split(" "))
    val count=mp.map(l=>(l,1))
    val countWord=count.reduceByKey(_+_)

    countWord.collect().foreach(println)
  }

}
