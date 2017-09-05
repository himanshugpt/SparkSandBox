package com.himanshu

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object CustomerSpending {
  
  def parseLine(line:String)= {
    val fields = line.split(",")
    val cust = fields(0).toInt
    val amt = fields(2).toFloat
    (cust, amt)
  }
  
  def main(args: Array[String]){
    
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
     // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "CustomerSpending")   
    
    // Read each line of my book into an RDD
    val input = sc.textFile("../SparkScala/customer-orders.csv")
    
    val rdd1 = input.map(parseLine)
    
    val rdd2 = rdd1.reduceByKey((x,y) => (x+y))
    val rdd3 = rdd2.map((x) => (x._2, x._1))
    
    val results = rdd3.collect()
    
    for(custrecord <- results.sorted){
      println(custrecord)
    }
    
  }
  
}