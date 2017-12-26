
package main

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object BatchFileReader {
  def main(args: Array[String]) {
    import org.apache.spark.SparkConf
    val conf = new SparkConf().setAppName("Batch File Reader").setMaster("local[2]").set("spark.executor.memory", "1g")
    val sc = new SparkContext(conf)
    val textFile = sc.textFile("file:///users.csv")
//    val textfileMap = textFile.

    val genderMap = textFile.flatMap(line=>line.split(","))


//    val countPrep = tokenizedFileData.map(word=>(word, 1))
//    val counts = countPrep.reduceByKey((accumValue, newValue)=>accumValue + newValue)
//    val sortedCounts = counts.sortBy(kvPair=>kvPair._2, false)


    textFile.saveAsTextFile("file:///PluralsightData/ReadMeWordCountViaApp")
  }
}