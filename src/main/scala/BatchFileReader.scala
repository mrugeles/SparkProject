
package main

import main.BatchFileReader.getClass
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

class BatchFileReader(sc: SparkContext){

  def run() = {
    val textFile = sc.textFile(getClass.getResource("/users.csv").getPath)
  }

  def getGenderCount(lines: RDD[String]): RDD[(String, Float)]  = {

    //ej: Array(('male', 45), ('female', 55))
    val gendersRDD = lines.map(
      line =>{
        val data = line.split(",")
        data(4)
      }
    )

    null
  }

}

object BatchFileReader {
  def main(args: Array[String]) {
    import org.apache.spark.SparkConf
    val conf = new SparkConf()
      .setAppName("Batch File Reader")
      .setMaster("local[2]")
      .set("spark.executor.memory", "1g")
    val sc = new SparkContext(conf)


    val job = new BatchFileReader(sc)

    job.run()
//    val textfileMap = textFile.


    //val genderMap = textFile.flatMap(line=>line.split(","))


//    val countPrep = tokenizedFileData.map(word=>(word, 1))
//    val counts = countPrep.reduceByKey((accumValue, newValue)=>accumValue + newValue)
//    val sortedCounts = counts.sortBy(kvPair=>kvPair._2, false)


    //textFile.saveAsTextFile("file:///PluralsightData/ReadMeWordCountViaApp")
  }

}