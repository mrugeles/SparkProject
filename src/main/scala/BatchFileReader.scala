
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

    val countPrep = gendersRDD.map(word=>(word, 1))
    val counts = countPrep.reduceByKey((accumValue, newValue)=>accumValue + newValue)
    val  total = lines.count()

    val resultRDD = counts.map(
      line =>{
        val percentage: Float = line._2*100/total
        (line._1, percentage)
      }
    )
    resultRDD
  }

  def sortUsersBySalary(usersRDD: RDD[String]): RDD[String] = {

    val salariesRDD = usersRDD.map(
      line =>{
        val data = line.split(",")
        (data(5), line)
      }
    )

    val sortedRDD = salariesRDD.sortByKey(false)
    val resultRDD = sortedRDD.map(
      line => {
        (line._2)
      }
    )

    val topTenRDD = resultRDD.take(10)
    sc.parallelize(topTenRDD)

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

  }

}