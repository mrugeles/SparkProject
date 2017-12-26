package main

import com.holdenkarau.spark.testing.{RDDComparisons, SharedSparkContext}
import org.scalatest.FunSuite

/**
  * Created by mrugeles on 26/12/2017.
  */
class BatchFileReaderTest extends FunSuite with SharedSparkContext with RDDComparisons {
  test("test RDDComparisons") {

    val users = List(
      "1,Perceval,Brokenbrow,pbrokenbrow0@bloomberg.com,Male,$4298.61",
      "2,Sibley,Terne,sterne1@ft.com,Female,$8984.15",
      "3,Priscella,Nornasell,pnornasell2@intel.com,Female,$2775.81",
      "4,Carlyle,De Launde,cdelaunde3@tamu.edu,Male,$5670.45"
    )
    val usersRDD = sc.parallelize(users)

    val expectedRDD = sc.parallelize(List(("Male", 50), ("Female", 50)))
    val job = new BatchFileReader(sc)

    val resultRDD = job.getGenderCount(usersRDD)


    assert(None === compareRDD(expectedRDD, resultRDD)) // succeed
    //assert(None === compareRDDWithOrder(expectedRDD, resultRDD)) // Fail

    //assertRDDEquals(expectedRDD, resultRDD) // succeed
    //assertRDDEqualsWithOrder(expectedRDD, resultRDD) // Fail
  }
}
