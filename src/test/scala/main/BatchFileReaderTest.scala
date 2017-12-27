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

    val expectedRDD = sc.parallelize(List(("Male", 50F), ("Female", 50F)))
    val job = new BatchFileReader(sc)

    val resultRDD = job.getGenderCount(usersRDD)

    assert(None === compareRDD(expectedRDD, resultRDD)) // succeed
    //assert(None === compareRDDWithOrder(expectedRDD, resultRDD)) // Fail

//    assertRDDEquals(expectedRDD, resultRDD) // succeed
//    assertRDDEqualsWithOrder(expectedRDD, resultRDD) // Fail
  }

  test("test SortBySalary") {

    val users = List(
      "1,Perceval,Brokenbrow,pbrokenbrow0@bloomberg.com,Male,$4298.61",
      "2,Sibley,Terne,sterne1@ft.com,Female,$8984.15",
      "3,Priscella,Nornasell,pnornasell2@intel.com,Female,$2775.81",
      "4,Carlyle,De Launde,cdelaunde3@tamu.edu,Male,$5670.45",
      "5,Baryram,Shepcutt,bshepcutt4@umn.edu,Male,$3588.73",
      "6,Pearline,O'Kennavain,pokennavain5@nifty.com,Female,$7287.04",
      "7,Francklyn,Glandfield,fglandfield6@fastcompany.com,Male,$7300.84",
      "8,Alix,Plackstone,aplackstone7@ed.gov,Male,$9285.78",
      "9,Chane,Worstall,cworstall8@icq.com,Male,$2066.03",
      "10,Cleon,felip,cfelip9@homestead.com,Male,$7978.32",
      "11,Olenka,Baldacchi,obaldacchia@forbes.com,Female,$9320.61",
      "12,Eileen,Kilsby,ekilsbyb@apple.com,Female,$9625.42",
      "13,Kinnie,Mennithorp,kmennithorpc@typepad.com,Male,$4475.70",
      "14,Lyn,Mapledoram,lmapledoramd@spotify.com,Female,$7142.35",
      "15,Jillana,Arckoll,jarckolle@walmart.com,Female,$7922.16",
      "16,Jourdan,Giddens,jgiddensf@aol.com,Female,$5792.63",
      "17,Celia,Grote,cgroteg@t.co,Female,$5274.53",
      "18,Itch,Stavers,istaversh@ftc.gov,Male,$8634.44",
      "19,Jude,Ecles,jeclesi@canalblog.com,Male,$3380.87",
      "20,Elspeth,Fluin,efluinj@netvibes.com,Female,$4081.08"
    )
    val usersRDD = sc.parallelize(users)

    val expectedRDD = sc.parallelize(List(
      "12,Eileen,Kilsby,ekilsbyb@apple.com,Female,$9625.42",
      "11,Olenka,Baldacchi,obaldacchia@forbes.com,Female,$9320.61",
      "8,Alix,Plackstone,aplackstone7@ed.gov,Male,$9285.78",
      "2,Sibley,Terne,sterne1@ft.com,Female,$8984.15",
      "18,Itch,Stavers,istaversh@ftc.gov,Male,$8634.44",
      "10,Cleon,felip,cfelip9@homestead.com,Male,$7978.32",
      "15,Jillana,Arckoll,jarckolle@walmart.com,Female,$7922.16",
      "7,Francklyn,Glandfield,fglandfield6@fastcompany.com,Male,$7300.84",
      "6,Pearline,O'Kennavain,pokennavain5@nifty.com,Female,$7287.04",
      "14,Lyn,Mapledoram,lmapledoramd@spotify.com,Female,$7142.35"))

    val job = new BatchFileReader(sc)
    val resultRDD = job.sortUsersBySalary(usersRDD)

    assert(None === compareRDD(expectedRDD, resultRDD)) // succeed
    //assert(None === compareRDDWithOrder(expectedRDD, resultRDD)) // Fail

    //    assertRDDEquals(expectedRDD, resultRDD) // succeed
    //    assertRDDEqualsWithOrder(expectedRDD, resultRDD) // Fail
  }


}
