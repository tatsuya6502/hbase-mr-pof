package hfileinput

import hfileinput.util.StopWatch
import hfileinput.v1.{HFileResultScanner, HFileKeyValueScanner}
import hfileinput.testutil.{TestResult, HFileKVRowScannerTestResult, TestUtilities}

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{HTable, Result, Scan}
import org.apache.hadoop.hbase.io.hfile.{BlockCache, LruBlockCache}
import org.apache.hadoop.hbase.util.Bytes

import java.util.TreeSet
import scala.collection.JavaConversions._

object HFileKVScannerSpecs {

  def main(arguments: Array[String]) {
    val rowCount = if (arguments.length >= 1)  Integer.parseInt(arguments(0)) else 10
    val useCache = arguments.length == 2  &&  "useCache".equals(arguments(1))

    val tests = new TestData(rowCount)

    for (test <- tests.Descriptors) {
      TestUtilities.tryToRunGC
      val result = runTest(test, useCache)
      println(result)
    }
  }

  def runTest(testDataDesc: TestDataDescriptor, useCache: Boolean): TestResult = {
    val config = HBaseConfiguration.create
    val testResult = new HFileKVRowScannerTestResult(testDataDesc)

    val cacheSize: Long = (1.5 * 1024 * 1024 * 1024).asInstanceOf[Long]
    val cache: Option[LruBlockCache] = if (useCache)
                                         Some(new LruBlockCache(cacheSize, 64 * 1024, false))
                                       else
                                         None

    val table = new HTable(testDataDesc.tableName)
    val storeInfoList = TestUtilities.getStoreInfoList(config, table)
    storeInfoList.map(_.setRootDir(TestDataDescriptor.v1BaseDir))

    testResult.nRegions = storeInfoList.length
    testResult.nFiles = 0
    testResult.totalFileSize = TestUtilities.getTotalStoreFileSize(storeInfoList)

    val nRuns = if (cache.isDefined) 2 else 1
    for (i <- 1 to nRuns) {
      scanFullTable(storeInfoList, testDataDesc, testResult, cache.getOrElse(null))

      if (cache.isDefined) {
        val stats = cache.get.getStats
        println("%,d hits, %,d misses. %n"
                .format(stats.getHitCount, stats.getMissCount))
      }
    }

    if (cache.isDefined)
      cache.get.shutdown


    testResult
  }

  def scanFullTable(storeInfoList: List[StoreInfo],
                    testDataDesc: TestDataDescriptor,
                    testResult: HFileKVRowScannerTestResult,
                    cache: BlockCache): Unit = {

    testResult.startTime = System.currentTimeMillis

    val family = testDataDesc.family
    val qualifiers = testDataDesc.qualifiers

    var startKey: String = null
    var endKey:   String = null
    var count = 0


//    val createScannerSW = new StopWatch("Create scanners")
//    val scannerSW = new StopWatch("HFileKeyValueRowScanner#nextRow()")
//    val convertSW = new StopWatch("KeyValueSet#getValueAsString()")
//    val hfileRowScannerSW = new StopWatch("HFileRowScanner")

    for (storeInfo <- storeInfoList) {
//      println("[%s - %s)".format(Bytes.toString(storeInfo.getStartRow),
//                               Bytes.toString(storeInfo.getEndRow)))
//      println(storeInfo.getStoreFilePaths.mkString("\n"))

//      createScannerSW.start

      val columns = new TreeSet[Array[Byte]]

      val scan = new Scan
      scan.setStartRow(storeInfo.getStartRow)
      scan.setStopRow(storeInfo.getEndRow)
      scan.setMaxVersions(1)

      val scanners = storeInfo.createKeyValueScanners(cache)
      val kvScanner = new HFileKeyValueScanner(storeInfo, scanners, scan, columns)
      val scanner = new HFileResultScanner(kvScanner)

      testResult.nFiles += scanners.length
      

//      createScannerSW.stop

      var hasNext = false
      var result: Result = null
      var lastResult: Result = null

//      scannerSW.start
      result = scanner.next
//      scannerSW.stop

      while (result.size > 0) {
        count += 1

        if (startKey == null  &&  result.size > 0) {
          startKey = TestDataDescriptor.rowIdToString(result.getRow)
        }

//        convertSW.start
        qualifiers.foreach{ (qualifier) =>
          val value = Bytes.toString(result.getValue(family, qualifier))
          require(value != null  &&  value.length == testDataDesc.valueLength)
//          println("%s:%s = %s".format(Bytes.toString(result.getRow), Bytes.toString(qualifier), value))
        }
//        convertSW.stop

        lastResult = result

//        scannerSW.start
        result = scanner.next
//        scannerSW.stop
      }

      endKey = TestDataDescriptor.rowIdToString(lastResult.getRow)

    }

    testResult.rowCount = count
    testResult.startKey = startKey
    testResult.endKey   = endKey
    testResult.endTime  = System.currentTimeMillis
  }
}

