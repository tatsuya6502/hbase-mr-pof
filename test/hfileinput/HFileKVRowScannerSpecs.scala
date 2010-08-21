package hfileinput

import hfileinput.v2.{KeyValueHashSet, KeyValueRowMergeScanner}
import hfileinput.util.StopWatch

import hfileinput.testutil.{TestResult, HFileKVRowScannerTestResult, TestUtilities}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.io.hfile.{BlockCache, LruBlockCache, HFileRowScanner}
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.JavaConversions._

object HFileKVRowScannerSpecs {

  def main(arguments: Array[String]) {
    val rowCount = if (arguments.length == 1)  Integer.parseInt(arguments(0)) else 10
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
    storeInfoList.map(_.setRootDir(TestDataDescriptor.v2BaseDir))

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

    var startKey: String = null
    var endKey: String   = null
    var count = 0

    val createScannerSW = new StopWatch("Create scanners")
    val scannerSW = new StopWatch("HFileKeyValueRowScanner#nextRow()")
    val convertSW = new StopWatch("KeyValueSet#getValueAsString()")
    val hfileRowScannerSW = new StopWatch("HFileRowScanner")

//    val hfileReadSW = new StopWatch("HFileRead")
//    val loadBlockSW = new StopWatch("loadBlock")
//    val updateCurrentSW = new StopWatch("updateCurrent")
//    val markerSW = new StopWatch("marker")
//    val keyValueSetAddSW = new StopWatch("KeyValueSet#Add")

    for (storeInfo <- storeInfoList) {
//      println("[%s - %s)".format(Bytes.toString(storeInfo.getStartRow),
//                               Bytes.toString(storeInfo.getEndRow)))
//      println(storeInfo.getStoreFilePaths.mkString("\n"))

      createScannerSW.start
      val scanners = storeInfo.createKeyValueRowScanners(cache)
      for (s <- scanners) {
        val hs = s.asInstanceOf[HFileRowScanner]
//        hs.hfileReaderSW = hfileReadSW
//        hs.loadBlockSW = loadBlockSW
//        hs.updateCurrentSW = updateCurrentSW
//        hs.markerSW = markerSW
//        hs.keyValueSetAddSW = keyValueSetAddSW
        hs.seek
      }

      testResult.nFiles += scanners.length

      val scanner = new KeyValueRowMergeScanner(scanners)
      createScannerSW.stop

      val row = new KeyValueHashSet
      val familyAndQualifiers = testDataDesc.familyAndQualifiers

      var hasNext = false

      do {
        row.clear

//        scannerSW.start
        hasNext = scanner.nextRow(row)
//        scannerSW.stop
        count += 1

        if (startKey == null) {
          val rowID = row.getRow
          startKey = TestDataDescriptor.rowIdToString(rowID.get, rowID.getOffset, rowID.getLength)
        }

//        convertSW.start
        familyAndQualifiers.foreach{ (fq) =>
          val value = row.getValueAsString(fq)
          require(value != null  &&  value.length == testDataDesc.valueLength)

//          val rowID = row.getRow
//          val rowStr = Bytes.toString(rowID.get, rowID.getOffset, rowID.getLength)
//          println("%s:%s = %s".format(rowStr, Bytes.toString(fq), value))
        }
//        convertSW.stop

      } while (hasNext)

      val rowID = row.getRow
      endKey = TestDataDescriptor.rowIdToString(rowID.get, rowID.getOffset, rowID.getLength)
    }

    testResult.rowCount = count
    testResult.startKey = startKey
    testResult.endKey   = endKey
    testResult.endTime  = System.currentTimeMillis
  }
}
