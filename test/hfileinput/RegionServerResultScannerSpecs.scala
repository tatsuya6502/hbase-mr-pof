package hfileinput

import hfileinput.testutil.{TestUtilities, HFileKVRowScannerTestResult, TestResult}

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.JavaConversions._

object RegionServerResultScannerSpecs {

  def main(arguments: Array[String]) {
    val rowCount = if (arguments.length == 1)  Integer.parseInt(arguments(0)) else 10
    val tests = new TestData(rowCount)

    for (test <- tests.Descriptors) {
      TestUtilities.tryToRunGC
      val result = runTest(test)
      println(result)
    }
  }

  def runTest(test: TestDataDescriptor): TestResult = {
    val config = HBaseConfiguration.create
    val testResult = new HFileKVRowScannerTestResult(test)

    testResult.startTime = System.currentTimeMillis

    config.set(TableInputFormat.INPUT_TABLE, test.tableName)
    config.setInt(TableInputFormat.SCAN_CACHEDROWS, 500)
    config.setInt(TableInputFormat.SCAN_MAXVERSIONS, 1)

    val inputFormat = new TableInputFormat
    inputFormat.setConf(config)
    val splits = inputFormat.getSplits(null)

    testResult.nRegions = splits.length
//    testResult.totalFileSize = TestUtilities.getTotalStoreFileSize(storeInfoList)


    val family = test.family
    val qualifiers = test.qualifiers

    var startKey: String = null
    var endKey: String   = null
    var count = 0

    for (split <- splits) {
      val trr = inputFormat.createRecordReader(split, null)
      var hasNext = trr.nextKeyValue
      if (startKey == null) {
        startKey = TestDataDescriptor.rowIdToString(trr.getCurrentKey.get)
      }

      while(hasNext) {
        val result = trr.getCurrentValue

        qualifiers.foreach{ (qualifier) =>
          val value = Bytes.toString(result.getValue(family, qualifier))
          require(value.length > 0)
        }

        count += 1

        hasNext = trr.nextKeyValue
      }

      endKey = TestDataDescriptor.rowIdToString(trr.getCurrentKey.get)
    }
    testResult.endTime = System.currentTimeMillis

    testResult.rowCount = count
    testResult.startKey = startKey
    testResult.endKey = endKey

    testResult.nFiles = 1
    testResult.totalFileSize = 0

    testResult
  }
}
