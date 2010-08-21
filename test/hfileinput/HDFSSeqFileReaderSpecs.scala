package hfileinput

import hfileinput.testutil.{TestResult, HDFSTestResult, TestUtilities}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.io.{BytesWritable, SequenceFile, Text}

object HDFSSeqFileReaderSpecs {

  def main(arguments: Array[String]) {
    val rowCount = if (arguments.length == 1)  Integer.parseInt(arguments(0)) else 10
    val tests = new TestData(rowCount)

    for (test <- tests.Descriptors) {
      if (! test.useLZO) {
        for (bufferSize <- TestDataDescriptor.hdfsReadBufferSizes) {
          TestUtilities.tryToRunGC
          val result = runTest(test, bufferSize)
          println(result)
        }
      }
    }
  }

  def runTest(test: TestDataDescriptor, bufferSize: Int): TestResult = {
    val config = HBaseConfiguration.create
    val testResult = new HDFSTestResult(test)

    testResult.startTime = System.currentTimeMillis
    testResult.readBufferSize = bufferSize

    var startKey: String = null
    var endKey: String   = null
    var count = 0

    val path = test.hdfsPath
    val fs = path.getFileSystem(config)

    val key = new BytesWritable
    val value = new Text

    config.setInt("io.file.buffer.size", bufferSize)
    val reader = new SequenceFile.Reader(fs, path, config)
    var hasNext = reader.next(key, value)

    testResult.startKey = TestDataDescriptor.rowIdToString(key.getBytes, 0, key.getLength)

    while(hasNext) {
      val columns = value.toString.split("\t")
      require(columns.length == test.nQualifiers)

      count += 1
      hasNext = reader.next(key, value)
    }

    testResult.endKey = TestDataDescriptor.rowIdToString(key.getBytes, 0, key.getLength)
    testResult.rowCount = count
    testResult.endTime = System.currentTimeMillis

    testResult.totalFileSize = fs.getFileStatus(path).getLen

    testResult
  }

}
