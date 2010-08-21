package hfileinput

import hfileinput.testutil.{HBaseTestUtilities, HDFSTestUtilities}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{HTable, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.io.{BytesWritable, IOUtils, SequenceFile, Text}

object LoadTestData {

  def main(arguments: Array[String]) {
    val config = HBaseConfiguration.create

    val rowCount = if (arguments.length >= 1)  Integer.parseInt(arguments(0)) else 10
    val tests = new TestData(rowCount)

    HDFSTestUtilities.deleteHDFSFile(config, TestDataDescriptor.hdfsBaseDir)

    for (test <- tests.Descriptors) {
      if (! test.useLZO) {
        loadHDFS(config, test)
      }
      loadHBase(config, test)
    }
  }

  def loadHDFS(config: Configuration, test: TestDataDescriptor): Unit = {
    val path = test.hdfsPath
    val fs = path.getFileSystem(config)

    val key = new BytesWritable
    val value = new Text

    val writer = SequenceFile.createWriter(fs, config, path,
                  key.getClass, value.getClass, SequenceFile.CompressionType.NONE)

    try {

      for (rowNumber <- 1 to test.nRows) {
        val rowId = test.generateRowId(rowNumber)
        key.set(rowId, 0, rowId.length)
        test.populateValues(value)

        writer.append(key, value)

        if (rowNumber % 10000 == 0) {
          println("HDFS %s - Wrote %,d records.".format(test.tableName, rowNumber))
        }
      }

    } finally {
      IOUtils.closeStream(writer)
    }
  }

  def loadHBase(config: Configuration, test: TestDataDescriptor): Unit = {
    HBaseTestUtilities.createOrTruncateTable(config,
        Bytes.toBytes(test.tableName), test.family, test.useLZO)

    val writeToWAL = false
    val table = new HTable(config, test.tableName)
    table.setAutoFlush(false)

    for (version <- 1 to test.nVersions) {
      writeHBaseRows(table, test)
    }

    table.flushCommits

    Thread.sleep(5000)
    HBaseTestUtilities.flushMemStore(config, table)
  }

  def writeHBaseRows(table: HTable, test: TestDataDescriptor) = {
    val writeToWAL = false

    for (rowNumber <- 1 to test.nRows) {
      val rowId = test.generateRowId(rowNumber)
      val put = new Put(rowId)
      put.setWriteToWAL(writeToWAL)

      test.populateValues(put)
      table.put(put)

      if (rowNumber % 10000 == 0) {
        println("HBase %s - Wrote %,d records.".format(test.tableName, rowNumber))
      }
    }
  }

}

