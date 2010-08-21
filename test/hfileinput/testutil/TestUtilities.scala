package hfileinput.testutil

import hfileinput.StoreInfo

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableSplit}
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.JavaConversions._
import java.util.Random

object TestUtilities {
  val random = new Random()

  /* from YCSB */
  def generateAsciiString(length: Int): String = {
    val interval: Int ='~' - ' ' + 1

    val buf = new Array[Byte](length)
    random.nextBytes(buf)
    for (i <- 0 until length) {
      if (buf(i) < 0) {
        buf(i) = ((-buf(i) % interval) + ' ').asInstanceOf[Byte]
      } else {
        buf(i) = ((buf(i) % interval) + ' ').asInstanceOf[Byte]
      }
    }

    new String(buf)
  }

  def digits(n: Int, digit: Int): String = {
    val nStr = n.toString
    val nStrLen = nStr.length

    if (nStrLen == digit) {
      nStr

    } else if (nStrLen > digit) {
      val result = nStr.substring(nStrLen - digit, nStrLen)
      if (result.length != digit) throw new IllegalStateException
      result
    } else {
      val buf = new StringBuilder
      for (i <- 1 to digit - nStr.length) {
        buf.append("0")
      }
      buf.append(nStr)

      val result = buf.toString
      if (result.length != digit) throw new IllegalStateException
      result
    }
  }

  def getStoreInfoList(config: Configuration, table: HTable): List[StoreInfo] = {
    val tableDescriptor   = table.getTableDescriptor
    val familyDescriptors = tableDescriptor.getColumnFamilies

    config.set(TableInputFormat.INPUT_TABLE, Bytes.toString(table.getTableName))
    val inputFormat = new TableInputFormat
    inputFormat.setConf(config)
    val splits = inputFormat.getSplits(null)

    val storeInfoList = splits.map{ (split) =>
      val tableSplit = split.asInstanceOf[TableSplit]
      new StoreInfo(config, tableDescriptor, familyDescriptors(0),
                    tableSplit.getStartRow, tableSplit.getEndRow, new KeyValue.KVComparator)
    }.toList

//    println("splits %d, storeInfoList %d".format(splits.length, storeInfoList.length))

    storeInfoList
  }

  def getTotalStoreFileSize(storeInfoList: List[StoreInfo]): Long = {
    storeInfoList.map(_.totalStoreFileSize).reduceLeft(_ + _)
  }

  def tryToRunGC(): Unit = {
    System.gc
    Thread.sleep(5000)
  }

  def displayStringFor(kv: KeyValue): String = {
    "row: %s, column: %s, timestamp: %d, value: \"%s\"".format(
      Bytes.toString(kv.getRow),
      Bytes.toString(kv.getQualifier),
      kv.getTimestamp,
      Bytes.toString(kv.getValue))
  }

  def printRegionInfo(table: HTable, rowId: String): Unit = {
    val regionLocation = table.getRegionLocation(rowId)
    val regionInfo = regionLocation.getRegionInfo

    println("======================================================================")
    println("rowId:              %s".format(rowId))
    println("regionId:           %s".format(regionInfo.getRegionId))
    println("regionNameAsString: %s".format(regionInfo.getRegionNameAsString))
    println("startKey:           %s".format(Bytes.toString(regionInfo.getStartKey)))
    println("endKey:             %s".format(Bytes.toString(regionInfo.getEndKey)))
    println("isSplit:            %s".format(regionInfo.isSplit))
  }

}
