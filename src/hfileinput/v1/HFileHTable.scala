package hfileinput.v1

import hfileinput.StoreInfo

import org.apache.hadoop.hbase.client.{ResultScanner, Scan, HTable}
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.conf.Configuration

import java.util.TreeSet

object HFileHTable {

  val UseKeyValueRowScannersKey = "useKeyValueRowScanners"
  val UseKeyValueRowScannersKeyDefault = true

  val CacheEverything = "cacheEverything"
  val CacheEverythingDefault = false
}

class HFileHTable(val config: Configuration, val tableName: Array[Byte])
        extends HTable(config, tableName) {

  override def getScanner(scan: Scan): ResultScanner = {
    val tableDescriptor   = getTableDescriptor
    val familyDescriptors = tableDescriptor.getColumnFamilies

    // TODO Support multiple column families
    val storeInfo = new StoreInfo(getConfiguration, tableDescriptor, familyDescriptors(0),
      scan.getStartRow, scan.getStopRow, new KeyValue.KVComparator)

    val columns = new TreeSet[Array[Byte]]

    val scanners = storeInfo.createKeyValueScanners(null)
    if (scanners.size == 0)
      throw new IllegalStateException("There is no store available.")

    import scala.collection.JavaConversions._
    val kvScanner = new HFileKeyValueScanner(storeInfo, scanners, scan, columns)

    new HFileResultScanner(kvScanner)
  }

}