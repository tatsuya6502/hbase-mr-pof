
package hfileinput.testutil

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HTableDescriptor, HColumnDescriptor}
import org.apache.hadoop.hbase.TableNotFoundException
import org.apache.hadoop.hbase.client.{HTable, HBaseAdmin}
import org.apache.hadoop.hbase.io.hfile.Compression

object HBaseTestUtilities {

  def createOrTruncateTable(config: Configuration,
                            tableName: String, family: String, useLZO: Boolean): HTable = {
    createOrTruncateTable(config, Bytes.toBytes(tableName), Bytes.toBytes(family), useLZO)
  }


  def createOrTruncateTable(config: Configuration,
                            tableName: Array[Byte], family: Array[Byte], useLZO: Boolean): HTable = {
    val admin = new HBaseAdmin(config)

    if (admin.isTableAvailable(tableName)) {
      try {
        admin.disableTable(tableName)
        admin.deleteTable(tableName)
      } catch {
        case e: TableNotFoundException => // do nothing
      }
    }

    val tableDescriptor = new HTableDescriptor(tableName)
    val familyDescriptor = new HColumnDescriptor(family)
    if (useLZO) {
      familyDescriptor.setCompressionType(Compression.Algorithm.LZO)
    }
    tableDescriptor.addFamily(familyDescriptor)

    admin.createTable(tableDescriptor)

    new HTable(config, tableName)
  }

  def flushMemStore(config: Configuration, table: HTable): Unit = {
    flushMemStore(config, table.getTableName)
  }

  def flushMemStore(config: Configuration, tableName: Array[Byte]): Unit = {
    val admin = new HBaseAdmin(config)
    admin.flush(tableName)
  }



}
