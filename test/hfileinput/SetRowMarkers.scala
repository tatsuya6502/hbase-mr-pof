package hfileinput

import hfileinput.v2.KeyValue2
import hfileinput.v3.KeyValue3
import hfileinput.testutil.HDFSTestUtilities

import org.apache.hadoop.hbase.{KeyValue, HBaseConfiguration}
import org.apache.hadoop.hbase.io.hfile.{HFile, HFileScanner, HFileRowScanner}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration

import java.util.{Map => JavaMap}
import scala.collection.JavaConversions._

object SetRowMarkers {

  val EmptyBytes = Array[Byte]()

  def main(arguments: Array[String]) {
    val config = HBaseConfiguration.create

    val rowCount = if (arguments.length == 1)  Integer.parseInt(arguments(0)) else 10
    val tests = new TestData(rowCount)

    HDFSTestUtilities.deleteDirectory(config, TestDataDescriptor.v1BaseDir)
    HDFSTestUtilities.deleteDirectory(config, TestDataDescriptor.v2BaseDir)
    HDFSTestUtilities.deleteDirectory(config, TestDataDescriptor.v3BaseDir)

    HDFSTestUtilities.createDirectory(config, TestDataDescriptor.v1BaseDir)
    HDFSTestUtilities.createDirectory(config, TestDataDescriptor.v2BaseDir)
    HDFSTestUtilities.createDirectory(config, TestDataDescriptor.v3BaseDir)

    for (test <- tests.Descriptors) {
      val table = new HTable(config, test.tableName)

      for (storeInfo <- StoreInfo.getStoreInfoEntries(config, table)) {

        for (inputPath <- storeInfo.getStoreFilePaths) {
          val v1Path = duplicateV1Files(config, inputPath)
          createV2AndV3Files(config, v1Path, test.useLZO)
        }

      }
    }
  }

  def duplicateV1Files(config: Configuration, inputPath: Path): Path = {
    val v1Path = TestDataDescriptor.translateToV1OutputPath(inputPath)

    println("Copying HFile %nfrom: %s %nto:   %s%n"
      .format(inputPath.toUri, v1Path.toUri))

    HDFSTestUtilities.duplicateFile(config, inputPath, v1Path)
    v1Path
  }

  def createV2AndV3Files(config: Configuration, inputPath: Path, useLZO: Boolean): Unit = {
    val (v1scanner, v2writer, v3writer, fileInfo) =
                createScannerAndWriters(config, inputPath, useLZO)

    val (rowCount, kvCount) = convertRows(v1scanner, v2writer, v3writer)

    println("Wrote %,d rows and %,d KeyValues.%n".format(rowCount, kvCount))

    appendFileInfo(v2writer, v3writer, fileInfo)

    v2writer.close
    v3writer.close
  }

  def createScannerAndWriters(config: Configuration, inputPath: Path, useLZO: Boolean):
       (HFileScanner, HFile.Writer, HFile.Writer, JavaMap[Array[Byte], Array[Byte]]) = {

    val fs = inputPath.getFileSystem(config)

    val cache = null
    val inMemory = false

    val reader = new HFile.Reader(fs, inputPath, cache, inMemory)
    val fileInfo = reader.loadFileInfo

    val v1scanner = reader.getScanner(false, false)
    v1scanner.seekTo

    val v2Path = TestDataDescriptor.translateV1ToV2OutputPath(inputPath)
    val v3Path = TestDataDescriptor.translateV1ToV3OutputPath(inputPath)

    val compression = if (useLZO) "lzo" else "none"
    val comparator = new KeyValue2.KeyComparator

    val v2Writer = new HFile.Writer(fs, v2Path, HFile.DEFAULT_BLOCKSIZE, compression, comparator)
    val v3Writer = new HFile.Writer(fs, v3Path, HFile.DEFAULT_BLOCKSIZE, compression, comparator)

    println("Converting HFile %nfrom: %s %nto:   %s%nto:   %s"
      .format(inputPath.toUri, v2Path.toUri, v3Path.toUri))

    (v1scanner, v2Writer, v3Writer, fileInfo)
  }

  def convertRows(scanner: HFileScanner,
                  v2Writer: HFile.Writer, v3Writer: HFile.Writer): (Int, Int) = {

    var kvCount = 0
    var rowCount = 0
    var currentRowID = EmptyBytes
    var currentQual  = EmptyBytes

    var kv = scanner.getKeyValue

    while (kv != null) {
      kvCount += 1

      val isSameRow = KeyValue2.arrayEquals(currentRowID, kv.getRow)
      val isDuplicateColumn = KeyValue2.arrayEquals(currentQual, kv.getQualifier)

      if (! isSameRow) {
        currentRowID = kv.getRow
        currentQual  = EmptyBytes
        rowCount    += 1
      }

      if (! isDuplicateColumn)
        currentQual = kv.getQualifier

      val (kv2, kv3) = createKeyValues(kv, isSameRow, isDuplicateColumn)

      v2Writer.append(kv2)
      v3Writer.append(kv3)

      scanner.next
      kv = scanner.getKeyValue
    }

    (rowCount, kvCount)
  }

  def createKeyValues(kv: KeyValue,
                      isSameRow: Boolean,
                      isDuplicateColumn: Boolean): (KeyValue2, KeyValue3) = {

    val kvQualifier = kv.getQualifier
    val markerBytes = getMarkerBytes(kv.getFamily, kvQualifier, isSameRow, isDuplicateColumn)

    val v2Qualifier = kvQualifier ++ markerBytes

    val kv2 = new KeyValue2(kv.getRow, kv.getFamily, v2Qualifier,
                            kv.getTimestamp, KeyValue.Type.codeToType(kv.getType), kv.getValue)


    val (v3Row, v3Family) = if (isSameRow) (EmptyBytes, EmptyBytes) else (kv.getRow, kv.getFamily)
    val v3Qualifier = if (isDuplicateColumn) markerBytes  else v2Qualifier

    val kv3 = new KeyValue3(v3Row, v3Family, v3Qualifier,
                            kv.getTimestamp, KeyValue.Type.codeToType(kv.getType), kv.getValue)

    (kv2, kv3)
  }


  def getMarkerBytes(family: Array[Byte], qualifier: Array[Byte],
                    isSameRow: Boolean, isDuplicateColumn: Boolean): Array[Byte] = {

    val hash = KeyValue2.calculateQualifierHashCode(family, qualifier)

    val hashHigh8 = hash & 0x00000FF0
    val hashLow4  = hash & 0x0000000F
    val hash12    = ((hashHigh8 << 4) + hashLow4).toShort

    val marker = Bytes.toBytes(hash12)

    if (isSameRow)
      marker(1) = ((marker(1) ^ 16) & 0xFF).asInstanceOf[Byte]

    if (isDuplicateColumn)
      marker(1) = ((marker(1) ^ 32) & 0xFF).asInstanceOf[Byte]

    marker
  }

  val RowMarkerSupportedKey   = Bytes.toBytes(HFileRowScanner.ROW_MARKER_SUPPORTED_KEY)
  val RowMarkerSupportedValue = Bytes.toBytes("yes")

  def appendFileInfo(v2Writer: HFile.Writer, v3Writer: HFile.Writer,
                     fileInfo: JavaMap[Array[Byte], Array[Byte]]): Unit = {

    for (entry <- fileInfo.entrySet) {
      val keyString = Bytes.toString(entry.getKey)
      if (! keyString.startsWith("hfile.")) {
        v2Writer.appendFileInfo(entry.getKey, entry.getValue)
        v3Writer.appendFileInfo(entry.getKey, entry.getValue)
      }
    }

    v2Writer.appendFileInfo(RowMarkerSupportedKey, RowMarkerSupportedValue)
    v3Writer.appendFileInfo(RowMarkerSupportedKey, RowMarkerSupportedValue)
  }

}