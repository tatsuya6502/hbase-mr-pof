package hfileinput

import hfileinput.testutil.TestUtilities

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.io.Text

import scala.collection.mutable.ListBuffer

object TestDataDescriptor {

  val host = "hdfs://localhost:9000"
//  val host = "hdfs://hbase0.localdomain:9000"
  val baseDirFrom = "/hbase/"
  val hdfsDir = "/user/hbase/hdfs/"
  val v1Dir = "/user/hbase/hfileV1/"
  val v2Dir = "/user/hbase/hfileV2/"
  val v3Dir = "/user/hbase/hfileV3/"

  val hdfsBaseDir  = new Path(host + hdfsDir)
  val v1BaseDir = new Path(host + v1Dir)
  val v2BaseDir = new Path(host + v2Dir)
  val v3BaseDir = new Path(host + v3Dir)

  def kb(n: Int): Int = n * 1024
  def mb(n: Int): Int = n * 1024 * 1024

  val hdfsReadBufferSizes = List(kb(4), kb(64), kb(128), kb(256), kb(512), mb(1), mb(2), mb(4))

  def translateToV1OutputPath(inputPath: Path): Path =
    new Path(host + inputPath.toUri.getPath.replace(baseDirFrom, v1Dir))

  def translateV1ToV2OutputPath(inputPath: Path): Path =
    new Path(host + inputPath.toUri.getPath.replace(v1Dir, v2Dir))

  def translateV1ToV3OutputPath(inputPath: Path): Path =
    new Path(host + inputPath.toUri.getPath.replace(v1Dir, v3Dir))

  def rowIdToString(rowId: Array[Byte]): String =
    rowIdToString(rowId, 0, rowId.length)

  def rowIdToString(rowId: Array[Byte], offset: Int, length: Int): String = {
    val digitLength = length - Bytes.SIZEOF_LONG
    val digitPart = Bytes.toString(rowId, offset, digitLength)
    val reverseOrderedEpoch = Bytes.toLong(rowId, offset + digitLength)

    digitPart + "@" + reverseOrderedEpoch
  }

}


case class TestDataDescriptor(val tableName: String,
                val nVersions: Int = 1,
                val nRows: Int = 10, val rowLength: Int = 20,
                val nQualifiers: Int = 10, val qualifierLength: Int = 5,
                val rowValueLength: Int = 100,
                val useLZO: Boolean = false) {

  val rowIdPrefix = "r"

  val rowDigitLength = rowLength - (rowIdPrefix.length + Bytes.SIZEOF_LONG)
  require(rowDigitLength > 0)

  require(rowValueLength % nQualifiers == 0)
  val valueLength = rowValueLength / nQualifiers
//  println("valueLength: " + valueLength)

  val family = Bytes.toBytes("f0001")

  lazy val qualifiers = {
    (1 to nQualifiers).map("q" + TestUtilities.digits(_, qualifierLength - 1))
                      .map(Bytes.toBytes(_)).toList }

  lazy val familyAndQualifiers = qualifiers.map( family ++ _ ).toList

  lazy val hdfsPath = new Path(TestDataDescriptor.hdfsBaseDir, tableName)

  def generateRowId(rowNumber: Int): Array[Byte] = {
    val reverseOrderedEpoch = java.lang.Long.MAX_VALUE - System.currentTimeMillis

    (Bytes.toBytes(rowIdPrefix + TestUtilities.digits(rowNumber, rowDigitLength)) ++
     Bytes.toBytes(reverseOrderedEpoch) )
  }

  def populateValues(put: Put): Unit = qualifiers.foreach{ (qual) =>
    val value = TestUtilities.generateAsciiString(valueLength)
//    println("%s = %s".format(Bytes.toString(qual), value))
    put.add(family, qual, Bytes.toBytes(value))
  }

  def populateValues(text: Text): Unit = {
    val values = ListBuffer[String]()
    for (i <- 1 to nQualifiers) {
      values += TestUtilities.generateAsciiString(valueLength)
    }
    text.set(values.mkString("\t"))
  }

}
