package hfileinput.testutil

import hfileinput.TestDataDescriptor
import hfileinput.util.StopWatch

trait TestResult {

  val descriptor: TestDataDescriptor

  var nRegions: Int = -1
  var nFiles:   Int = -1
  var totalFileSize: Long = -1L // size in byte

  var startKey: String = ""
  var endKey: String   = ""
  var rowCount: Int    = -1

  var startTime: Long  = -1L
  var endTime: Long    = -1L
  def elapseTime: Long = endTime - startTime

  // bytes / second
  def keyValueBandwidth: Double = totalFileSize / (elapseTime / 1000.0d)
  def valueBandwidth: Double =    descriptor.rowValueLength * rowCount / (elapseTime / 1000.0d)

  var stopWatches: List[StopWatch] = List[StopWatch]()

  def bytes2MB(bytes: Long): Double   = bytes / 1024.0d / 1024.0d
  def bytes2MB(bytes: Double): Double = bytes / 1024.0d / 1024.0d

  def bytes2KB(bytes: Long): Double   = bytes / 1024.0d
  def bytes2KB(bytes: Double): Double = bytes / 1024.0d

  override def toString(): String =
    bannerTemplate.format(descriptor.tableName, nRegions, nFiles, 
                          bytes2MB(totalFileSize),
                          startKey, endKey, rowCount, elapseTime,
                          bytes2MB(keyValueBandwidth),
                          bytes2MB(valueBandwidth))

    val bannerTemplate = """
================================================
Test          %s
regions:      %,d
files:        %,d
file size:    %,.2f MB
================================================
start key:    %s
end key:      %s
row count:    %,d
elapse time:  %,d ms
bandwidth kv: %,.2f MB/s
bandwidth  v: %,.2f MB/s
================================================""".trim


}
