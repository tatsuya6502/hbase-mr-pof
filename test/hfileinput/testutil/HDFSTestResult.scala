package hfileinput.testutil

import hfileinput.TestDataDescriptor

class HDFSTestResult(desc: TestDataDescriptor) extends TestResult {

  val descriptor = desc

  var readBufferSize: Int = 0

  override def toString(): String =
    bannerTemplate.format(descriptor.tableName,
                          bytes2MB(totalFileSize),
                          bytes2KB(readBufferSize),
                          startKey, endKey, rowCount, elapseTime,
                          bytes2MB(keyValueBandwidth),
                          bytes2MB(valueBandwidth))

  override val bannerTemplate = """
================================================
Test          %s
file size:    %,.2f MB
read buffer   %,.2f KB
================================================
start key:    %s
end key:      %s
row count:    %,d
elapse time:  %,d ms
bandwidth kv: %,.2f MB/s
bandwidth  v: %,.2f MB/s
================================================""".trim

}
