package hfileinput

class TestData(val rowCount: Int = 10) {

  val versions = 1

  val TestR20C1010V0500 = new TestDataDescriptor("Test-R20-C10-10-V0500",
                                                  nVersions = versions,
                                                  nRows = rowCount,
                                                  rowLength = 20,
                                                  nQualifiers = 10,
                                                  qualifierLength = 10,
                                                  rowValueLength = 0500)

  val TestR20C1010V1000 = new TestDataDescriptor("Test-R20-C10-10-V1000",
                                                  nVersions = versions,
                                                  nRows = rowCount,
                                                  rowLength = 20,
                                                  nQualifiers = 10,
                                                  qualifierLength = 10,
                                                  rowValueLength = 1000)

  val TestR20C1010V2000 = new TestDataDescriptor("Test-R20-C10-10-V2000",
                                                  nVersions = versions,
                                                  nRows = rowCount,
                                                  rowLength = 20,
                                                  nQualifiers = 10,
                                                  qualifierLength = 10,
                                                  rowValueLength = 2000)

  val TestR20C1010V4000 = new TestDataDescriptor("Test-R20-C10-10-V4000",
                                                  nVersions = versions,
                                                  nRows = rowCount,
                                                  rowLength = 20,
                                                  nQualifiers = 10,
                                                  qualifierLength = 10,
                                                  rowValueLength = 4000)

  val TestR20C1010V8000 = new TestDataDescriptor("Test-R20-C10-10-V8000",
                                                  nVersions = versions,
                                                  nRows = rowCount,
                                                  rowLength = 20,
                                                  nQualifiers = 10,
                                                  qualifierLength = 10,
                                                  rowValueLength = 8000)


  val TestR20C2010V2000 = new TestDataDescriptor("Test-R20-C20-10-V2000",
                                                  nVersions = versions,
                                                  nRows = rowCount,
                                                  rowLength = 20,
                                                  nQualifiers = 20,
                                                  qualifierLength = 10,
                                                  rowValueLength = 2000)

  val TestR20C4010V4000 = new TestDataDescriptor("Test-R20-C40-10-V4000",
                                                  nVersions = versions,
                                                  nRows = rowCount,
                                                  rowLength = 20,
                                                  nQualifiers = 40,
                                                  qualifierLength = 10,
                                                  rowValueLength = 4000)

  val TestR20C6010V6000 = new TestDataDescriptor("Test-R20-C60-10-V6000",
                                                  nVersions = versions,
                                                  nRows = rowCount,
                                                  rowLength = 20,
                                                  nQualifiers = 60,
                                                  qualifierLength = 10,
                                                  rowValueLength = 6000)

//  val TestR20C8010V8000 = new TestDataDescriptor("Test-R20-C80-10-V8000",
//                                                  nVersions = versions,
//                                                  nRows = rowCount,
//                                                  rowLength = 20,
//                                                  nQualifiers = 80,
//                                                  qualifierLength = 10,
//                                                  rowValueLength = 8000)


  val TestR20C1010V0500LZO = new TestDataDescriptor("Test-R20-C10-10-V0500LZO",
                                                  nVersions = versions,
                                                  nRows = rowCount,
                                                  rowLength = 20,
                                                  nQualifiers = 10,
                                                  qualifierLength = 10,
                                                  rowValueLength = 0500,
                                                  useLZO = true)

  val TestR20C1010V1000LZO = new TestDataDescriptor("Test-R20-C10-10-V1000LZO",
                                                  nVersions = versions,
                                                  nRows = rowCount,
                                                  rowLength = 20,
                                                  nQualifiers = 10,
                                                  qualifierLength = 10,
                                                  rowValueLength = 1000,
                                                  useLZO = true)

  val TestR20C1010V2000LZO = new TestDataDescriptor("Test-R20-C10-10-V2000LZO",
                                                  nVersions = versions,
                                                  nRows = rowCount,
                                                  rowLength = 20,
                                                  nQualifiers = 10,
                                                  qualifierLength = 10,
                                                  rowValueLength = 2000,
                                                  useLZO = true)

  val TestR20C1010V4000LZO = new TestDataDescriptor("Test-R20-C10-10-V4000LZO",
                                                  nVersions = versions,
                                                  nRows = rowCount,
                                                  rowLength = 20,
                                                  nQualifiers = 10,
                                                  qualifierLength = 10,
                                                  rowValueLength = 4000,
                                                  useLZO = true)

  val TestR20C1010V8000LZO = new TestDataDescriptor("Test-R20-C10-10-V8000LZO",
                                                  nVersions = versions,
                                                  nRows = rowCount,
                                                  rowLength = 20,
                                                  nQualifiers = 10,
                                                  qualifierLength = 10,
                                                  rowValueLength = 8000,
                                                  useLZO = true)


  val TestR20C2010V2000LZO = new TestDataDescriptor("Test-R20-C20-10-V2000LZO",
                                                  nVersions = versions,
                                                  nRows = rowCount,
                                                  rowLength = 20,
                                                  nQualifiers = 20,
                                                  qualifierLength = 10,
                                                  rowValueLength = 2000,
                                                  useLZO = true)

  val TestR20C4010V4000LZO = new TestDataDescriptor("Test-R20-C40-10-V4000LZO",
                                                  nVersions = versions,
                                                  nRows = rowCount,
                                                  rowLength = 20,
                                                  nQualifiers = 40,
                                                  qualifierLength = 10,
                                                  rowValueLength = 4000,
                                                  useLZO = true)

  val TestR20C6010V6000LZO = new TestDataDescriptor("Test-R20-C60-10-V6000LZO",
                                                  nVersions = versions,
                                                  nRows = rowCount,
                                                  rowLength = 20,
                                                  nQualifiers = 60,
                                                  qualifierLength = 10,
                                                  rowValueLength = 6000,
                                                  useLZO = true)

  val TestR20C8010V8000LZO = new TestDataDescriptor("Test-R20-C80-10-V8000LZO",
                                                  nVersions = versions,
                                                  nRows = rowCount,
                                                  rowLength = 20,
                                                  nQualifiers = 80,
                                                  qualifierLength = 10,
                                                  rowValueLength = 8000,
                                                  useLZO = true)

  val Descriptors = List(TestR20C1010V0500,
                         TestR20C1010V1000,
                         TestR20C1010V2000,
                         TestR20C1010V4000,
                         TestR20C1010V8000,

                         TestR20C2010V2000,
                         TestR20C4010V4000,
                         TestR20C6010V6000 )
//                         TestR20C8010V8000,
//
//                         TestR20C1010V0500LZO,
//                         TestR20C1010V1000LZO,
//                         TestR20C1010V2000LZO,
//                         TestR20C1010V4000LZO,
//                         TestR20C1010V8000LZO,
//
//                         TestR20C2010V2000LZO,
//                         TestR20C4010V4000LZO,
//                         TestR20C6010V6000LZO,
//                         TestR20C8010V8000LZO )
}
