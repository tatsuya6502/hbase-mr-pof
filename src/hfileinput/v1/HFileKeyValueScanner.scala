package hfileinput.v1

import hfileinput.StoreInfo

import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.regionserver.ScanQueryMatcher.MatchCode._
import org.apache.hadoop.hbase.regionserver.{KeyValueHeap, KeyValueScanner, ScanQueryMatcher}

import java.util.{ArrayList, NavigableSet, List => JavaList}

class HFileKeyValueScanner(val storeInfo: StoreInfo, val scanners: JavaList[KeyValueScanner],
                           scan: Scan, columns: NavigableSet[Array[Byte]]) extends KeyValueScanner {

  var closing = false

  val matcher = new ScanQueryMatcher(scan, storeInfo.getFamily.getName,
                          columns, storeInfo.getTtl, storeInfo.getComparator.getRawComparator,
                          storeInfo.versionsToReturn(scan.getMaxVersions))

//  var heap: Option[KeyValueScanner] = if (scanners.size > 1) {
//                                        Some(new KeyValueHeap(scanners, storeInfo.getComparator))
//                                      } else {
//                                        Some(scanners.get(0))
//                                      }
  var heap: Option[KeyValueScanner] = Some(new KeyValueHeap(scanners, storeInfo.getComparator))

  import scala.collection.JavaConversions._
  for(scanner <- scanners) {
    scanner.seek(matcher.getStartKey)
  }

  def seek(key: KeyValue): Boolean = {
    throw new UnsupportedOperationException
  }

  override def peek(): KeyValue = synchronized {
    if (heap.isEmpty)
      null
    else
      heap.get.peek
  }

  override def next(): KeyValue = {
    throw new UnsupportedOperationException("Never call StoreScanner.nextKeyVale()")
  }

  override def close: Unit = synchronized {
    if (closing) return

    closing = true

    if (heap.isDefined) {
      heap.get.close
      heap = None
    }
  }

  def next(outResult: JavaList[KeyValue]): Boolean = synchronized {
    if (heap.isEmpty  ||  heap.get.peek == null) {
      close
      false

    } else {
      matcher.setRow(heap.get.peek.getRow)

      val row = new ArrayList[KeyValue]
      val hasMoreRows = readRow(row)
      if (! row.isEmpty)
        outResult.addAll(row)

      hasMoreRows
    }
  }

  private def readRow(row: ArrayList[KeyValue]): Boolean = {
    var kv = heap.get.peek
    if (kv == null) {
      close; false

    } else {
      matcher.`match`(kv) match {
        case INCLUDE =>       row.add(heap.get.next); readRow(row)
        case DONE =>          true
        case DONE_SCAN =>     close; false
        case SEEK_NEXT_ROW => heap.get.next; readRow(row)
        case SEEK_NEXT_COL => heap.get.next; readRow(row)
        case SKIP =>          heap.get.next; readRow(row)
      }
    }
  }

}
