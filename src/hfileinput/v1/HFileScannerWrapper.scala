package hfileinput.v1

import org.apache.hadoop.hbase.regionserver.KeyValueScanner
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.io.hfile.HFileScanner

class HFileScannerWrapper(val hfs: HFileScanner) extends KeyValueScanner {

  private var current: Option[KeyValue] = None

  require(hfs != null)

  def seek(key: KeyValue): Boolean = {
    if (seekAtOrAfter(hfs, key)) {
      val kv = hfs.getKeyValue
      current = if (kv == null) None  else Some(kv)
      hfs.next
      true
    } else {
      close
      false
    }
  }

  def peek(): KeyValue = {
    if (current.isEmpty) {
      val kv = hfs.getKeyValue
      if (kv != null)
        current = Some(kv)
      else
        current = None
    }

    current.getOrElse(null)
  }

  def next(): KeyValue = {
    val result = current
    val kv = hfs.getKeyValue
    current = if (kv == null) None  else Some(kv)

    // only seek if we aren't at the end. cur == null implies 'end'.
    if (current.isDefined)
      hfs.next

    result.getOrElse(null)
  }

  def close(): Unit = {
    current = None
  }

  private def seekAtOrAfter(hfs: HFileScanner, kv: KeyValue): Boolean = {
    val result = hfs.seekTo(kv.getBuffer, kv.getKeyOffset, kv.getKeyLength)

    if (result == 0) {
      true

    } else if (result < 0) {
      // Passed KV is smaller than first KV in file, work from start of file
      hfs.seekTo

    } else /* (result > 0) */ {
      // Passed KV is larger than current KV in file, if there is a nextKeyVale
      // it is the "after", if not then this scanner is done.
      hfs.next
    }
  }

}