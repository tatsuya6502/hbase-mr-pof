package hfileinput.v1

import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.client.{Result, ResultScanner}

import java.util.{ArrayList, Iterator}

class HFileResultScanner(val kvScanner: HFileKeyValueScanner) extends ResultScanner {

  var hasNext = false

//  var stopWatch: Option[StopWatch] = None

  def iterator: Iterator[Result] = throw new UnsupportedOperationException

  def next(): Result = {
//    if (stopWatch.isDefined)
//      stopWatch.get.start

    val row = new ArrayList[KeyValue]
    hasNext = kvScanner.next(row)

    val result = new Result(row)

//    if (stopWatch.isDefined)
//      stopWatch.get.stop

    result
  }

  def next(numberOfRows: Int): Array[Result] = throw new UnsupportedOperationException

  def close: Unit = {
    kvScanner.close
  }

}