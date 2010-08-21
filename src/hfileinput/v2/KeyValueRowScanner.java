package hfileinput.v2;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import java.io.IOException;

public interface KeyValueRowScanner {

    int getMaxVersions();

//    NavigableSet<byte[]> getColumnsToIncludes();

    ImmutableBytesWritable peekRowID();

    boolean nextRow(KeyValueSet row) throws IOException;

    boolean seek(byte[] rowID);

    void close();

}
