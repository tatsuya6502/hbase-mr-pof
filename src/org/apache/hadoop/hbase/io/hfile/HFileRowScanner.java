package org.apache.hadoop.hbase.io.hfile;

//import hfileinput.util.StopWatch;
import hfileinput.v2.KeyValue2;
import hfileinput.v2.KeyValueSet;
import hfileinput.v2.KeyValueRowScanner;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

public class HFileRowScanner implements KeyValueRowScanner {

    public static final String ROW_MARKER_SUPPORTED_KEY = "hfileScanner.rowMarkerSupported";

//    public StopWatch hfileReaderSW;
//    public StopWatch keyValueSetAddSW;
//    public StopWatch markerSW;
//    public StopWatch loadBlockSW;
//    public StopWatch updateCurrentSW;

    private HFile.Reader reader;

    protected ByteBuffer block = null;
    protected int currBlock = -1;
    protected byte[] currBuffer;
    protected int currArrayOffset;

    protected int currKeyLen = 0;
    protected int currValueLen = 0;
    protected KeyValue2 currKV = null;


    private Map<byte[], byte[]> fileInfo;

    private boolean isOpen = false;

    private boolean rowMarkerSupported;


    public HFileRowScanner(HFile.Reader reader) {
        this.reader = reader;
    }

    public int getMaxVersions() {
        return 1;
    }

//    public NavigableSet<byte[]> getColumnsToInclude() {
//        throw new UnsupportedOperationException();
//    }

    private void loadFileInfo() throws IOException {
//        long start = System.currentTimeMillis();

        fileInfo = reader.loadFileInfo();

        byte[] value = fileInfo.get(Bytes.toBytes(ROW_MARKER_SUPPORTED_KEY));
        rowMarkerSupported = "yes".equals(Bytes.toString(value));

        if (! rowMarkerSupported) {
            throw new IllegalStateException("rowMarkerSupported is false.");
        }

        isOpen = true;
//        long end = System.currentTimeMillis();

//        System.out.format("File info loaded in %,d ms. rowMarkerSupported: %s %n",
//                end - start, rowMarkerSupported);

    }

    public ImmutableBytesWritable peekRowID() {
        return currKV.getRowIDWritable();
    }

    public boolean seek() throws IOException {

        if (! isOpen) {
            loadFileInfo();
        }

        loadBlock(0);
        updateCurrent();


        return true;
    }

    public boolean seek(byte[] rowID) {
        throw new UnsupportedOperationException();
    }

    public boolean nextRow(KeyValueSet row) throws IOException {
        boolean hasNext;
        boolean isNewRow  = false;
        boolean isPrimaryColumn  = true;

        do {
            if (isPrimaryColumn) {
//                keyValueSetAddSW.start();
                row.put(currKV);
//                keyValueSetAddSW.stop();
            }

//            hfileReaderSW.start();
            hasNext  = nextKeyVale();
//            hfileReaderSW.stop();

//            markerSW.start();
            isNewRow = currKV.isNewRow();
            isPrimaryColumn = currKV.isPrimaryColumn();
//            markerSW.stop();

        } while (hasNext && ! isNewRow);

        return hasNext;
    }

    protected boolean nextKeyVale() throws IOException {
        if (block == null) {
            throw new IOException("Next called on non-seek scanner");
        }

        block.position(block.position() + currKeyLen + currValueLen);

        if (block.remaining() <= 0) {
            int blockNo = currBlock + 1;

            if (blockNo >= reader.blockIndex.count) {
                // End of data blocks
                currBlock = 0;
                block = null;
                return false;
            }

            loadBlock(blockNo);
        }

        updateCurrent();

        return true;
    }


    protected void updateCurrent() {
//        updateCurrentSW.start();

        currKeyLen = block.getInt();
        currValueLen = block.getInt();

        currKV = new KeyValue2(currBuffer,
                currArrayOffset + block.position() - 8,
                currKeyLen + currValueLen,
                currKeyLen);

//        updateCurrentSW.stop();
    }

    protected void loadBlock(int bloc) throws IOException {
//        loadBlockSW.start();

        if (block != null && bloc == currBlock) {
            block.rewind();
        } else {
            block = reader.readBlock(bloc, true, false);
            currBlock = bloc;
            currBuffer = block.array();
            currArrayOffset = block.arrayOffset();
        }

//        loadBlockSW.stop();
    }

    public void close() {

    }

}
