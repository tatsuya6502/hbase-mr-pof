package org.apache.hadoop.hbase.io.hfile;

//import hfileinput.util.StopWatch;
import hfileinput.v2.KeyValueSet;
import hfileinput.v3.KeyValue3;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import java.io.IOException;

public class HFileRowScanner3 extends HFileRowScanner {

//    public StopWatch hfileReaderSW;
//    public StopWatch keyValueSetAddSW;
//    public StopWatch markerSW;
//    public StopWatch updateCurrentSW;

    protected KeyValue3 currKV = null;

    public HFileRowScanner3(HFile.Reader reader) {
        super(reader);
    }

    @Override
    public ImmutableBytesWritable peekRowID() {
        return currKV.getRowIDWritable();
    }

    @Override
    public boolean nextRow(KeyValueSet row) throws IOException {
        KeyValue3 firstKV = currKV;
        boolean hasNext;
        boolean isNewRow  = false;
        boolean isPrimaryColumn  = true;

        do {
            if (isPrimaryColumn) {
                if (! isNewRow) {
                    currKV.setRowReference(firstKV);
                }

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

    @Override
    protected void updateCurrent() {
//        updateCurrentSW.start();

        currKeyLen = block.getInt();
        currValueLen = block.getInt();

        currKV = new KeyValue3(currBuffer,
                currArrayOffset + block.position() - 8,
                currKeyLen + currValueLen,
                currKeyLen);

//        updateCurrentSW.stop();
    }

}
