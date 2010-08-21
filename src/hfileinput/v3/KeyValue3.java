package hfileinput.v3;

import hfileinput.v2.KeyValue2;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

public class KeyValue3 extends KeyValue2 {

    private KeyValue3 rowReference;

    public KeyValue3(byte[] bytes, int offset, int length, int keyLength) {
        super(bytes, offset, length, keyLength);
        initMarker(bytes, offset, keyLength);
    }

    public KeyValue3(final byte[] row, final byte[] family,
        final byte[] qualifier, final long timestamp, KeyValue.Type type, final byte[] value) {

        super(row, family, qualifier, timestamp, type, value);
        initMarker(super.getBuffer(), super.getOffset(), super.getKeyLength());
    }

    public void setRowReference(KeyValue3 kv) {
        rowReference = kv;
    }

    @Override
    public byte[] getRow() {
        if (rowReference == null) {
            return super.getRow();
        } else {
            return rowReference.getRow();
        }
    }

    @Override
    public ImmutableBytesWritable getRowIDWritable() {
        if (rowReference == null) {
            return super.getRowIDWritable();
        } else {
            KeyValue kv = rowReference;
            return new ImmutableBytesWritable(kv.getBuffer(), kv.getRowOffset(), kv.getRowLength());
        }
    }

//    @Override
//    public ImmutableBytesWritable getColumnName() {
//        return new ImmutableBytesWritable(getBuffer(), getFamilyOffset(),
//                getFamilyLength() + getQualifierLength() - 2);
//    }

    @Override
    public boolean rowIDEquals(ImmutableBytesWritable other) {
        if (rowReference == null) {
            return super.rowIDEquals(other);
        } else {
            KeyValue kv = rowReference;
            return arrayEquals(kv.getBuffer(), kv.getRowOffset(), kv.getRowLength(),
                               other.get(),    other.getOffset(), other.getLength());
        }
    }

    // TODO Implement the following column related methods
//    @Override
//    public boolean columnEquals(KeyValue2 other) {
//        return arrayEquals(super.getBuffer(), super.getFamilyOffset(),
//                                 super.getFamilyLength() + super.getQualifierLength() - 2,
//                           other.getBuffer(), other.getFamilyOffset(),
//                                 other.getFamilyLength() + other.getQualifierLength() - 2);
//    }
//
//    @Override
//    public boolean columnEquals(byte[] familyAndQualifier) {
//        return arrayEquals(super.getBuffer(), super.getFamilyOffset(),
//                                 super.getFamilyLength() + super.getQualifierLength() - 2,
//                           familyAndQualifier, 0, familyAndQualifier.length);
//    }

}
