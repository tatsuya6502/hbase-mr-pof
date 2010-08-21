package hfileinput.v2;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Hash;

public class KeyValue2 extends KeyValue {

    private static Hash murmur = Hash.getInstance(Hash.MURMUR_HASH);
    
    private int     keyLength;
    private long    timestamp;
    private short   qualifierHash = -1;

    private byte[] marker = new byte[2];

    private boolean isTSInitialized = false;


    public KeyValue2(byte[] bytes, int offset, int length, int keyLength) {
        super(bytes, offset, length);
        this.keyLength = keyLength;
        initMarker(bytes, offset, keyLength);
    }

    public KeyValue2(final byte[] row, final byte[] family,
        final byte[] qualifier, final long timestamp, KeyValue.Type type, final byte[] value) {

        super(row, family, qualifier, timestamp, type, value);
        keyLength = super.getKeyLength();
        initMarker(super.getBuffer(), super.getOffset(), keyLength);
    }

    public ImmutableBytesWritable getRowIDWritable() {
        return new ImmutableBytesWritable(getBuffer(), getRowOffset(), getRowLength());
    }

    public ImmutableBytesWritable getColumnName() {
        return new ImmutableBytesWritable(getBuffer(), getFamilyOffset(),
                getFamilyLength() + getQualifierLength() - 2);
    }

    @Override
    public int getKeyLength() {
        return keyLength;
    }

    @Override
    public long getTimestamp() {
        if (! isTSInitialized) {
            timestamp = super.getTimestamp();
            isTSInitialized = true;
        }
        return timestamp;
    }

    public short getQualifierHash() {
        if (qualifierHash < 0) {
            qualifierHash = parseQualifierHashCode();
        }
        return qualifierHash;
    }

    public void initMarker(byte[] buffer, int offset, int keyLength) {
        int markerOffset = offset + ROW_OFFSET - TIMESTAMP_TYPE_SIZE + keyLength - 1;

        marker[0] = buffer[markerOffset - 1];
        marker[1] = buffer[markerOffset];
    }

    private short parseQualifierHashCode() {
        int hash   =  marker[0] & 0xFF;
        hash     <<=  4;
        hash      ^=  marker[1] & 0x0F;

        return (short)hash;
    }

    public boolean isNewRow() {
        return (marker[1] & (byte)16) == (byte)0;
    }

    public boolean isPrimaryColumn() {
        return (marker[1] & (byte)32) == (byte)0;
    }

    public boolean rowIDEquals(ImmutableBytesWritable other) {
        return arrayEquals(super.getBuffer(), super.getRowOffset(), super.getRowLength(), 
                           other.get(),       other.getOffset(),    other.getLength());
    }

    public boolean columnEquals(KeyValue2 other) {
        return arrayEquals(super.getBuffer(), super.getFamilyOffset(),
                                 super.getFamilyLength() + super.getQualifierLength() - 2,
                           other.getBuffer(), other.getFamilyOffset(),
                                 other.getFamilyLength() + other.getQualifierLength() - 2);
    }

    public boolean columnEquals(byte[] familyAndQualifier) {
        return arrayEquals(super.getBuffer(), super.getFamilyOffset(),
                                 super.getFamilyLength() + super.getQualifierLength() - 2,
                           familyAndQualifier, 0, familyAndQualifier.length);
    }



    public static boolean arrayEquals(byte[] b1, byte[] b2) {
        return arrayEquals(b1, 0, b1.length, b2, 0, b2.length);
    }

    public static boolean arrayEquals(byte[] b1, int b1Offset, int b1Length, byte[] b2, int b2Offset, int b2Length) {
      if (b1Length != b2Length)
        return false;

      if (b1Offset == b2Offset) {
          int pos = b1Offset + b1Length - 1;
          do {
              if (b1[pos] != b2[pos])  return false;
              --pos;
          } while (pos >= b1Offset);
          return true;

      } else {
          int b1pos = b1Offset + b1Length - 1;
          int b2pos = b2Offset + b2Length - 1;
          do {
              if (b1[b1pos] != b2[b2pos])  return false;
              --b1pos;
              --b2pos;
          } while (b1pos >= b1Offset);
          return true;
      }
    }

    public static int calculateQualifierHashCode(byte[] familyAndQualifier) {
      return murmur.hash(familyAndQualifier) & 0x00000FFF;
    }

    public static int calculateQualifierHashCode(byte[] family, byte[] qualifier) {
        byte[] familyAndQualifier = new byte[family.length + qualifier.length];
        System.arraycopy(family,    0, familyAndQualifier, 0,             family.length);
        System.arraycopy(qualifier, 0, familyAndQualifier, family.length, qualifier.length);

        return calculateQualifierHashCode(familyAndQualifier);
    }


    public static class KeyComparator extends KeyValue.KeyComparator {

        @Override
        public int compare(byte[] left, int loffset, int llength, byte[] right,
            int roffset, int rlength) {

            return -1;
        }

        @Override
        public int compare(byte[] o1, byte[] o2) {
            return 0;
        }
    }

}
