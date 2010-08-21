package hfileinput.v2;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Arrays;

public class KeyValueHashSet implements KeyValueSet {

    public static final int MAX_DATA_COUNT = 4096;

    public static final short NONE = -1;

    private short[] pointers = new short[MAX_DATA_COUNT];

    private KeyValue2[] heap = new KeyValue2[MAX_DATA_COUNT];

    private short heapTop = 0;

    private KeyValue2 firstKV = null;

    public KeyValueHashSet() {
        Arrays.fill(pointers, NONE);
    }
    
//    public void putAll(Collection<KeyValue2> kvs) {
//        for (KeyValue2 kv : kvs) {
//            put(kv);
//        }
//    }

    public KeyValue2 put(KeyValue2 kv) {
        short hash = kv.getQualifierHash();
        short pointer = pointers[hash];
        KeyValue2 old = null;

        if (pointer == NONE) {
            handleInsert(kv, hash);

        } else {
            KeyValue2 existing = heap[pointer];

            if (kv.columnEquals(existing)) {
                old = handleUpdate(kv, hash);

            } else {
                old = handleHashCollision(kv, hash, pointer);
            }
        }

        return old;
    }

    private KeyValue2 handleHashCollision(KeyValue2 kv, int hash, short pointer) {
//        throw new IllegalStateException(kv + ", hash: " + hash);
        KeyValue2 old = null;

        int count = MAX_DATA_COUNT;
        do {
            hash = (hash + 1) & 0x00000FFF;
            pointer = pointers[hash];
            --count;
        } while (count >= 0  &&  pointer != NONE
                &&  kv.columnEquals(heap[pointer]) ); // TODO Can we use hash here?


        if (count == 0) {
            throw new IllegalStateException("Sorry, no space left to keep your KeyValue.");

        } else if (pointer == NONE) {
            handleInsert(kv, hash);

        } else {
            old = handleUpdate(kv, hash);

        }
        return old;
    }

    private void handleInsert(KeyValue2 kv, int hash) {
        pointers[hash] = heapTop;
        heap[heapTop] = kv;
        ++heapTop;

        if (firstKV == null)
            firstKV = kv;
    }

    private KeyValue2 handleUpdate(KeyValue2 kv, int hash) {
        short pointer = pointers[hash];
        KeyValue2 existing = heap[pointer];

        if (kv.getTimestamp() > existing.getTimestamp()) {
            heap[pointer] = kv;
        }

        return existing;
    }

//    public void transferValues(List<KeyValue2> list) {
//        for (int i = 0; i < heapTop; ++i) {
//            KeyValue2 kv = heap[i];
//            heap[i] = null;
//            pointers[kv.getQualifierHash()] = NONE;
//
//            list.add(kv);
//        }
//
//        heapTop = 0;
//        firstKV = null;
//    }

    public void clear() {
        for (int i = 0; i < heapTop; ++i) {
            KeyValue2 kv = heap[i];
            heap[i] = null;
            pointers[kv.getQualifierHash()] = NONE;
        }

        heapTop = 0;
        firstKV = null;
    }

    public ImmutableBytesWritable getRow() {
        return firstKV == null ? null : firstKV.getRowIDWritable();
    }

    public int size() {
        return heapTop;
    }

    public KeyValue2 getKeyValue(byte[] familyAndQualifier) {
        int hash = KeyValue2.calculateQualifierHashCode(familyAndQualifier);
        short pointer = pointers[hash];

        if (pointer == NONE) {
            return null;
        } else {
            KeyValue2 kv = heap[pointer];
            if (kv.columnEquals(familyAndQualifier)) {
                return kv;
            } else {
                throw new UnsupportedOperationException("getKeyValue with hash collision is not supported yet.");
            }
        }
    }

    public ImmutableBytesWritable getValueAsBytesWritable(byte[] familyAndQualifier) {
        KeyValue2 kv = getKeyValue(familyAndQualifier);
        if (kv == null)
            return null;
        else
            return new ImmutableBytesWritable(kv.getBuffer(),
                    kv.getValueOffset(), kv.getValueLength());
    }

    public byte getValueAsByte(byte[] familyAndQualifier) {
        KeyValue2 kv = getKeyValue(familyAndQualifier);
        if (kv == null)
            return NONE;
        else
            return kv.getBuffer()[kv.getValueOffset()];
    }

    public short getValueAsShort(byte[] familyAndQualifier) {
        KeyValue2 kv = getKeyValue(familyAndQualifier);
        if (kv == null) 
            return NONE;
        else
            return Bytes.toShort(kv.getBuffer(), kv.getValueOffset(), kv.getValueLength());
    }

    public int getValueAsInt(byte[] familyAndQualifier) {
        KeyValue2 kv = getKeyValue(familyAndQualifier);
        if (kv == null)
            return NONE;
        else
            return Bytes.toInt(kv.getBuffer(), kv.getValueOffset(), kv.getValueLength());
    }

    public long getValueAsLong(byte[] familyAndQualifier) {
        KeyValue2 kv = getKeyValue(familyAndQualifier);
        if (kv == null)
            return NONE;
        else
            return Bytes.toLong(kv.getBuffer(), kv.getValueOffset(), kv.getValueLength());
    }

    public String getValueAsString(byte[] familyAndQualifier) {
        KeyValue2 kv = getKeyValue(familyAndQualifier);
        if (kv == null)
            return null;
        else
            return Bytes.toString(kv.getBuffer(), kv.getValueOffset(), kv.getValueLength());
    }

}
