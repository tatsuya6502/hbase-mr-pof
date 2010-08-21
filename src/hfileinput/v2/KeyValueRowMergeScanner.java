package hfileinput.v2;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class KeyValueRowMergeScanner implements KeyValueRowScanner {

    private List<KeyValueRowScanner> scanners;

    private List<KeyValueRowScanner> currentScanners;
    private List<KeyValueRowScanner> closedScanners;

    private KeyValueHashSet mergedRow = new KeyValueHashSet();
    private boolean singleScanner = false;

    private Bytes.ByteArrayComparator comparator = new Bytes.ByteArrayComparator();

    private ImmutableBytesWritable rowID;

    public KeyValueRowMergeScanner(List<KeyValueRowScanner> scanners) {
        this.scanners = scanners;
        this.currentScanners = new ArrayList<KeyValueRowScanner>(scanners.size());
        this.closedScanners = new ArrayList<KeyValueRowScanner>(scanners.size());
        this.rowID = findScannersWithMinRowID();
    }

    // TODO For now, only supports 1 version

    public int getMaxVersions() {
        return 1;
    }

//    public NavigableSet<byte[]> getColumnsToInclude() {
//        throw new UnsupportedOperationException();
//    }

    public boolean seek(byte[] rowID) {
        throw new UnsupportedOperationException();
    }

    public ImmutableBytesWritable peekRowID() {
        return rowID;
    }

    // TODO Support deleted columns

    public boolean nextRow(KeyValueSet row) throws IOException {
        if (rowID == null)
            return false;

        if (singleScanner)
            return nextRowSingleScanner(row);
        else
            return nextRowMultiScanners(row);

    }

    private boolean nextRowSingleScanner(KeyValueSet row) throws IOException {
        KeyValueRowScanner scanner = scanners.get(0);
        boolean hasNext = scanner.nextRow(row);

        if (hasNext) {
            rowID = scanner.peekRowID();
        } else {
            scanner.close();
            rowID = null;
        }

        return hasNext;
    }

    private boolean nextRowMultiScanners(KeyValueSet row) throws IOException {
        for (KeyValueRowScanner scanner : currentScanners) {
            boolean hasNext = scanner.nextRow(row);

            if (! hasNext) {
                scanner.close();
                closedScanners.add(scanner);
            }
        }

        if (closedScanners.size() > 0) {
            for (KeyValueRowScanner scanner : closedScanners)
                scanners.remove(scanner);

            closedScanners.clear();
        }

        if (scanners.size() >= 2) {
            currentScanners.clear();
            rowID = findScannersWithMinRowID();

        } else if (scanners.size() == 1) {
            currentScanners.clear();
            rowID = scanners.get(0).peekRowID();
            singleScanner = true;

        } else { // scanners.size() == 0
            rowID = null;
        }

        return scanners.size() > 0;
    }

    private ImmutableBytesWritable findScannersWithMinRowID() {
        ImmutableBytesWritable min = null;

        for (KeyValueRowScanner scanner : scanners) {
            ImmutableBytesWritable rowID = scanner.peekRowID();

            if (min == null) {
                min = rowID;
                currentScanners.add(scanner);
            } else {
                int compare = ImmutableBytesWritable.Comparator
                        .compareBytes(min.get(), min.getOffset(), min.getLength(),
                                rowID.get(), rowID.getOffset(), rowID.getLength());
                if (compare > 0) {
                    min = rowID;
                    currentScanners.clear();
                    currentScanners.add(scanner);
                } else if (compare == 0) {
                    currentScanners.add(scanner);
                }
            }
        }

        return min;
    }


    public void close() {

    }


}
