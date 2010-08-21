package hfileinput;

import hfileinput.v1.HFileScannerWrapper;
import hfileinput.v2.KeyValueRowScanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.hfile.*;
import org.apache.hadoop.hbase.regionserver.KeyValueScanner;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class StoreInfo {

    private Configuration config;
    private HTableDescriptor table;
    private HColumnDescriptor family;
    private byte[] startRow;
    private byte[] endRow;
    private KeyValue.KVComparator comparator;
    private long ttl;
    private Path rootDir;
    List<HFile.Reader> readers;

    public StoreInfo(Configuration config, HTableDescriptor table, HColumnDescriptor family,
            byte[] startRow, byte[] endRow, KeyValue.KVComparator comparator) {

        if (family == null || comparator == null) {
            throw new IllegalArgumentException();
        }

        this.config = config;
        this.table = table;
        this.family = family;
        this.startRow = startRow;
        this.endRow = endRow;
        this.comparator = comparator;
        this.ttl = family.getTimeToLive();
        this.rootDir = new Path(config.get("hbase.rootdir"));
    }

    public HColumnDescriptor getFamily() {
        return family;
    }

    public byte[] getStartRow() {
        return startRow;
    }

    public byte[] getEndRow() {
        return endRow;
    }

    public long getTtl() {
        return ttl;
    }

    public Path getRootDir() {
        return rootDir;
    }

    public void setRootDir(Path rootDir) {
        this.rootDir = rootDir;
    }

    public KeyValue.KVComparator getComparator() {
        return comparator;
    }

    public int versionsToReturn(int wantedVersions) {
        if (wantedVersions <= 0) {
            throw new IllegalArgumentException("Number of versions must be > 0");
        }

        return Math.min(wantedVersions, family.getMaxVersions());
    }

    public long totalStoreFileSize() throws IOException {
        long total = 0L;

        List<Path> storeFilePaths = getStoreFilePaths();
        for (Path path : storeFilePaths) {
            FileSystem fs = path.getFileSystem(config);
            total += fs.getFileStatus(path).getLen();
        }

        return total;
    }

    public List<Path> getStoreFilePaths() throws IOException {
        HTable hTable = new HTable(config, table.getName());
        Path tableDir = HTableDescriptor.getTableDir(rootDir, table.getName());
        HRegionInfo regionInfo = hTable.getRegionLocation(startRow).getRegionInfo();

        // TODO Verify the endRow is on the same region
        //

        // TODO Verify this is not a split region
        //


        Path storeHomeDir = new Path(tableDir, regionInfo.getEncodedName() + "/" + Bytes.toString(family.getName()));

        FileSystem fs = storeHomeDir.getFileSystem(config);
        FileStatus[] statusList = fs.listStatus(storeHomeDir);

        if (statusList == null) {
            return new ArrayList<Path>();

        } else {
            List<Path> paths = new ArrayList<Path>();

            for (FileStatus status : statusList) {
                if (!status.isDir() && status.getLen() > 0) {
                    paths.add(status.getPath());
                }
            }

            return paths;
        }
    }

    public List<KeyValueScanner> createKeyValueScanners(BlockCache cache) throws IOException {
        boolean inMemory = false;
        boolean cacheBlocks = (cache != null);
        boolean positionalRead = false;

        List<KeyValueScanner> scanners = new ArrayList<KeyValueScanner>();

        for (HFile.Reader reader : getHFileReaders(cache)) {
            reader.loadFileInfo();
            HFileScanner scanner = reader.getScanner(cacheBlocks, positionalRead);
            scanner.seekTo();

            scanners.add(new HFileScannerWrapper(scanner));
        }

        return scanners;
    }

    public List<KeyValueRowScanner> createKeyValueRowScanners(BlockCache cache) throws IOException {
        List<KeyValueRowScanner> scanners = new ArrayList<KeyValueRowScanner>();

        for (HFile.Reader reader : getHFileReaders(cache)) {
            HFileRowScanner scanner = new HFileRowScanner(reader);
//            scanner.seek();

            scanners.add(scanner);
        }

        return scanners;
    }

    public List<KeyValueRowScanner> createKeyValueRowScannersV3(BlockCache cache) throws IOException {
        List<KeyValueRowScanner> scanners = new ArrayList<KeyValueRowScanner>();

        for (HFile.Reader reader : getHFileReaders(cache)) {
            HFileRowScanner scanner = new HFileRowScanner3(reader);
//            scanner.seek();

            scanners.add(scanner);
        }

        return scanners;
    }

    public List<HFile.Reader> getHFileReaders(BlockCache cache) throws IOException {
        if (readers != null) {
            return readers;
        }

        readers = new ArrayList<HFile.Reader>();
        for (Path path : getStoreFilePaths()) {

            readers.add(createHFileReaderForPath(path, cache));
        }

        return readers;
    }

    public HFile.Reader createHFileReaderForPath(Path path, BlockCache cache) throws IOException {
        boolean inMemory = false;
        FileSystem fs = path.getFileSystem(config);

        return new HFile.Reader(fs, path, cache, inMemory);
    }

    public static List<StoreInfo> getStoreInfoEntries(Configuration config, HTable table)
            throws IOException {

        List<StoreInfo> entries = new ArrayList<StoreInfo>();

        HTableDescriptor tableDescriptor = table.getTableDescriptor();
        HColumnDescriptor[] familyDescriptors = tableDescriptor.getColumnFamilies();
        byte[][] startKeys = table.getStartKeys();
        byte[][] endKeys = table.getEndKeys();
        KeyValue.KVComparator comparator = new KeyValue.KVComparator();

        for (int i = 0; i < startKeys.length; i++) {
            StoreInfo storeInfo = new StoreInfo(config, tableDescriptor, familyDescriptors[0],
                    startKeys[i], endKeys[i], comparator);
            entries.add(storeInfo);
        }
        return entries;
    }

}
