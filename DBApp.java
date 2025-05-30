package DBMS;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

public class DBApp {
    static int dataPageSize = 2;

    public static void createTable(String tableName, String[] columnsNames) {
        Table t = new Table(tableName, columnsNames);
        FileManager.storeTable(tableName, t);
    }

    public static void insert(String tableName, String[] record) {
        Table t = FileManager.loadTable(tableName);
        if (t == null) {
            throw new IllegalArgumentException("Table " + tableName + " does not exist");
        }
        t.insert(record);
       
        for (String colName : t.getIndexedColumns()) {
            int colIndex = t.getColumnIndex(colName);
            String value = record[colIndex];
            BitMapIndex index = FileManager.loadTableIndex(tableName, colName);
            if (index == null) {
                index = new BitMapIndex();
            }
            index.insert(value);
            FileManager.storeTableIndex(tableName, colName, index);
        }
        FileManager.storeTable(tableName, t);
    }

    public static ArrayList<String[]> select(String tableName) {
        Table t = FileManager.loadTable(tableName);
        ArrayList<String[]> res = t.select();
        FileManager.storeTable(tableName, t);
        return res;
    }

    public static ArrayList<String[]> select(String tableName, int pageNumber, int recordNumber) {
        Table t = FileManager.loadTable(tableName);
        ArrayList<String[]> res = t.select(pageNumber, recordNumber);
        FileManager.storeTable(tableName, t);
        return res;
    }

    public static ArrayList<String[]> select(String tableName, String[] cols, String[] vals) {
        Table t = FileManager.loadTable(tableName);
        ArrayList<String[]> res = t.select(cols, vals);
        FileManager.storeTable(tableName, t);
        return res;
    }

    public static String getFullTrace(String tableName) {
        Table t = FileManager.loadTable(tableName);
        String res = t.getFullTrace();
        return res;
    }

    public static String getLastTrace(String tableName) {
        Table t = FileManager.loadTable(tableName);
        String res = t.getLastTrace();
        return res;
    }

    public static void createBitMapIndex(String tableName, String columnName) {
        Table t = FileManager.loadTable(tableName);
        if (t == null) {
            throw new IllegalArgumentException("Table " + tableName + " does not exist");
        }
        long startTime = System.currentTimeMillis();
        int colIndex = t.getColumnIndex(columnName);
        if (colIndex == -1) {
            throw new IllegalArgumentException("Column " + columnName + " does not exist");
        }
        ArrayList<String> values = new ArrayList<>();
        for (int i = 0; i < t.getPageCount(); i++) {
            Page page = FileManager.loadTablePage(tableName, i);
            if (page != null) {
                ArrayList<String[]> pageRecords = page.select();
                for (String[] record : pageRecords) {
                    values.add(record[colIndex]);
                }
            }
        }
        BitMapIndex index = new BitMapIndex();
        index.initialize(values.toArray(new String[0]));
        FileManager.storeTableIndex(tableName, columnName, index);
        t.addIndexedColumn(columnName);
        long stopTime = System.currentTimeMillis();
        t.getTrace().add("Index created for column: " + columnName + ", execution time (mil):" + (stopTime - startTime));
        FileManager.storeTable(tableName, t);
    }

    public static String getValueBits(String tableName, String colName, String value) {
        Table t = FileManager.loadTable(tableName);
        if (t == null) {
            throw new IllegalArgumentException("Table " + tableName + " does not exist");
        }
        BitMapIndex index = FileManager.loadTableIndex(tableName, colName);
        if (index == null) {
            throw new IllegalArgumentException("Index does not exist for column " + colName);
        }
        String bitstream = index.getBitstream(value);
        if (bitstream == null) {
            int totalRecords = 0;
            for (int i = 0; i < t.getPageCount(); i++) {
                Page page = FileManager.loadTablePage(tableName, i);
                if (page != null) {
                    totalRecords += page.select().size();
                }
            }
            StringBuilder zeroStream = new StringBuilder();
            for (int i = 0; i < totalRecords; i++) {
                zeroStream.append('0');
            }
            return zeroStream.toString();
        }
        return bitstream;
    }

    public static ArrayList<String[]> selectIndex(String tableName, String[] cols, String[] vals) {
        Table t = FileManager.loadTable(tableName);
        if (t == null) {
            throw new IllegalArgumentException("Table " + tableName + " does not exist");
        }
        long startTime = System.currentTimeMillis();
        ArrayList<String[]> result = new ArrayList<>();
        ArrayList<Integer> indexedColsIndices = new ArrayList<>();
        ArrayList<Integer> nonIndexedColsIndices = new ArrayList<>();
        ArrayList<String> indexedColsNames = new ArrayList<>(); // For trace
        ArrayList<String> nonIndexedColsNames = new ArrayList<>(); // For trace

        
        for (int i = 0; i < cols.length; i++) {
            if (t.getIndexedColumns().contains(cols[i])) {
                indexedColsIndices.add(i);
                indexedColsNames.add(cols[i]);
            } else {
                nonIndexedColsIndices.add(i);
                nonIndexedColsNames.add(cols[i]);
            }
        }

        
        Collections.sort(indexedColsNames);
        Collections.sort(nonIndexedColsNames);

        int indexedSelectionCount = 0;
        if (nonIndexedColsIndices.isEmpty()) {
            
            String combinedBitstream = null;
            for (int i : indexedColsIndices) {
                String bitstream = getValueBits(tableName, cols[i], vals[i]);
                if (combinedBitstream == null) {
                    combinedBitstream = bitstream;
                } else {
                    combinedBitstream = andBitstreams(combinedBitstream, bitstream);
                }
            }
            result = getRecordsFromBitstream(t, combinedBitstream);
            indexedSelectionCount = result.size();
        } else if (indexedColsIndices.size() == 1) {
           
            int i = indexedColsIndices.get(0);
            String bitstream = getValueBits(tableName, cols[i], vals[i]);
            ArrayList<String[]> candidates = getRecordsFromBitstream(t, bitstream);
            indexedSelectionCount = candidates.size();
            result = filterNonIndexed(t, candidates, cols, vals, nonIndexedColsIndices);
        } else if (!indexedColsIndices.isEmpty()) {
            
            String combinedBitstream = null;
            for (int i : indexedColsIndices) {
                String bitstream = getValueBits(tableName, cols[i], vals[i]);
                if (combinedBitstream == null) {
                    combinedBitstream = bitstream;
                } else {
                    combinedBitstream = andBitstreams(combinedBitstream, bitstream);
                }
            }
            ArrayList<String[]> candidates = getRecordsFromBitstream(t, combinedBitstream);
            indexedSelectionCount = candidates.size();
            result = filterNonIndexed(t, candidates, cols, vals, nonIndexedColsIndices);
        } else {
           
            result = t.select(cols, vals);
            indexedSelectionCount = result.size();
        }

      
        StringBuilder traceBuilder = new StringBuilder();
        traceBuilder.append("Select index condition:")
                    .append(Arrays.toString(cols))
                    .append("->")
                    .append(Arrays.toString(vals))
                    .append(", ");

        
        if (!indexedColsNames.isEmpty()) {
            traceBuilder.append("Indexed columns: ")
                        .append(indexedColsNames)
                        .append(", Indexed selection count: ")
                        .append(indexedSelectionCount)
                        .append(", ");
        } else {
            traceBuilder.append("Indexed selection count: ")
                        .append(indexedSelectionCount)
                        .append(", ");
        }

        
        if (!nonIndexedColsNames.isEmpty()) {
            traceBuilder.append("Non Indexed: ")
                        .append(nonIndexedColsNames)
                        .append(", ");
        }

        
        traceBuilder.append("Final count: ")
                    .append(result.size())
                    .append(", execution time (mil):")
                    .append(System.currentTimeMillis() - startTime);

        t.getTrace().add(traceBuilder.toString());
        FileManager.storeTable(tableName, t);
        return result;
    }
    public static ArrayList<String[]> validateRecords(String tableName) {
        Table t = FileManager.loadTable(tableName);
        if (t == null) {
            throw new IllegalArgumentException("Table " + tableName + " does not exist");
        }
        ArrayList<String[]> missing = new ArrayList<>();
        ArrayList<String[]> currentRecords = t.select();
        ArrayList<String[]> backupRecords = t.getBackupRecords();
        for (String[] record : backupRecords) {
            boolean found = false;
            for (String[] current : currentRecords) {
                if (Arrays.equals(record, current)) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                missing.add(record);
            }
        }
        t.getTrace().add("Validating records: " + missing.size() + " records missing.");
        FileManager.storeTable(tableName, t);
        return missing;
    }

    public static void recoverRecords(String tableName, ArrayList<String[]> missing) {
        Table t = FileManager.loadTable(tableName);
        if (t == null) {
            throw new IllegalArgumentException("Table " + tableName + " does not exist");
        }
        ArrayList<Integer> recoveredPages = new ArrayList<>();
        for (String[] record : missing) {
            int pageNum = t.getOriginalPageNum(record);
            if (pageNum < 0) {
               
                continue; 
            }
            Page page = FileManager.loadTablePage(tableName, pageNum);
            if (page == null) {
                page = new Page();
                if (!recoveredPages.contains(pageNum)) {
                    recoveredPages.add(pageNum);
                }
            }
            if (page.insert(record)) {
                FileManager.storeTablePage(tableName, pageNum, page);
            }
            
            if (pageNum >= t.getPageCount()) {
                t.setPageCount(pageNum + 1); 
                t.getTrace().add("Updated pageCount to " + (pageNum + 1));
            }
        }
        
        for (String colName : t.getIndexedColumns()) {
            int colIndex = t.getColumnIndex(colName);
            if (colIndex == -1) {
                
                continue;
            }
            ArrayList<String> values = new ArrayList<>();
            ArrayList<String[]> allRecords = t.select();
            for (String[] record : allRecords) {
                values.add(record[colIndex]);
            }
            BitMapIndex index = new BitMapIndex();
            index.initialize(values.toArray(new String[0]));
            FileManager.storeTableIndex(tableName, colName, index);
        }
        t.getTrace().add("Recovering " + missing.size() + " records in pages: " + recoveredPages);
        FileManager.storeTable(tableName, t);
    }

    private static String andBitstreams(String b1, String b2) {
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < Math.min(b1.length(), b2.length()); i++) {
            result.append(b1.charAt(i) == '1' && b2.charAt(i) == '1' ? '1' : '0');
        }
        return result.toString();
    }

    private static ArrayList<String[]> getRecordsFromBitstream(Table t, String bitstream) {
        ArrayList<String[]> result = new ArrayList<>();
        int globalRowIndex = 0;
        for (int i = 0; i < t.getPageCount(); i++) {
            Page page = FileManager.loadTablePage(t.getName(), i);
            if (page != null) {
                ArrayList<String[]> records = page.select();
                for (String[] record : records) {
                    if (globalRowIndex < bitstream.length() && bitstream.charAt(globalRowIndex) == '1') {
                        result.add(record);
                    }
                    globalRowIndex++;
                }
            }
        }
        return result;
    }

    private static ArrayList<String[]> filterNonIndexed(Table t, ArrayList<String[]> candidates,
                                                       String[] cols, String[] vals, ArrayList<Integer> nonIndexedCols) {
        ArrayList<String[]> result = new ArrayList<>();
        for (String[] record : candidates) {
            boolean matches = true;
            for (int i : nonIndexedCols) {
                int colIndex = t.getColumnIndex(cols[i]);
                if (!record[colIndex].equals(vals[i])) {
                    matches = false;
                    break;
                }
            }
            if (matches) {
                result.add(record);
            }
        }
        return result;
    }



}