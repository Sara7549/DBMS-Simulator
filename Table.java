package DBMS;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;

public class Table implements Serializable {
    private String name;
    private String[] columnsNames;
    private int pageCount;
    private int recordsCount;
    private ArrayList<String> trace;
    private ArrayList<String> indexedColumns;
    private ArrayList<String[]> backupRecords;
    private static final long serialVersionUID = 1L;

    public Table(String name, String[] columnsNames) {
        super();
        this.name = name;
        this.columnsNames = columnsNames;
        this.trace = new ArrayList<>();
        this.indexedColumns = new ArrayList<>();
        this.backupRecords = new ArrayList<>();
        this.trace.add("Table created name:" + name + ", columnsNames:" + Arrays.toString(columnsNames));
    }

    @Override
    public String toString() {
        return "Table [name=" + name + ", columnsNames=" + Arrays.toString(columnsNames) + ", pageCount=" + pageCount
                + ", recordsCount=" + recordsCount + ", Indexed Columns: " + indexedColumns + "]";
    }

    public void insert(String[] record) {
        long startTime = System.currentTimeMillis();
        Page current = FileManager.loadTablePage(this.name, pageCount - 1);
        if (current == null || !current.insert(record)) {
            current = new Page();
            current.insert(record);
            pageCount++;
        }
        FileManager.storeTablePage(this.name, pageCount - 1, current);
        recordsCount++;
        backupRecords.add(record.clone());
        long stopTime = System.currentTimeMillis();
        this.trace.add("Inserted:" + Arrays.toString(record) + ", at page number:" + (pageCount - 1)
                + ", execution time (mil):" + (stopTime - startTime));
    }

    public String[] fixCond(String[] cols, String[] vals) {
        String[] res = new String[columnsNames.length];
        for (int i = 0; i < res.length; i++) {
            for (int j = 0; j < cols.length; j++) {
                if (columnsNames[i].equals(cols[j])) {
                    res[i] = vals[j];
                }
            }
        }
        return res;
    }

    public ArrayList<String[]> select(String[] cols, String[] vals) {
        String[] cond = fixCond(cols, vals);
        String tracer = "Select condition:" + Arrays.toString(cols) + "->" + Arrays.toString(vals);
        ArrayList<ArrayList<Integer>> pagesResCount = new ArrayList<>();
        ArrayList<String[]> res = new ArrayList<>();
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < pageCount; i++) {
            Page p = FileManager.loadTablePage(this.name, i);
            ArrayList<String[]> pRes = p.select(cond);
            if (pRes.size() > 0) {
                ArrayList<Integer> pr = new ArrayList<>();
                pr.add(i);
                pr.add(pRes.size());
                pagesResCount.add(pr);
                res.addAll(pRes);
            }
        }
        long stopTime = System.currentTimeMillis();
        tracer += ", Records per page:" + pagesResCount + ", records:" + res.size()
                + ", execution time (mil):" + (stopTime - startTime);
        this.trace.add(tracer);
        return res;
    }

    public ArrayList<String[]> select(int pageNumber, int recordNumber) {
        String tracer = "Select pointer page:" + pageNumber + ", record:" + recordNumber;
        ArrayList<String[]> res = new ArrayList<>();
        long startTime = System.currentTimeMillis();
        Page p = FileManager.loadTablePage(this.name, pageNumber);
        ArrayList<String[]> pRes = p.select(recordNumber);
        if (pRes.size() > 0) {
            res.addAll(pRes);
        }
        long stopTime = System.currentTimeMillis();
        tracer += ", total output count:" + res.size()
                + ", execution time (mil):" + (stopTime - startTime);
        this.trace.add(tracer);
        return res;
    }

    public ArrayList<String[]> select() {
        ArrayList<String[]> res = new ArrayList<>();
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < pageCount; i++) {
            Page p = FileManager.loadTablePage(this.name, i);
            if (p != null) {
                res.addAll(p.select());
            }
        }
        long stopTime = System.currentTimeMillis();
        this.trace.add("Select all pages:" + pageCount + ", records:" + res.size()
                + ", execution time (mil):" + (stopTime - startTime));
        return res;
    }

    public String getFullTrace() {
        String res = "";
        for (int i = 0; i < this.trace.size(); i++) {
            res += this.trace.get(i) + "\n";
        }
        return res + "Pages Count: " + pageCount + ", Records Count: " + recordsCount
                + ", Indexed Columns: " + indexedColumns;
    }

    public String getLastTrace() {
        return this.trace.get(this.trace.size() - 1);
    }

    public int getColumnIndex(String colName) {
        for (int i = 0; i < columnsNames.length; i++) {
            if (columnsNames[i].equals(colName)) {
                return i;
            }
        }
        return -1;
    }

    public ArrayList<String> getIndexedColumns() {
        return indexedColumns;
    }

    public void addIndexedColumn(String colName) {
        if (!indexedColumns.contains(colName)) {
            indexedColumns.add(colName);
        }
    }

    public ArrayList<String[]> getBackupRecords() {
        return backupRecords;
    }

    public int getOriginalPageNum(String[] record) {
        int recordIndex = -1;
        for (int i = 0; i < backupRecords.size(); i++) {
            if (Arrays.equals(backupRecords.get(i), record)) {
                recordIndex = i;
                break;
            }
        }
        if (recordIndex == -1) {
            return -1; // Indicate record not found
        }
        return recordIndex / DBApp.dataPageSize;
    }

    public String getName() {
        return name;
    }

    public int getPageCount() {
        return pageCount;
    }

    public void setPageCount(int newPageCount) {
        this.pageCount = newPageCount;
    }

    public ArrayList<String> getTrace() {
        return trace;
    }

    public String[] getColumnNames() {
        return columnsNames;
    }
}