package DBMS;

import java.io.Serializable;
import java.util.HashMap;

public class BitMapIndex implements Serializable {
    private static final long serialVersionUID = 1L;
    private HashMap<String, StringBuilder> bitmaps;
    private int rowCount;

    public BitMapIndex() {
        bitmaps = new HashMap<>();
        rowCount = 0;
    }

    public void initialize(String[] values) {
        bitmaps.clear(); // Ensure we start fresh
        rowCount = 0;
        for (String value : values) {
            insert(value);
        }
    }

    public void insert(String value) {
        
        bitmaps.putIfAbsent(value, new StringBuilder());
        
        for (StringBuilder bitmap : bitmaps.values()) {
            while (bitmap.length() < rowCount) {
                bitmap.append('0');
            }
        }
        
        bitmaps.get(value).append('1');
        
        for (String key : bitmaps.keySet()) {
            if (!key.equals(value)) {
                bitmaps.get(key).append('0');
            }
        }
        rowCount++;
    }

    public String getBitstream(String value) {
        StringBuilder bitmap = bitmaps.get(value);
        return bitmap != null ? bitmap.toString() : null;
    }
}