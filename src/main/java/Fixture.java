import com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Fixture {
    private final Comparable<?>[] fields;
    private final long numRows;
    private final long batchSize;
    private final boolean cleanup;
    private boolean unlogged;

    public Fixture(long numRows, long batchSize, boolean cleanup, boolean unlogged, Comparable<?>... fields) {
        this.numRows = numRows;
        this.batchSize = batchSize;
        this.cleanup = cleanup;
        this.unlogged = unlogged;
        this.fields = fields;
    }

    public List<Map.Entry<Class<?>, String>> fields() {
        List<Map.Entry<Class<?>, String>> res = new ArrayList<>();

        for (int i = 0; i < fields.length; i++) {
            Comparable<?> field = fields[i];
            res.add(Maps.immutableEntry(field.getClass(), "f" + i));
        }

        return res;
    }

    public Comparable<?>[] fieldValues() {
        return fields;
    }

    public long numRows() {
        return numRows;
    }

    public long batchSize() {
        return batchSize;
    }

    public boolean cleanup() {
        return cleanup;
    }

    public boolean unLogged() {
        return unlogged;
    }
}
