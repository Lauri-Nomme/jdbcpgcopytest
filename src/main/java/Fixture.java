import com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Fixture {
    private final Holder<?>[] fields;
    private final long numRows;
    private final long batchSize;
    private final long threads;
    private final boolean cleanup;
    private final boolean unlogged;

    public Fixture(long numRows, long batchSize, long threads, boolean cleanup, boolean unlogged, Holder<?>... fields) {
        this.numRows = numRows;
        this.batchSize = batchSize;
        this.threads = threads;
        this.cleanup = cleanup;
        this.unlogged = unlogged;
        this.fields = fields;
    }

    public List<Map.Entry<Class<?>, String>> fields() {
        List<Map.Entry<Class<?>, String>> res = new ArrayList<>();

        for (int i = 0; i < fields.length; i++) {
            Holder<?> field = fields[i];
            res.add(Maps.immutableEntry(field.clazz(), "f" + i));
        }

        return res;
    }

    public Holder<?>[] fieldValues() {
        return fields;
    }

    public long numRows() {
        return numRows;
    }

    public long batchSize() {
        return batchSize;
    }

    public long threads() {
        return threads;
    }

    public boolean cleanup() {
        return cleanup;
    }

    public boolean unLogged() {
        return unlogged;
    }
}
