import com.google.common.collect.ImmutableMap;

public class SliceStat {
    public final long totalRows;
    public ImmutableMap<String, Long> measurements;

    public <K, V> SliceStat(long totalRows, ImmutableMap<String, Long> measurements) {
        this.totalRows = totalRows;
        this.measurements = measurements;
    }
}
