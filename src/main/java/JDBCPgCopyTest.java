import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.sql.SQLException;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class JDBCPgCopyTest {
    public static void main(String[] args) throws SQLException {
        Fixture fixture = new Fixture(
                2_000_000,
                100_000,
                true,
                false,
                1234, 23456, 1092840124L
        );
        printStats("batchedInsert", new BatchedInsert(fixture, args[0]).run());
    }

    private static void printStats(String name, ImmutableMap<String, Long> stats) {
        Long total = stats.get("total");
        System.out.println(
                "= " + name + " =\n" +
                stats.entrySet().stream()
                        .map(measurement -> Joiner.on("\t").join(
                                ImmutableList.of(
                                        measurement.getKey(),
                                        TimeUnit.NANOSECONDS.toMillis(measurement.getValue()) + "ms",
                                        String.format(Locale.ROOT, "%.1f%%", measurement.getValue() * 100d / total)
                                )
                        ))
                        .collect(Collectors.joining("\n"))
        );
    }
}
