import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class JDBCPgCopyTest {
    public static void main(String[] args) throws SQLException, InterruptedException {
        Fixture fixture = new Fixture(
                22_000_000,
                100_000,
                4,
                true,
                false,
                new Holder<>(Integer.class, 1234), new Holder<>(Integer.class, 23456), new Holder<>(Long.class, 1092840124L)
        );

        if (args.length < 2 || Arrays.asList(args).contains("batched")) {
            try (BatchedInsert batchedInsert = new BatchedInsert(fixture, args[0])) {
                printStats("batchedInsert", fixture, batchedInsert.run());
            }
        }

        if (args.length < 2 || Arrays.asList(args).contains("copy")) {
            try (CopyInsert copyInsert = new CopyInsert(fixture, args[0])) {
                printStats("copyInsert", fixture, copyInsert.run());
            }
        }
    }

    private static void printStats(String name, Fixture fixture, Map.Entry<Long, List<SliceStat>> stats) {
        Long totalTime = stats.getKey();

        System.out.println(
                "= " + name + ";  numRows = " + fixture.numRows() + "; batchSize = " + fixture.batchSize() + " \t" +
                "total time = "  + TimeUnit.NANOSECONDS.toMillis(totalTime) + "ms" + "\t" +
                        String.format(Locale.ROOT, "%.1f rows/sec", fixture.numRows() * 1d / TimeUnit.NANOSECONDS.toSeconds(totalTime)) +
                        " ="
        );

        System.out.println((stats.getValue().stream().map(stat -> {
            long threadTotal = Optional.ofNullable(stat.measurements.get("total")).orElse(0L);

            return stat.measurements.entrySet().stream()
                    .map(measurement -> Joiner.on("\t").join(
                            ImmutableList.of(
                                    measurement.getKey(),
                                    TimeUnit.NANOSECONDS.toMillis(measurement.getValue()) + "ms",
                                    String.format(Locale.ROOT, "%.1f%%", measurement.getValue() * 100d / threadTotal)
                            )
                    ))
                    .collect(Collectors.joining("\n")) + "\n" +
                    String.format(Locale.ROOT, "%d rows, %.1f rows/sec", stat.totalRows, stat.totalRows * 1d / TimeUnit.NANOSECONDS.toSeconds(threadTotal));
                })
                .collect(Collectors.joining("\n------\n"))));
    }
}
