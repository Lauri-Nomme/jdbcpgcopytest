import com.google.common.collect.*;
import org.postgresql.PGConnection;
import org.postgresql.copy.CopyIn;
import org.postgresql.copy.CopyManager;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.lang.System.nanoTime;

public class CopyInsert extends TestBase {
    private static final long PG_EPOCH_US = 946_684_800_000_000L;

    public CopyInsert(Fixture fixture, String url) throws SQLException {
        super(CopyInsert.class.getSimpleName(), fixture, url);
    }

    @Override
    public Map.Entry<Long, List<SliceStat>> run() throws SQLException, InterruptedException {
        createTable();
        long totalTime = -nanoTime();
        List<SliceStat> stats = insert();
        totalTime += nanoTime();

        if (fixture.cleanup()) {
            dropTable();
        }

        return Maps.immutableEntry(totalTime, stats);
    }

    @Override
    protected SliceStat insertSlice(long sliceSize) throws SQLException {
        Holder<?>[] fieldHolders = fixture.fieldValues();

        String sql = "COPY " + tableName + " (" +
                fixture.fields().stream().map(Map.Entry::getValue).collect(Collectors.joining(", "))
                + ") FROM STDIN BINARY";

        try (Connection connection = connect()) {
            long totalRows = 0;
            long totalTime = 0;
            long exportTime = 0;
            long writeToCopyTime = 0;
            long endCopyTime = 0;
            totalTime -= nanoTime();
            Buf buf = new Buf(64_000_000);

            CopyManager copyAPI = ((PGConnection) connection).getCopyAPI();

            Memo<Holder<?>[], FieldBinaryExporter> exporterCreator = new Memo<>(this::createFieldsBinaryExporter);
            for (long batchStart = 0; batchStart < sliceSize; batchStart += fixture.batchSize()) {
                long batchEnd = Math.min(batchStart + fixture.batchSize(), sliceSize);
                CopyIn copyIn = copyAPI.copyIn(sql);
                byte[] header = {
                        'P', 'G', 'C', 'O', 'P', 'Y', '\n', (byte) 255, '\r', '\n', 0,
                        0, 0, 0, 0,
                        0, 0, 0, 0,
                };
                buf.put(header);

                exportTime -= nanoTime();
                for (long batchRowIndex = batchStart; batchRowIndex < batchEnd; batchRowIndex++) {
                    exporterCreator.apply(fieldHolders)
                            .export(buf);
                }
                exportTime += nanoTime();
                totalRows += batchEnd - batchStart;

                byte[] footer = {-1, -1};
                buf.put(footer);

                writeToCopyTime -= nanoTime();
                copyIn.writeToCopy(buf.array(), buf.arrayOffset(), buf.position());
                writeToCopyTime += nanoTime();
                buf.position(0);

                endCopyTime -= nanoTime();
                long affectedRows = copyIn.endCopy();
                endCopyTime += nanoTime();
            }

            long commitTime = -nanoTime();
            connection.commit();
            commitTime += nanoTime();

            totalTime += nanoTime();
            return new SliceStat(totalRows, ImmutableMap.of(
                    "total", totalTime,
                    "export", exportTime,
                    "writeToCopy", writeToCopyTime,
                    "endCopy", endCopyTime,
                    "commit", commitTime
            ));
        }
    }

    private static class Memo<T, R> implements Function<T, R> {
        private final Function<T, R> delegate;
        private T t;
        private R r;

        private Memo(Function<T, R> delegate) {
            this.delegate = delegate;
        }

        @Override
        public R apply(T t) {
            if (this.t != t) {
                this.t = t;
                this.r = delegate.apply(t);
            }

            return this.r;
        }
    }

    private FieldBinaryExporter createFieldsBinaryExporter(Holder<?>[] fields) {
        short fieldCount = (short) fields.length;

        FieldBinaryExporter writeFieldCount = (Buf target) -> target.putShort(fieldCount);
        FieldBinaryExporter[] fbes = ImmutableList.builder()
                .add(writeFieldCount)
                .addAll(Arrays.stream(fields)
                        .map(this::createFieldExporter)
                        .collect(Collectors.toList())
                )
                .build()
                .toArray(new FieldBinaryExporter[0]);


        return (Buf target) -> {
            for (FieldBinaryExporter fbe : fbes) {
                fbe.export(target);
            }
        };
    }

    private FieldBinaryExporter createFieldExporter(Holder<?> field) {
        String type = columnPgType(field.clazz());
        switch (type) {
            case "int2":
                Short shortField = (Short) field.value();
                return (target) -> {
                    target.putInt(2);
                    target.putShort(shortField);
                };
            case "int4":
                Integer integerField = (Integer) field.value();
                return (target) -> {
                    target.putInt(4);
                    target.putInt(integerField);
                };
            case "int8":
                Long longField = (Long) field.value();
                return (target) -> {
                    target.putInt(8);
                    target.putLong(longField);
                };
            case "timestamp":
            case "timestamptz":
                XTimestamp timestampField = (XTimestamp) field.value();
                return (target) -> {
                    target.putInt(8);
                    target.putLong(timestampField.getMillis() * 1000 - PG_EPOCH_US);
                };
        }

        throw new IllegalStateException("unsupported " + type);
    }
}
