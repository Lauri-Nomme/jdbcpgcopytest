import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.postgresql.PGConnection;
import org.postgresql.copy.CopyIn;
import org.postgresql.copy.CopyManager;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

import static java.lang.System.nanoTime;

public class CopyInsert extends TestBase {
    private static final long PG_EPOCH_US = 946_684_800_000_000L;
    private Holder<?>[] exporterFields = null;
    private FieldBinaryExporter exporter;

    public CopyInsert(Fixture fixture, String url) throws SQLException {
        super(CopyInsert.class.getSimpleName(), fixture, url);
    }

    @Override
    public ImmutableMap<String, Long> run() throws SQLException {
        createTable();
        ImmutableMap<String, Long> stats = insert();

        if (fixture.cleanup()) {
            dropTable();
        }

        return stats;
    }

    private ImmutableMap<String, Long> insert() throws SQLException {
        Holder<?>[] fieldHolders = fixture.fieldValues();

        String sql = "COPY " + tableName + " (" +
                fixture.fields().stream().map(Map.Entry::getValue).collect(Collectors.joining(", "))
                + ") FROM STDIN BINARY";

        long totalTime = 0;
        long exportTime = 0;
        long writeToCopyTime = 0;
        long endCopyTime = 0;
        totalTime -= nanoTime();
        Buf buf = new Buf(64_000_000);

        CopyManager copyAPI = ((PGConnection) connection).getCopyAPI();

        for (long batchIndex = 0; batchIndex < (fixture.numRows() / fixture.batchSize()); batchIndex++) {
            CopyIn copyIn = copyAPI.copyIn(sql);
            byte[] header = {
                    'P', 'G', 'C', 'O', 'P', 'Y', '\n', (byte) 255, '\r', '\n', 0,
                    0, 0, 0, 0,
                    0, 0, 0, 0,
            };
            buf.put(header);

            exportTime -= nanoTime();
            for (long batchRowIndex = 0; batchRowIndex < fixture.batchSize(); batchRowIndex++) {
                getExporterFor(fieldHolders)
                        .export(buf);
            }
            exportTime += nanoTime();

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
        return ImmutableMap.of(
                "total", totalTime,
                "export", exportTime,
                "writeToCopy", writeToCopyTime,
                "endCopy", endCopyTime,
                "commit", commitTime
        );
    }

    private FieldBinaryExporter getExporterFor(Holder<?>[] fields) {
        if (exporterFields != fields) {
            exporterFields = fields;
            exporter = createFieldsBinaryExporter(fields);
        }

        return exporter;
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
