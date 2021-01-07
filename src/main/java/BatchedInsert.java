import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.lang.System.nanoTime;
import static java.util.Collections.nCopies;

public class BatchedInsert {
    private final Fixture fixture;
    private final String url;
    private Connection connection;
    private final String tableName = BatchedInsert.class.getSimpleName();

    public BatchedInsert(Fixture fixture, String url) {
        this.fixture = fixture;
        this.url = url;
    }

    public ImmutableMap<String, Long> run() throws SQLException {
        connection = DriverManager.getConnection(url);
        connection.setAutoCommit(false);
        createTable();
        ImmutableMap<String, Long> stats = insert();

        if (fixture.cleanup()) {
            dropTable();
        }

        return stats;
    }

    private ImmutableMap<String, Long> insert() throws SQLException {
        Comparable<?>[] fields = fixture.fieldValues();
        String sql = "INSERT INTO " + tableName + " VALUES(" +
                Joiner.on(",").join(
                        nCopies(fields.length, "?")
                ) + ")";

        long totalTime = 0;
        long setObjectTime = 0;
        long addBatchTime = 0;
        long executeBatchTime = 0;
        long commitTime = 0;
        totalTime -= nanoTime();
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            for (long batchIndex = 0; batchIndex < (fixture.numRows() / fixture.batchSize()); batchIndex++) {
                for (long batchRowIndex = 0; batchRowIndex < fixture.batchSize(); batchRowIndex++) {
                    setObjectTime -= nanoTime();
                    for (int fieldIndex = 0; fieldIndex < fields.length; fieldIndex++) {
                        statement.setObject(1 + fieldIndex, fields[fieldIndex]);
                    }
                    setObjectTime += nanoTime();
                    addBatchTime -= nanoTime();
                    statement.addBatch();
                    addBatchTime += nanoTime();
                }

                executeBatchTime -= nanoTime();
                statement.executeBatch();
                executeBatchTime += nanoTime();
            }
        }
        commitTime -= nanoTime();
        connection.commit();
        commitTime += nanoTime();

        totalTime += nanoTime();
        return ImmutableMap.of(
                "total", totalTime,
                "setObject", setObjectTime,
                "addBatch", addBatchTime,
                "executeBatch", executeBatchTime,
                "commit", commitTime
        );
    }

    private void dropTable() throws SQLException {
        String sql = "DROP TABLE " + tableName;
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.execute();
        }
        connection.commit();
    }

    private void createTable() throws SQLException {
        String sql = "CREATE " + (fixture.unLogged() ? "UNLOGGED " : "") + "TABLE " + tableName +
                "(" + Joiner.on(",").join(fieldsToColumnDefs(fixture.fields())) + ")";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.execute();
        }
        connection.commit();
    }

    private List<String> fieldsToColumnDefs(List<Map.Entry<Class<?>, String>> fields) {
        return fields.stream()
                .map(field -> field.getValue() + " " + columnPgType(field.getKey()))
                .collect(Collectors.toList());
    }

    private String columnPgType(Class<?> column) {
        if (column == Integer.class) {
            return "int4";
        } else if (column == Long.class) {
            return "int8";
        }

        throw new IllegalStateException(column.getName());
    }
}
