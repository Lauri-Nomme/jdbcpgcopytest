import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;

import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

public abstract class TestBase implements AutoCloseable {
    protected final Fixture fixture;
    protected final String url;
    protected final String tableName;

    public TestBase(String testName, Fixture fixture, String url) throws SQLException {
        this.tableName = testName;
        this.fixture = fixture;
        this.url = url;
    }

    protected void dropTable() throws SQLException {
        try (Connection connection = connect()) {
            String sql = "DROP TABLE " + tableName;
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.execute();
            }
            connection.commit();
        }
    }

    protected void createTable() throws SQLException {
        try (Connection connection = connect()) {
            String sql = "CREATE " + (fixture.unLogged() ? "UNLOGGED " : "") + "TABLE " + tableName +
                    "(" + Joiner.on(",").join(fieldsToColumnDefs(fixture.fields())) + ")";
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.execute();
            }
            connection.commit();
        }
    }

    private List<String> fieldsToColumnDefs(List<Map.Entry<Class<?>, String>> fields) {
        return fields.stream()
                .map(field -> field.getValue() + " " + columnPgType(field.getKey()))
                .collect(Collectors.toList());
    }

    protected String columnPgType(Class<?> column) {
        if (column == Integer.class) {
            return "int4";
        } else if (column == Long.class) {
            return "int8";
        } else if (column == XTimestamp.class) {
            return "timestamp";
        }

        throw new IllegalStateException(column.getName());
    }

    protected Connection connect() throws SQLException {
        Connection connection = DriverManager.getConnection(url);
        connection.setAutoCommit(false);
        return connection;
    }

    @Override
    public void close() {
    }

    public abstract Map.Entry<Long, List<SliceStat>> run() throws SQLException, InterruptedException;

    protected List<SliceStat> insert() throws InterruptedException {
        List<SliceStat> stats = Collections.synchronizedList(new ArrayList<>());

        ArrayList<Thread> threads = new ArrayList<>();
        long rowsPerThread = fixture.numRows() / fixture.threads();
        long rowsLeft = fixture.numRows();
        for (long threadIndex = 0; threadIndex < fixture.threads(); threadIndex++) {
            long rows = Math.min(rowsLeft, threadIndex + 1 == fixture.threads() ? Long.MAX_VALUE : rowsPerThread);
            rowsLeft -= rows;

            Thread thread = new Thread(() -> {
                try {
                    stats.add(insertSlice(rows));
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            });
            threads.add(thread);
            thread.start();
        }

        for (Thread thread : threads) {
            thread.join();
        }
        return stats;
    }

    protected abstract SliceStat insertSlice(long sliceSize) throws SQLException;
}
