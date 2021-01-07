import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;

import java.sql.*;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public abstract class TestBase implements AutoCloseable {
    protected final Fixture fixture;
    protected final String url;
    protected final String tableName;
    protected Connection connection;

    public TestBase(String testName, Fixture fixture, String url) throws SQLException {
        this.tableName = testName;
        this.fixture = fixture;
        this.url = url;
        connect();
    }

    protected void dropTable() throws SQLException {
        String sql = "DROP TABLE " + tableName;
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.execute();
        }
        connection.commit();
    }

    protected void createTable() throws SQLException {
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

    protected void connect() throws SQLException {
        connection = DriverManager.getConnection(url);
        connection.setAutoCommit(false);
    }

    @Override
    public void close() {
        try {
            connection.close();
        } catch (SQLException ignored) {
        }
        connection = null;
    }

    public abstract ImmutableMap<String, Long> run() throws SQLException;
}
