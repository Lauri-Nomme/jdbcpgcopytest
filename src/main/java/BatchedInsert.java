import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import static java.lang.System.nanoTime;
import static java.util.Collections.nCopies;

public class BatchedInsert extends TestBase {
    public BatchedInsert(Fixture fixture, String url) throws SQLException {
        super(BatchedInsert.class.getSimpleName(), fixture, url);
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
        Holder<?>[] fields = fixture.fieldValues();
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
                        statement.setObject(1 + fieldIndex, fields[fieldIndex].value());
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

}
