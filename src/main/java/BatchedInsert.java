import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import java.sql.*;
import java.util.List;
import java.util.Map;

import static java.lang.System.nanoTime;
import static java.util.Collections.nCopies;

public class BatchedInsert extends TestBase {
    public BatchedInsert(Fixture fixture, String url) throws SQLException {
        super(BatchedInsert.class.getSimpleName(), fixture, url);
    }

    protected SliceStat insertSlice(long sliceSize) throws SQLException {
        Holder<?>[] fields = fixture.fieldValues();
        String sql = "INSERT INTO " + tableName + " VALUES(" +
                Joiner.on(",").join(
                        nCopies(fields.length, "?")
                ) + ")";

        try (Connection connection = connect()) {

            long totalRows = 0;
            long totalTime = 0;
            long setObjectTime = 0;
            long addBatchTime = 0;
            long executeBatchTime = 0;
            long commitTime = 0;
            totalTime -= nanoTime();
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                for (long batchStart = 0; batchStart < sliceSize; batchStart += fixture.batchSize()) {
                    long batchEnd = Math.min(batchStart + fixture.batchSize(), sliceSize);

                    for (long batchRowIndex = batchStart; batchRowIndex < batchEnd; batchRowIndex++) {
                        setObjectTime -= nanoTime();
                        for (int fieldIndex = 0; fieldIndex < fields.length; fieldIndex++) {
                            statement.setObject(1 + fieldIndex, fields[fieldIndex].value());
                        }
                        setObjectTime += nanoTime();

                        addBatchTime -= nanoTime();
                        statement.addBatch();
                        addBatchTime += nanoTime();
                    }

                    totalRows += batchEnd - batchStart;
                    executeBatchTime -= nanoTime();
                    statement.executeBatch();
                    executeBatchTime += nanoTime();
                }
            }
            commitTime -= nanoTime();
            connection.commit();
            commitTime += nanoTime();

            totalTime += nanoTime();
            return new SliceStat(totalRows, ImmutableMap.of(
                    "total", totalTime,
                    "setObject", setObjectTime,
                    "addBatch", addBatchTime,
                    "executeBatch", executeBatchTime,
                    "commit", commitTime
            ));
        }
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

}
