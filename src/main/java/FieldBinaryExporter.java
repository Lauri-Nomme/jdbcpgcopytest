import java.sql.SQLException;

@FunctionalInterface
public interface FieldBinaryExporter {
    void export(Buf target) throws SQLException;
}
