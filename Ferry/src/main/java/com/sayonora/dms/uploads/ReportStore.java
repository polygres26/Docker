package com.sayonora.dms.uploads;

import com.sayonora.dms.core.BackendConnectionPools;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * CRUD store for {@link UploadedReport}s -- metadata in the same embedded HSQLDB database
 * {@link com.sayonora.dms.core.ConnectionStore} uses, extracted report text on disk under
 * {@code <data dir>/reports/<id>.txt} (kept out of the database row since a real AWR report can
 * run to several MB, well past what belongs in a VARCHAR column).
 */
public class ReportStore {

    private static final String POOL_KEY = "sayonora-control";
    private final String jdbcUrl;
    private final Path reportsDir;

    public ReportStore() {
        String dataDir = System.getenv().getOrDefault("SAYONORA_DATA_DIR",
            System.getProperty("user.home") + "/.sayonora");
        this.jdbcUrl = "jdbc:hsqldb:file:" + dataDir + "/sayonora-store;shutdown=true";
        this.reportsDir = Path.of(dataDir, "reports");
        try (Connection connection = borrow(); Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE IF NOT EXISTS uploaded_reports ("
                + "id VARCHAR(64) PRIMARY KEY, "
                + "name VARCHAR(255), "
                + "dialect VARCHAR(32), "
                + "filename VARCHAR(512), "
                + "text_length INTEGER, "
                + "uploaded_at VARCHAR(64), "
                + "analysis_json CLOB, "
                + "analyzed_at VARCHAR(64))");
            Files.createDirectories(reportsDir);
        } catch (SQLException | IOException e) {
            throw new RuntimeException("Could not initialize report store at " + jdbcUrl, e);
        }
    }

    public UploadedReport create(String name, String dialect, String filename, String extractedText) {
        UploadedReport report = new UploadedReport();
        report.id = UUID.randomUUID().toString();
        report.name = (name == null || name.isBlank()) ? filename : name;
        report.dialect = dialect;
        report.filename = filename;
        report.textLength = extractedText.length();
        report.uploadedAt = Instant.now().toString();

        try {
            Files.writeString(textPath(report.id), extractedText, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException("Could not write report text to disk", e);
        }

        try (Connection connection = borrow();
             PreparedStatement ps = connection.prepareStatement(
                 "INSERT INTO uploaded_reports (id, name, dialect, filename, text_length, uploaded_at) VALUES (?, ?, ?, ?, ?, ?)")) {
            ps.setString(1, report.id);
            ps.setString(2, report.name);
            ps.setString(3, report.dialect);
            ps.setString(4, report.filename);
            ps.setInt(5, report.textLength);
            ps.setString(6, report.uploadedAt);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("Could not save report metadata", e);
        }
        return report;
    }

    public List<UploadedReport> list() {
        List<UploadedReport> reports = new ArrayList<>();
        try (Connection connection = borrow();
             Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery("SELECT * FROM uploaded_reports ORDER BY uploaded_at DESC")) {
            while (rs.next()) reports.add(fromRow(rs));
        } catch (SQLException e) {
            throw new RuntimeException("Could not list reports", e);
        }
        return reports;
    }

    public Optional<UploadedReport> get(String id) {
        try (Connection connection = borrow();
             PreparedStatement ps = connection.prepareStatement("SELECT * FROM uploaded_reports WHERE id = ?")) {
            ps.setString(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? Optional.of(fromRow(rs)) : Optional.empty();
            }
        } catch (SQLException e) {
            throw new RuntimeException("Could not read report " + id, e);
        }
    }

    public String getText(String id) {
        try {
            return Files.readString(textPath(id), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException("Could not read report text for " + id, e);
        }
    }

    public void saveAnalysis(String id, String analysisJson) {
        try (Connection connection = borrow();
             PreparedStatement ps = connection.prepareStatement(
                 "UPDATE uploaded_reports SET analysis_json = ?, analyzed_at = ? WHERE id = ?")) {
            ps.setString(1, analysisJson);
            ps.setString(2, Instant.now().toString());
            ps.setString(3, id);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("Could not save analysis for " + id, e);
        }
    }

    public boolean delete(String id) {
        try (Connection connection = borrow();
             PreparedStatement ps = connection.prepareStatement("DELETE FROM uploaded_reports WHERE id = ?")) {
            ps.setString(1, id);
            boolean removed = ps.executeUpdate() > 0;
            if (removed) Files.deleteIfExists(textPath(id));
            return removed;
        } catch (SQLException | IOException e) {
            throw new RuntimeException("Could not delete report " + id, e);
        }
    }

    private Path textPath(String id) {
        return reportsDir.resolve(id + ".txt");
    }

    private UploadedReport fromRow(ResultSet rs) throws SQLException {
        UploadedReport r = new UploadedReport();
        r.id = rs.getString("id");
        r.name = rs.getString("name");
        r.dialect = rs.getString("dialect");
        r.filename = rs.getString("filename");
        r.textLength = rs.getInt("text_length");
        r.uploadedAt = rs.getString("uploaded_at");
        r.analysisJson = rs.getString("analysis_json");
        r.analyzedAt = rs.getString("analyzed_at");
        return r;
    }

    private Connection borrow() throws SQLException {
        return BackendConnectionPools.borrow(POOL_KEY, jdbcUrl, "SA", "");
    }
}
