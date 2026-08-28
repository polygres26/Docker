package com.nexagres.advisor.uploads;

/**
 * Metadata for one uploaded performance/workload report (AWR for Oracle, a MySQL performance
 * report, a SQL Server DMV/Query Store export, ...) -- the alternate on-ramp for customers who
 * won't hand Advisor a live connect string but will share a report their DBA already pulled. The
 * report's extracted text body is NOT held here (see {@link ReportStore} -- it lives on disk,
 * keyed by id) to keep this class cheap to list/serialize.
 */
public class UploadedReport {
    public String id;
    public String name;
    public String dialect;      // ORACLE | MYSQL | MARIADB | SQL_SERVER -- free text, not the same enum as live connections (a report may not let us detect the exact dialect the way a JDBC URL does)
    public String filename;
    public int textLength;
    public String uploadedAt;

    /** Cached JSON analysis result (see ReportAnalyzer), null until "Analyze" has been run at least once. */
    public String analysisJson;
    public String analyzedAt;

    public UploadedReport() {}
}
