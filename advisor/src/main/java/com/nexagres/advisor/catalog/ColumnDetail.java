package com.nexagres.advisor.catalog;

/** One column, as returned by any {@link ObjectExplorer#describeTable}. Shared across dialects so the UI's Objects tab doesn't need dialect-specific rendering. */
public record ColumnDetail(String name, String dataType, boolean nullable, String defaultValue) {}
