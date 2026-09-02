package com.sayonora.dms.catalog;

/** One database/session-level parameter -- Oracle's {@code V$PARAMETER}, MySQL/MariaDB's {@code SHOW VARIABLES}, SQL Server's {@code sys.configurations}, all mapped into this one shape. */
public record ParameterInfo(String name, String value, String defaultValue, boolean isDefault, String description) {}
