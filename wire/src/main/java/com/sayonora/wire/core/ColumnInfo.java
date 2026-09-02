package com.sayonora.wire.core;

import java.io.Serializable;

public record ColumnInfo(String name, int jdbcType, int precision, int scale, int displaySize, boolean nullable)
        implements Serializable {
}
