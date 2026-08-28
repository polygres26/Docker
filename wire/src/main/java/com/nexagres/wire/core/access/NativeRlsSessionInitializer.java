package com.nexagres.wire.core.access;

import com.nexagres.wire.core.AccessContext;
import java.sql.Connection;
import java.sql.SQLException;

public interface NativeRlsSessionInitializer {

    void initialize(Connection connection, AccessContext accessContext) throws SQLException;
}
