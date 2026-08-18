package com.polygres.wire.core.access;

import com.polygres.wire.core.AccessContext;
import java.sql.Connection;
import java.sql.SQLException;

public interface NativeRlsSessionInitializer {

    void initialize(Connection connection, AccessContext accessContext) throws SQLException;
}
