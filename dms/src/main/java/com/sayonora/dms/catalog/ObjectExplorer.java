package com.sayonora.dms.catalog;

import com.sayonora.dms.core.BackendTarget;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/** One implementation per source dialect -- backs the Objects tab's tree + detail view. */
public interface ObjectExplorer {
    /** Object name -> object type, grouped for a tree view. */
    Map<String, List<String>> listObjects(BackendTarget target) throws SQLException;

    List<ColumnDetail> describeTable(BackendTarget target, String tableName) throws SQLException;

    /** Full source for a routine/trigger-shaped object (procedure, function, trigger, ...). */
    String fetchSource(BackendTarget target, String objectName, String objectType) throws SQLException;
}
