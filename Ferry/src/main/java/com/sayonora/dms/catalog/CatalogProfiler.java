package com.sayonora.dms.catalog;

import com.sayonora.dms.core.BackendTarget;
import java.sql.SQLException;

/** One implementation per source dialect. Oracle ships first; MariaDB/MySQL is next (see README). */
public interface CatalogProfiler {
    CatalogSnapshot profile(BackendTarget target) throws SQLException;
}
