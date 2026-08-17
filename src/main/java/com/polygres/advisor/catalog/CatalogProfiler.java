package com.polygres.advisor.catalog;

import com.polygres.advisor.core.BackendTarget;
import java.sql.SQLException;

/** One implementation per source dialect. Oracle ships first; MariaDB/MySQL is next (see README). */
public interface CatalogProfiler {
    CatalogSnapshot profile(BackendTarget target) throws SQLException;
}
