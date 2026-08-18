package com.polygres.advisor.catalog;

import com.polygres.advisor.core.BackendTarget;
import java.sql.SQLException;
import java.util.List;

/** One implementation per source dialect -- backs the Parameters tab. */
public interface ParameterReader {
    List<ParameterInfo> listParameters(BackendTarget target) throws SQLException;
}
