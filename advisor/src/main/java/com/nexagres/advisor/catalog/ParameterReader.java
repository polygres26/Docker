package com.nexagres.advisor.catalog;

import com.nexagres.advisor.core.BackendTarget;
import java.sql.SQLException;
import java.util.List;

/** One implementation per source dialect -- backs the Parameters tab. */
public interface ParameterReader {
    List<ParameterInfo> listParameters(BackendTarget target) throws SQLException;
}
