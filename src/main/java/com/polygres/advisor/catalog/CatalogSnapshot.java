package com.polygres.advisor.catalog;

import com.polygres.advisor.core.SourceDialect;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Deterministic feature inventory pulled from a source database's catalog + source text.
 * This is the ground truth {@link com.polygres.advisor.score.MigrationScorer} scores against --
 * no LLM involved in producing these counts (see project decision: scoring must be reproducible
 * and auditable; LLMs are only used downstream to explain/summarize/propose rewrites).
 */
public class CatalogSnapshot {

    public SourceDialect dialect;
    public String sourceVersion;          // e.g. "Oracle Database 19c Enterprise Edition Release 19.0.0.0.0"
    public String versionWarning;         // non-null if sourceVersion isn't the 19c baseline this rubric targets

    public int tableCount;
    public int viewCount;
    public int materializedViewCount;
    public int sequenceCount;
    public int simpleTriggerCount;
    public int complexTriggerCount;       // triggers with more than a handful of statements -- rough proxy for rewrite effort
    public int packageCount;
    public int standaloneProcedureCount;
    public int standaloneFunctionCount;
    public int dbLinkCount;
    public int scheduledJobCount;
    public int synonymCount;
    public int partitionedTableCount;

    /** Built-in package/API usage found by scanning source text, e.g. "DBMS_OUTPUT" -> 42 call sites. */
    public Map<String, Integer> builtinPackageUsage = new LinkedHashMap<>();

    /** Syntax constructs found by scanning source text, e.g. "CONNECT BY" -> 7, "(+)" outer join -> 3. */
    public Map<String, Integer> syntaxConstructUsage = new LinkedHashMap<>();

    public List<String> warnings = new ArrayList<>();
}
