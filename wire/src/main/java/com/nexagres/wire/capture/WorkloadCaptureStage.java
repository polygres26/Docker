package com.nexagres.wire.capture;

import com.nexagres.wire.core.ExecutionResult;
import com.nexagres.wire.core.PipelineChain;
import com.nexagres.wire.core.PipelineStage;
import com.nexagres.wire.core.Statement;
import java.sql.SQLException;

/**
 * Opt-in {@link PipelineStage} that records every statement into this process's
 * {@link WorkloadCaptureBuffer}, in true arrival order, for later cross-instance replay via
 * {@code WorkloadReplayer}.
 *
 * <p>Placed immediately after {@code FirewallStage} in {@code Main}'s stage list -- after
 * auth/firewall admission (so denied statements aren't captured), but before {@code RouterStage}
 * / {@code QosControlStage} / {@code DialectTranslationStage} rewrite the statement's routing or
 * SQL text. A replay should reproduce what the client actually sent, not a particular backend's
 * translated form of it.
 *
 * <p>{@link WorkloadCaptureBuffer#append} runs synchronously, before {@code next.proceed(statement)}
 * is called -- i.e. capture order reflects arrival order at this instance, not completion order (a
 * slow statement finishing late must not reorder it after a faster one that arrived after it). The
 * append is a plain in-memory operation (no I/O), so unlike the pipeline's other optional stages
 * there's no failure mode here worth swallowing/logging -- it can't fail short of an OOM.
 */
public final class WorkloadCaptureStage implements PipelineStage {

    private final WorkloadCaptureBuffer buffer;

    public WorkloadCaptureStage(WorkloadCaptureBuffer buffer) {
        this.buffer = buffer;
    }

    @Override
    public ExecutionResult handle(Statement statement, PipelineChain next) throws SQLException {
        buffer.append(statement);
        return next.proceed(statement);
    }
}
