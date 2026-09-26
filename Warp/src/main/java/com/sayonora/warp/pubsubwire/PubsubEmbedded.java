package com.sayonora.warp.pubsubwire;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import com.sayonora.warp.core.BackendRegistry;

/**
 * In-process access to the unary Pub/Sub RPCs of pubsubwire for the MCP tools: {@code "google.pubsub.v1.Publisher/Publish"}
 * style method keys and proto3 JSON bodies, the exact table ({@link PsRpc}) the gRPC and REST transports share, so
 * semantics (validation, ordering keys, filters, dead-lettering, exactly-once, error texts) are the wire protocol's own.
 */
public final class PubsubEmbedded {

    /** A Pub/Sub API error with the gRPC status name Google would report. */
    public static final class ApiError extends RuntimeException {
        public final String status;

        ApiError(String status, String message) {
            super(status + ": " + message, null, false, false);
            this.status = status;
        }
    }

    private final PsRpc rpc;
    private final JsonFormat.Parser parser = JsonFormat.parser().ignoringUnknownFields();
    private final JsonFormat.Printer printer = JsonFormat.printer().omittingInsignificantWhitespace();

    public PubsubEmbedded(BackendRegistry registry) {
        this.rpc = new PsRpc(new PsService(new PsStore(new PsShards(registry)), PsConfig.fromEnv()));
    }

    /** Calls {@code method} (for example {@code google.pubsub.v1.Subscriber/Pull}) with a proto3 JSON body. */
    public String call(String method, String jsonBody) {
        PsRpc.Rpc r = rpc.byMethod.get(method);
        if (r == null) {
            throw new ApiError("UNIMPLEMENTED", "unknown Pub/Sub method " + method);
        }
        try {
            Message.Builder b = r.prototype().newBuilderForType();
            parser.merge(jsonBody == null || jsonBody.isBlank() ? "{}" : jsonBody, b);
            return printer.print(r.impl().apply(b.build()));
        } catch (PsException e) {
            throw new ApiError(e.code.name(), e.getMessage());
        } catch (InvalidProtocolBufferException e) {
            throw new ApiError("INVALID_ARGUMENT", e.getMessage());
        }
    }
}
