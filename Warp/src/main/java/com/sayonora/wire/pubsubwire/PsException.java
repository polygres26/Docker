package com.sayonora.wire.pubsubwire;

import com.google.protobuf.Any;
import com.google.rpc.ErrorInfo;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.StatusProto;
import java.util.Map;

/** A Pub/Sub API error: a gRPC status code and Google's message text, optionally with an ErrorInfo detail. */
final class PsException extends RuntimeException {

    final Status.Code code;
    final ErrorInfo info;

    PsException(Status.Code code, String message) {
        this(code, message, null);
    }

    PsException(Status.Code code, String message, ErrorInfo info) {
        super(message, null, false, false);
        this.code = code;
        this.info = info;
    }

    static PsException notFound(String shortId) {
        return new PsException(Status.Code.NOT_FOUND, "Resource not found (resource=" + shortId + ").");
    }

    static PsException exists(String shortId) {
        return new PsException(Status.Code.ALREADY_EXISTS, "Resource already exists in the project (resource=" + shortId + ").");
    }

    static PsException invalid(String message) {
        return new PsException(Status.Code.INVALID_ARGUMENT, message);
    }

    static PsException precondition(String message) {
        return new PsException(Status.Code.FAILED_PRECONDITION, message);
    }

    static PsException unimplemented(String message) {
        return new PsException(Status.Code.UNIMPLEMENTED, message);
    }

    static PsException internal(String message) {
        return new PsException(Status.Code.INTERNAL, message);
    }

    static PsException unavailable(String message) {
        return new PsException(Status.Code.UNAVAILABLE, message);
    }

    /** Exactly-once delivery failure for ack ids: INVALID_ARGUMENT with ErrorInfo metadata per ack id. */
    static PsException exactlyOnce(Map<String, String> ackIdFailures) {
        ErrorInfo.Builder b = ErrorInfo.newBuilder().setReason("EXACTLY_ONCE_ACKID_FAILURE")
                .setDomain("pubsub.googleapis.com");
        b.putAllMetadata(ackIdFailures);
        return new PsException(Status.Code.INVALID_ARGUMENT,
                "Some acknowledgement ids in the request failed: exactly-once delivery is enabled on this subscription.", b.build());
    }

    StatusRuntimeException toGrpc() {
        if (info == null) {
            return code.toStatus().withDescription(getMessage()).asRuntimeException();
        }
        com.google.rpc.Status st = com.google.rpc.Status.newBuilder().setCode(code.value()).setMessage(getMessage())
                .addDetails(Any.pack(info)).build();
        return StatusProto.toStatusRuntimeException(st, new Metadata());
    }

    int httpStatus() {
        return switch (code) {
            case OK -> 200;
            case INVALID_ARGUMENT, FAILED_PRECONDITION, OUT_OF_RANGE -> 400;
            case UNAUTHENTICATED -> 401;
            case PERMISSION_DENIED -> 403;
            case NOT_FOUND -> 404;
            case ALREADY_EXISTS, ABORTED -> 409;
            case RESOURCE_EXHAUSTED -> 429;
            case CANCELLED -> 499;
            case UNIMPLEMENTED -> 501;
            case UNAVAILABLE -> 503;
            case DEADLINE_EXCEEDED -> 504;
            default -> 500;
        };
    }
}
