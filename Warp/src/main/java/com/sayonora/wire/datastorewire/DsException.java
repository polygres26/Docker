package com.sayonora.wire.datastorewire;

import io.grpc.Status;

/** A Datastore API error: a canonical gRPC status code plus the message Cloud Datastore returns. */
final class DsException extends RuntimeException {

    final Status.Code code;

    DsException(Status.Code code, String message) {
        super(message, null, false, false);
        this.code = code;
    }

    static DsException invalid(String m) {
        return new DsException(Status.Code.INVALID_ARGUMENT, m);
    }

    static DsException notFound(String m) {
        return new DsException(Status.Code.NOT_FOUND, m);
    }

    static DsException exists(String m) {
        return new DsException(Status.Code.ALREADY_EXISTS, m);
    }

    static DsException aborted(String m) {
        return new DsException(Status.Code.ABORTED, m);
    }

    static DsException internal(String m) {
        return new DsException(Status.Code.INTERNAL, m);
    }

    static DsException unavailable(String m) {
        return new DsException(Status.Code.UNAVAILABLE, m);
    }

    static DsException unimplemented(String m) {
        return new DsException(Status.Code.UNIMPLEMENTED, m);
    }

    Status toStatus() {
        return Status.fromCode(code).withDescription(getMessage());
    }

    int httpStatus() {
        return switch (code) {
            case OK -> 200;
            case CANCELLED -> 499;
            case INVALID_ARGUMENT, FAILED_PRECONDITION, OUT_OF_RANGE -> 400;
            case DEADLINE_EXCEEDED -> 504;
            case NOT_FOUND -> 404;
            case ALREADY_EXISTS, ABORTED -> 409;
            case PERMISSION_DENIED -> 403;
            case RESOURCE_EXHAUSTED -> 429;
            case UNIMPLEMENTED -> 501;
            case UNAVAILABLE -> 503;
            case UNAUTHENTICATED -> 401;
            default -> 500;
        };
    }
}
