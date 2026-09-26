package com.sayonora.wire.firestorewire;

import io.grpc.Status;

/** A Firestore API error: a canonical gRPC status code plus the message real Firestore returns. */
final class FsException extends RuntimeException {

    final Status.Code code;

    FsException(Status.Code code, String message) {
        super(message, null, false, false);
        this.code = code;
    }

    static FsException invalid(String m) {
        return new FsException(Status.Code.INVALID_ARGUMENT, m);
    }

    static FsException notFound(String m) {
        return new FsException(Status.Code.NOT_FOUND, m);
    }

    static FsException exists(String m) {
        return new FsException(Status.Code.ALREADY_EXISTS, m);
    }

    static FsException precondition(String m) {
        return new FsException(Status.Code.FAILED_PRECONDITION, m);
    }

    static FsException aborted(String m) {
        return new FsException(Status.Code.ABORTED, m);
    }

    static FsException unimplemented(String m) {
        return new FsException(Status.Code.UNIMPLEMENTED, m);
    }

    static FsException internal(String m) {
        return new FsException(Status.Code.INTERNAL, m);
    }

    static FsException unavailable(String m) {
        return new FsException(Status.Code.UNAVAILABLE, m);
    }

    Status toStatus() {
        return Status.fromCode(code).withDescription(getMessage());
    }

    /** HTTP status of the REST error envelope. */
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

    String statusName() {
        return code.name();
    }
}
