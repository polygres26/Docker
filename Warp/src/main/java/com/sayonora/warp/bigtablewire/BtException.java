package com.sayonora.warp.bigtablewire;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;

/** A Bigtable API error: a gRPC status code and the message text the emulator / service uses. */
final class BtException extends RuntimeException {

    final Status.Code code;

    BtException(Status.Code code, String message) {
        super(message, null, false, false);
        this.code = code;
    }

    static BtException notFoundTable(String name) {
        return new BtException(Status.Code.NOT_FOUND, "table \"" + name + "\" not found");
    }

    static BtException invalid(String message) {
        return new BtException(Status.Code.INVALID_ARGUMENT, message);
    }

    static BtException unknown(String message) {
        return new BtException(Status.Code.UNKNOWN, message);
    }

    static BtException unimplemented(String message) {
        return new BtException(Status.Code.UNIMPLEMENTED, message);
    }

    static BtException unavailable(String message) {
        return new BtException(Status.Code.UNAVAILABLE, message);
    }

    static BtException precondition(String message) {
        return new BtException(Status.Code.FAILED_PRECONDITION, message);
    }

    StatusRuntimeException toGrpc() {
        return code.toStatus().withDescription(getMessage()).asRuntimeException();
    }
}
