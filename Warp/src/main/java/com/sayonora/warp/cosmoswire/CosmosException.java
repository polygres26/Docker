package com.sayonora.warp.cosmoswire;

import java.util.LinkedHashMap;
import java.util.Map;

/** An error answered to the client with the HTTP status, Cosmos error {@code code} and optional {@code x-ms-substatus}. */
final class CosmosException extends RuntimeException {

    final int status;
    final String code;
    final int substatus;
    final Map<String, String> headers = new LinkedHashMap<>();

    CosmosException(int status, String code, String message) {
        this(status, code, 0, message);
    }

    CosmosException(int status, String code, int substatus, String message) {
        super(message, null, false, false);
        this.status = status;
        this.code = code;
        this.substatus = substatus;
    }

    static CosmosException badRequest(String message) {
        return new CosmosException(400, "BadRequest", message);
    }

    static CosmosException notFound(String message) {
        return new CosmosException(404, "NotFound", message);
    }

    static CosmosException conflict(String message) {
        return new CosmosException(409, "Conflict", message);
    }

    static CosmosException precondition(String message) {
        return new CosmosException(412, "PreconditionFailed", message);
    }

    static CosmosException unsupported(String message) {
        return new CosmosException(501, "NotImplemented", message);
    }

    static CosmosException internal(String message) {
        return new CosmosException(500, "InternalServerError", message);
    }

    /** The Cosmos-style error text {@code Message: {"Errors":["..."]}}. */
    String wireMessage(String activityId) {
        String m = getMessage() == null ? "" : getMessage();
        String inner = m.startsWith("{") ? m : "{\"Errors\":[" + quote(m) + "]}";
        return "Message: " + inner + "\r\nActivityId: " + activityId + ", Microsoft.Azure.Documents.Common/2.14.0";
    }

    private static String quote(String s) {
        return new com.google.gson.JsonPrimitive(s).toString();
    }
}
