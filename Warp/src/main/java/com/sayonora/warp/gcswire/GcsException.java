package com.sayonora.warp.gcswire;

import java.util.LinkedHashMap;
import java.util.Map;

/** A GCS API failure: rendered as the JSON error envelope (JSON API) or the XML {@code <Error>} document (XML API). */
final class GcsException extends RuntimeException {

    final int status;
    /** JSON API {@code reason} (notFound, conditionNotMet, invalid, ...). */
    final String reason;
    /** XML API {@code Code} (NoSuchKey, PreconditionFailed, ...). */
    final String xmlCode;
    String location;
    String locationType;
    final Map<String, String> headers = new LinkedHashMap<>();

    GcsException(int status, String reason, String xmlCode, String message) {
        super(message);
        this.status = status;
        this.reason = reason;
        this.xmlCode = xmlCode;
    }

    GcsException at(String locationType, String location) {
        this.locationType = locationType;
        this.location = location;
        return this;
    }

    GcsException header(String k, String v) {
        headers.put(k, v);
        return this;
    }

    static GcsException noSuchObject(String bucket, String name) {
        return new GcsException(404, "notFound", "NoSuchKey", "No such object: " + bucket + "/" + name);
    }

    static GcsException noSuchBucket() {
        return new GcsException(404, "notFound", "NoSuchBucket", "The specified bucket does not exist.");
    }

    static GcsException precondition(String type, String location) {
        return new GcsException(412, "conditionNotMet", "PreconditionFailed",
                "At least one of the pre-conditions you specified did not hold.").at(type, location);
    }

    static GcsException invalid(String message) {
        return new GcsException(400, "invalid", "InvalidArgument", message);
    }

    static GcsException required(String message) {
        return new GcsException(400, "required", "InvalidArgument", message);
    }

    static GcsException internal(String message) {
        return new GcsException(500, "backendError", "InternalError", message);
    }
}
