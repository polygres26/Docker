package com.sayonora.warp.azurewire;

import java.util.LinkedHashMap;
import java.util.Map;

/** An Azure Storage error: HTTP status, {@code x-ms-error-code}, message and optional extra XML/JSON detail elements. */
public final class AzureException extends RuntimeException {

    public final int status;
    public final String code;
    /** extra {@code <Name>text</Name>} elements of the XML error body (e.g. AuthenticationErrorDetail), in order */
    public final Map<String, String> extra = new LinkedHashMap<>();
    /** extra response headers (e.g. x-ms-lease-*, Retry-After) */
    public final Map<String, String> headers = new LinkedHashMap<>();

    public AzureException(int status, String code, String message) {
        super(message);
        this.status = status;
        this.code = code;
    }

    public AzureException extra(String name, String value) {
        extra.put(name, value);
        return this;
    }

    public AzureException header(String name, String value) {
        headers.put(name, value);
        return this;
    }
}
