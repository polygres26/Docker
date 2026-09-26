package com.sayonora.wire.azurewire;

/** The Azure error catalogue (codes, statuses, messages) the services share. */
final class AzErrors {

    private AzErrors() {
    }

    static AzureException containerNotFound() {
        return new AzureException(404, "ContainerNotFound", "The specified container does not exist.");
    }

    static AzureException blobNotFound() {
        return new AzureException(404, "BlobNotFound", "The specified blob does not exist.");
    }

    static AzureException conditionNotMet() {
        return new AzureException(412, "ConditionNotMet", "The condition specified using HTTP conditional header(s) is not met.");
    }

    static AzureException invalidHeader(String name, String value) {
        AzureException e = new AzureException(400, "InvalidHeaderValue",
                "The value for one of the HTTP headers is not in the correct format.");
        e.extra("HeaderName", name).extra("HeaderValue", value == null ? "" : value);
        return e;
    }

    static AzureException invalidQuery(String name, String value) {
        AzureException e = new AzureException(400, "InvalidQueryParameterValue",
                "Value for one of the query parameters specified in the request URI is invalid.");
        e.extra("QueryParameterName", name).extra("QueryParameterValue", value == null ? "" : value);
        return e;
    }

    static AzureException outOfRange(String name, String value, String min, String max) {
        AzureException e = new AzureException(400, "OutOfRangeQueryParameterValue",
                "One of the query parameters specified in the request URI is outside the permissible range.");
        e.extra("QueryParameterName", name).extra("QueryParameterValue", value);
        if (min != null) {
            e.extra("MinimumAllowed", min);
        }
        if (max != null) {
            e.extra("MaximumAllowed", max);
        }
        return e;
    }

    static AzureException missingHeader(String name) {
        return new AzureException(400, "MissingRequiredHeader", "An HTTP header that's mandatory for this request is not specified.")
                .extra("HeaderName", name);
    }

    static AzureException invalidInput(String msg) {
        return new AzureException(400, "InvalidInput", msg);
    }

    static AzureException invalidXml() {
        return new AzureException(400, "InvalidXmlDocument", "XML specified is not syntactically valid.");
    }

    static AzureException unsupported(String what) {
        return new AzureException(501, "NotImplemented", "The requested operation is not implemented on the specified resource: "
                + what + " (not supported by Warp azurewire).");
    }

    static AzureException notAllowed(String method) {
        return new AzureException(405, "UnsupportedHttpVerb", "The resource doesn't support specified Http Verb.");
    }

    static AzureException invalidUri() {
        return new AzureException(400, "InvalidUri", "The requested URI does not represent any resource on the server.");
    }
}
