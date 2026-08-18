package com.polygres.wire.dynamowire;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class DynamoWireServer {

    private static final Logger log = LoggerFactory.getLogger(DynamoWireServer.class);
    private static final String TARGET_PREFIX = "DynamoDB_20120810.";

    private final Server server;
    private final PgItemStore store;
    private final OperationHandlers handlers;

    public DynamoWireServer(int port, String pgHost, int pgPort, String pgDatabase, String pgUser, String pgPassword) {
        this(port, pgHost, pgPort, pgDatabase, pgUser, pgPassword, null);
    }

    public DynamoWireServer(int port, String pgHost, int pgPort, String pgDatabase, String pgUser, String pgPassword,
            DynamoCache cache) {
        this(port, pgHost, pgPort, pgDatabase, pgUser, pgPassword, cache, com.polygres.wire.acl.ConnectionGate.DISABLED);
    }

    public DynamoWireServer(int port, String pgHost, int pgPort, String pgDatabase, String pgUser, String pgPassword,
            DynamoCache cache, com.polygres.wire.acl.ConnectionGate connectionGate) {
        this(port, pgHost, pgPort, pgDatabase, pgUser, pgPassword, cache, connectionGate,
                com.polygres.wire.http.auth.AccessContextResolver.DISABLED,
                com.polygres.wire.dynamowire.auth.AwsIamCredentialStore.DISABLED);
    }

    public DynamoWireServer(int port, String pgHost, int pgPort, String pgDatabase, String pgUser, String pgPassword,
            DynamoCache cache, com.polygres.wire.acl.ConnectionGate connectionGate,
            com.polygres.wire.http.auth.AccessContextResolver oauth) {
        this(port, pgHost, pgPort, pgDatabase, pgUser, pgPassword, cache, connectionGate, oauth,
                com.polygres.wire.dynamowire.auth.AwsIamCredentialStore.DISABLED);
    }

    public DynamoWireServer(int port, String pgHost, int pgPort, String pgDatabase, String pgUser, String pgPassword,
            DynamoCache cache, com.polygres.wire.acl.ConnectionGate connectionGate,
            com.polygres.wire.http.auth.AccessContextResolver oauth,
            com.polygres.wire.dynamowire.auth.AwsIamCredentialStore awsIamCredentials) {
        this.store = new PgItemStore(pgHost, pgPort, pgDatabase, pgUser, pgPassword);
        this.handlers = new OperationHandlers(store, cache);
        com.polygres.wire.dynamowire.auth.SigV4Verifier sigV4Verifier =
                new com.polygres.wire.dynamowire.auth.SigV4Verifier(awsIamCredentials);
        this.server = new Server(port);
        server.setHandler(new AbstractHandler() {
            @Override
            public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException {
                baseRequest.setHandled(true);
                if (!connectionGate.acceptHttp(request)) {
                    writeError(response, 403, "AccessDeniedException", "forbidden");
                    return;
                }
                String body = new String(request.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
                if (awsIamCredentials.isEnabled()) {
                    com.polygres.wire.dynamowire.auth.SigV4Verifier.Result sigResult = sigV4Verifier.verify(request, body);
                    if (!sigResult.valid()) {
                        log.warn("SigV4: rejecting request -- {}", sigResult.reason());
                        writeError(response, 401, "UnrecognizedClientException", sigResult.reason());
                        return;
                    }
                } else if (oauth.enforce(request, response) == null) {
                    return;
                }
                handleRequest(request, response, body);
            }
        });
    }

    private void handleRequest(HttpServletRequest request, HttpServletResponse response, String body) throws IOException {
        response.setContentType("application/x-amz-json-1.0");
        String amzTarget = request.getHeader("X-Amz-Target");
        if (amzTarget == null || !amzTarget.startsWith(TARGET_PREFIX)) {
            writeError(response, 400, "UnknownOperationException", "Missing or unrecognized X-Amz-Target header: " + amzTarget);
            return;
        }
        String operation = amzTarget.substring(TARGET_PREFIX.length());
        JsonObject requestJson;
        try {
            requestJson = body.isBlank() ? new JsonObject() : JsonParser.parseString(body).getAsJsonObject();
        } catch (RuntimeException e) {
            writeError(response, 400, "SerializationException", "Could not parse request body as JSON: " + e.getMessage());
            return;
        }
        try {
            JsonObject responseJson = handlers.dispatch(operation, requestJson);
            response.setStatus(HttpServletResponse.SC_OK);
            response.getWriter().write(responseJson.toString());
        } catch (DynamoException e) {
            writeError(response, statusForError(e.dynamoErrorType), e.dynamoErrorType, e.getMessage());
        } catch (RuntimeException e) {
            log.error("dynamowire operation {} failed", operation, e);
            writeError(response, 500, "InternalFailure", String.valueOf(e.getMessage()));
        }
    }

    private static int statusForError(String type) {
        return switch (type) {
            case "ResourceNotFoundException", "TableNotFoundException" -> 400;
            case "ResourceInUseException", "ConditionalCheckFailedException", "ValidationException",
                    "TransactionCanceledException" -> 400;
            default -> 400;
        };
    }

    private void writeError(HttpServletResponse response, int status, String errorType, String message) throws IOException {
        JsonObject err = new JsonObject();
        err.addProperty("__type", "com.amazonaws.dynamodb.v20120810#" + errorType);
        err.addProperty("message", message);
        response.setStatus(status);
        response.getWriter().write(err.toString());
    }

    public void start() throws Exception {
        server.start();
        log.info("polywire dynamowire (DynamoDB HTTP/JSON) listening on port {}",
                ((org.eclipse.jetty.server.ServerConnector) server.getConnectors()[0]).getPort());
    }

    public void stop() throws Exception {
        server.stop();
        store.close();
    }
}
