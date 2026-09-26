package com.sayonora.wire.ab;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Set;

/** Names an operation and decides read (idempotent, safe to compare) vs write, per store family. */
final class AbOps {

    /** Wire family of a store: decides how the operation is read from the request. */
    enum Kind { S3, DYNAMO, SQS }

    private static final Set<String> DYNAMO_READS = Set.of("GetItem", "Query", "Scan", "BatchGetItem", "TransactGetItems",
            "DescribeTable", "ListTables", "DescribeTimeToLive", "DescribeEndpoints", "DescribeLimits", "ListTagsOfResource",
            "DescribeContinuousBackups");
    private static final Set<String> SQS_READS = Set.of("GetQueueUrl", "ListQueues", "GetQueueAttributes", "ListQueueTags",
            "ListDeadLetterSourceQueues", "ListMessageMoveTasks");

    record Op(String name, boolean read) {
    }

    private AbOps() {
    }

    static Op classify(Kind kind, String method, String rawPath, String rawQuery, String amzTarget, byte[] body,
            boolean hasCopySource) {
        switch (kind) {
            case DYNAMO: {
                String op = amzTarget != null && amzTarget.contains(".") ? amzTarget.substring(amzTarget.indexOf('.') + 1) : "Unknown";
                return new Op(op, DYNAMO_READS.contains(op));
            }
            case SQS: {
                String op = null;
                if (amzTarget != null && amzTarget.contains(".")) {
                    op = amzTarget.substring(amzTarget.indexOf('.') + 1);
                } else {
                    op = formParam(rawQuery, "Action");
                    if (op == null && body != null) {
                        op = formParam(new String(body, StandardCharsets.UTF_8), "Action");
                    }
                }
                op = op == null ? "Unknown" : op;
                return new Op(op, SQS_READS.contains(op));
            }
            default:
                return s3(method, rawPath, rawQuery, hasCopySource);
        }
    }

    private static Op s3(String method, String rawPath, String rawQuery, boolean copy) {
        String p = rawPath == null ? "/" : rawPath;
        String rest = p.startsWith("/") ? p.substring(1) : p;
        int slash = rest.indexOf('/');
        boolean hasBucket = !rest.isEmpty();
        boolean hasKey = slash >= 0 && slash < rest.length() - 1;
        String q = rawQuery == null ? "" : rawQuery;
        boolean sub = q.startsWith("uploads") || q.contains("&uploads") || q.contains("uploadId=");
        String m = method.toUpperCase(java.util.Locale.ROOT);
        if (m.equals("GET") || m.equals("HEAD")) {
            String name = !hasBucket ? "ListBuckets" : !hasKey ? (m.equals("HEAD") ? "HeadBucket" : "ListObjects")
                    : (m.equals("HEAD") ? "HeadObject" : "GetObject");
            return new Op(name, true);
        }
        if (m.equals("POST") && q.contains("select")) {
            return new Op("SelectObjectContent", true);
        }
        String name = switch (m) {
            case "PUT" -> !hasKey ? "CreateBucket" : copy ? "CopyObject" : q.contains("partNumber") ? "UploadPart" : "PutObject";
            case "DELETE" -> !hasKey ? "DeleteBucket" : q.contains("uploadId") ? "AbortMultipartUpload" : "DeleteObject";
            case "POST" -> q.contains("delete") ? "DeleteObjects" : q.contains("uploads") ? "CreateMultipartUpload"
                    : q.contains("uploadId") ? "CompleteMultipartUpload" : "PostObject";
            default -> m;
        };
        return new Op(name, false);
    }

    static String formParam(String form, String name) {
        if (form == null) {
            return null;
        }
        for (String part : form.split("&")) {
            int eq = part.indexOf('=');
            if (eq > 0 && part.substring(0, eq).equals(name)) {
                return URLDecoder.decode(part.substring(eq + 1), StandardCharsets.UTF_8);
            }
        }
        return null;
    }

    static Map<String, String> none() {
        return Map.of();
    }
}
