package com.sayonora.warp.s3wire;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

/*
 * Portions adapted from Floci (https://github.com/floci-io/floci), MIT License, Copyright (c) 2025 Floci and its contributors
 * (S3PostPolicySigner: credential-scope / signature verification of presigned POST policies).
 */

/**
 * Browser form uploads ({@code POST /bucket} with {@code multipart/form-data}): a small in-memory
 * multipart parser and the POST policy document check (expiration, conditions, signature).
 */
final class S3PostForm {

    private S3PostForm() {
    }

    /** {@code fields} keys are lower-cased; the file part is the last part of the form. */
    record Form(Map<String, String> fields, String filename, String fileContentType, byte[] file) {
    }

    private static int indexOf(byte[] hay, byte[] needle, int from) {
        outer:
        for (int i = from; i <= hay.length - needle.length; i++) {
            for (int j = 0; j < needle.length; j++) {
                if (hay[i + j] != needle[j]) {
                    continue outer;
                }
            }
            return i;
        }
        return -1;
    }

    static String boundary(String contentType) {
        for (String part : contentType.split(";")) {
            String p = part.trim();
            if (p.toLowerCase(Locale.ROOT).startsWith("boundary=")) {
                String b = p.substring(9).trim();
                return b.startsWith("\"") && b.endsWith("\"") && b.length() >= 2 ? b.substring(1, b.length() - 1) : b;
            }
        }
        throw new S3WireException(400, "InvalidArgument", "POST requires a multipart/form-data boundary");
    }

    static Form parse(byte[] body, String boundary) {
        byte[] delim = ("--" + boundary).getBytes(StandardCharsets.ISO_8859_1);
        byte[] sep = ("\r\n--" + boundary).getBytes(StandardCharsets.ISO_8859_1);
        Map<String, String> fields = new LinkedHashMap<>();
        int pos = indexOf(body, delim, 0);
        if (pos < 0) {
            throw new S3WireException(400, "MalformedPOSTRequest", "The body of your POST request is not well-formed multipart/form-data.");
        }
        pos += delim.length;
        String filename = null;
        String fileType = null;
        byte[] file = null;
        while (pos < body.length) {
            if (pos + 1 < body.length && body[pos] == '-' && body[pos + 1] == '-') {
                break; // closing delimiter
            }
            if (pos + 1 < body.length && body[pos] == '\r' && body[pos + 1] == '\n') {
                pos += 2;
            }
            int hdrEnd = indexOf(body, new byte[] {'\r', '\n', '\r', '\n'}, pos);
            if (hdrEnd < 0) {
                throw new S3WireException(400, "MalformedPOSTRequest", "The body of your POST request is not well-formed multipart/form-data.");
            }
            String headers = new String(body, pos, hdrEnd - pos, StandardCharsets.UTF_8);
            int start = hdrEnd + 4;
            int end = indexOf(body, sep, start);
            if (end < 0) {
                throw new S3WireException(400, "MalformedPOSTRequest", "The body of your POST request is not well-formed multipart/form-data.");
            }
            String name = null;
            String fname = null;
            String ctype = null;
            for (String line : headers.split("\r\n")) {
                String lower = line.toLowerCase(Locale.ROOT);
                if (lower.startsWith("content-disposition:")) {
                    for (String attr : line.substring(line.indexOf(':') + 1).split(";")) {
                        String a = attr.trim();
                        if (a.toLowerCase(Locale.ROOT).startsWith("name=")) {
                            name = unquote(a.substring(5));
                        } else if (a.toLowerCase(Locale.ROOT).startsWith("filename=")) {
                            fname = unquote(a.substring(9));
                        }
                    }
                } else if (lower.startsWith("content-type:")) {
                    ctype = line.substring(line.indexOf(':') + 1).trim();
                }
            }
            if (name == null) {
                throw new S3WireException(400, "MalformedPOSTRequest", "A form part has no name.");
            }
            if (fname != null || "file".equalsIgnoreCase(name)) {
                filename = fname == null ? "" : fname;
                fileType = ctype;
                file = java.util.Arrays.copyOfRange(body, start, end);
            } else {
                fields.put(name.toLowerCase(Locale.ROOT), new String(body, start, end - start, StandardCharsets.UTF_8));
            }
            pos = end + sep.length;
        }
        if (file == null) {
            throw new S3WireException(400, "InvalidArgument", "POST requires exactly one file upload per request.", null,
                    Map.of("ArgumentName", "file", "ArgumentValue", "0"));
        }
        return new Form(fields, filename, fileType, file);
    }

    private static String unquote(String s) {
        String t = s.trim();
        return t.length() >= 2 && t.startsWith("\"") && t.endsWith("\"") ? t.substring(1, t.length() - 1) : t;
    }

    // ---- policy -----------------------------------------------------------------------------------

    private static byte[] hmac(byte[] key, String data) throws Exception {
        Mac mac = Mac.getInstance("HmacSHA256");
        mac.init(new SecretKeySpec(key, "HmacSHA256"));
        return mac.doFinal(data.getBytes(StandardCharsets.UTF_8));
    }

    static boolean validCredentialScope(String credential) {
        String[] p = credential.split("/");
        return p.length == 5 && "s3".equals(p[3]) && "aws4_request".equals(p[4]);
    }

    /** SigV4 signature of the base64 policy document: HMAC(signingKey, policyBase64). */
    static boolean verifySignature(String policyBase64, String credential, String signatureHex, String secret) {
        try {
            String[] p = credential.split("/");
            byte[] k = hmac(("AWS4" + secret).getBytes(StandardCharsets.UTF_8), p[1]);
            k = hmac(k, p[2]);
            k = hmac(k, "s3");
            k = hmac(k, "aws4_request");
            String expected = S3SigV4Verifier.hex(hmac(k, policyBase64));
            return MessageDigest.isEqual(expected.getBytes(StandardCharsets.UTF_8),
                    signatureHex.toLowerCase(Locale.ROOT).getBytes(StandardCharsets.UTF_8));
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Checks a decoded policy against the form: expiration, every condition, and that no form field lacks a
     * matching condition (S3: "Extra input fields").
     *
     * @param urlBucket the bucket the request addressed (implicit {@code bucket} condition)
     * @throws S3WireException 403 AccessDenied "Invalid according to Policy: ..." / 400 EntityTooLarge / EntityTooSmall
     */
    static void checkPolicy(String policyJson, Map<String, String> fields, long fileLength, String urlBucket, Instant now) {
        JsonObject policy;
        try {
            policy = JsonParser.parseString(policyJson).getAsJsonObject();
        } catch (RuntimeException e) {
            throw new S3WireException(400, "InvalidPolicyDocument", "Invalid Policy: Invalid JSON.");
        }
        if (!policy.has("expiration")) {
            throw new S3WireException(400, "InvalidPolicyDocument", "Invalid Policy: Policy missing expiration.");
        }
        try {
            if (Instant.parse(policy.get("expiration").getAsString()).isBefore(now)) {
                throw new S3WireException(403, "AccessDenied", "Invalid according to Policy: Policy expired.");
            }
        } catch (java.time.format.DateTimeParseException e) {
            throw new S3WireException(400, "InvalidPolicyDocument", "Invalid Policy: Invalid 'expiration' value.");
        }
        Map<String, String> effective = new LinkedHashMap<>(fields);
        effective.put("bucket", urlBucket);
        java.util.Set<String> covered = new java.util.HashSet<>(java.util.List.of("policy", "x-amz-signature", "file",
                "bucket"));
        JsonArray conds = policy.has("conditions") ? policy.getAsJsonArray("conditions") : new JsonArray();
        for (JsonElement c : conds) {
            if (c.isJsonObject()) {
                for (Map.Entry<String, JsonElement> e : c.getAsJsonObject().entrySet()) {
                    String f = e.getKey().toLowerCase(Locale.ROOT);
                    covered.add(f);
                    if (!e.getValue().getAsString().equals(effective.get(f))) {
                        throw new S3WireException(403, "AccessDenied", "Invalid according to Policy: Policy Condition failed: [\"eq\", \"$"
                                + f + "\", \"" + e.getValue().getAsString() + "\"]");
                    }
                }
                continue;
            }
            JsonArray a = c.getAsJsonArray();
            String op = a.get(0).getAsString().toLowerCase(Locale.ROOT);
            if (op.equals("content-length-range")) {
                long min = a.get(1).getAsLong();
                long max = a.get(2).getAsLong();
                if (fileLength < min) {
                    throw new S3WireException(400, "EntityTooSmall", "Your proposed upload is smaller than the minimum allowed size",
                            null, Map.of("MinSizeAllowed", String.valueOf(min), "ProposedSize", String.valueOf(fileLength)));
                }
                if (fileLength > max) {
                    throw new S3WireException(400, "EntityTooLarge", "Your proposed upload exceeds the maximum allowed size",
                            null, Map.of("MaxSizeAllowed", String.valueOf(max), "ProposedSize", String.valueOf(fileLength)));
                }
                continue;
            }
            String field = a.get(1).getAsString();
            if (field.startsWith("$")) {
                field = field.substring(1);
            }
            field = field.toLowerCase(Locale.ROOT);
            covered.add(field);
            String actual = effective.getOrDefault(field, "");
            String want = a.get(2).getAsString();
            boolean ok = switch (op) {
                case "eq" -> actual.equals(want);
                case "starts-with" -> actual.startsWith(want);
                default -> throw new S3WireException(400, "InvalidPolicyDocument", "Invalid Policy: Unknown condition " + op);
            };
            if (!ok) {
                throw new S3WireException(403, "AccessDenied", "Invalid according to Policy: Policy Condition failed: [\"" + op
                        + "\", \"$" + field + "\", \"" + want + "\"]");
            }
        }
        for (String f : fields.keySet()) {
            if (!covered.contains(f) && !f.startsWith("x-ignore-")) {
                throw new S3WireException(403, "AccessDenied", "Invalid according to Policy: Extra input fields: " + f);
            }
        }
    }
}
