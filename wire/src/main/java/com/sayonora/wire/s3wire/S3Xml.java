package com.sayonora.wire.s3wire;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilderFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

/** S3 REST XML rendering (hand-rolled, like the responses real S3 sends) and request-body parsing. */
final class S3Xml {
    static final String NS = "http://s3.amazonaws.com/doc/2006-03-01/";
    private static final String DECL = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n";
    private static final DateTimeFormatter ISO =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", Locale.ROOT).withZone(ZoneOffset.UTC);
    private static final DateTimeFormatter HTTP_DATE =
            DateTimeFormatter.ofPattern("EEE, dd MMM yyyy HH:mm:ss 'GMT'", Locale.US).withZone(ZoneOffset.UTC);

    private S3Xml() {
    }

    record ObjectEntry(String key, Instant lastModified, String eTag, long size) {
    }

    static String iso(Instant t) {
        return ISO.format(t == null ? Instant.EPOCH : t);
    }

    static String httpDate(Instant t) {
        return HTTP_DATE.format(t);
    }

    static String escape(String s) {
        StringBuilder sb = new StringBuilder(s.length());
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '&' -> sb.append("&amp;");
                case '<' -> sb.append("&lt;");
                case '>' -> sb.append("&gt;");
                case '"' -> sb.append("&quot;");
                case '\'' -> sb.append("&apos;");
                default -> sb.append(c);
            }
        }
        return sb.toString();
    }

    private static void tag(StringBuilder sb, String name, String value) {
        sb.append('<').append(name).append('>').append(escape(value)).append("</").append(name).append('>');
    }

    static String error(String code, String message, String resource, String requestId, String extraName, String extraValue) {
        StringBuilder sb = new StringBuilder(DECL + "<Error>");
        tag(sb, "Code", code);
        tag(sb, "Message", message == null ? code : message);
        if (extraName != null) {
            tag(sb, extraName, extraValue);
        }
        tag(sb, "Resource", resource == null ? "" : resource);
        tag(sb, "RequestId", requestId);
        return sb.append("</Error>").toString();
    }

    /** @param namesAndDates each element is {bucketName, iso8601CreationDate} */
    static String listBuckets(List<String[]> namesAndDates, String ownerId) {
        StringBuilder sb = new StringBuilder(DECL + "<ListAllMyBucketsResult xmlns=\"" + NS + "\">");
        sb.append("<Owner>");
        tag(sb, "ID", ownerId);
        tag(sb, "DisplayName", ownerId);
        sb.append("</Owner><Buckets>");
        for (String[] b : namesAndDates) {
            sb.append("<Bucket>");
            tag(sb, "Name", b[0]);
            tag(sb, "CreationDate", b[1]);
            sb.append("</Bucket>");
        }
        return sb.append("</Buckets></ListAllMyBucketsResult>").toString();
    }

    record ListParams(String bucket, String prefix, String delimiter, int maxKeys, boolean truncated, boolean v2,
            String continuationToken, String nextContinuationToken, String startAfter, String marker,
            String nextMarker, boolean urlEncode) {
    }

    static String listObjects(ListParams p, List<ObjectEntry> objects, List<String> commonPrefixes) {
        StringBuilder sb = new StringBuilder(DECL + "<ListBucketResult xmlns=\"" + NS + "\">");
        tag(sb, "Name", p.bucket());
        tag(sb, "Prefix", enc(p, p.prefix()));
        if (p.v2()) {
            tag(sb, "KeyCount", String.valueOf(objects.size() + commonPrefixes.size()));
        }
        tag(sb, "MaxKeys", String.valueOf(p.maxKeys()));
        if (p.delimiter() != null && !p.delimiter().isEmpty()) {
            tag(sb, "Delimiter", enc(p, p.delimiter()));
        }
        if (p.urlEncode()) {
            tag(sb, "EncodingType", "url");
        }
        tag(sb, "IsTruncated", String.valueOf(p.truncated()));
        if (p.v2()) {
            if (p.continuationToken() != null) {
                tag(sb, "ContinuationToken", p.continuationToken());
            }
            if (p.nextContinuationToken() != null) {
                tag(sb, "NextContinuationToken", p.nextContinuationToken());
            }
            if (p.startAfter() != null) {
                tag(sb, "StartAfter", enc(p, p.startAfter()));
            }
        } else {
            tag(sb, "Marker", enc(p, p.marker() == null ? "" : p.marker()));
            if (p.nextMarker() != null) {
                tag(sb, "NextMarker", enc(p, p.nextMarker()));
            }
        }
        for (ObjectEntry o : objects) {
            sb.append("<Contents>");
            tag(sb, "Key", enc(p, o.key()));
            tag(sb, "LastModified", iso(o.lastModified()));
            tag(sb, "ETag", o.eTag() == null ? "" : o.eTag());
            tag(sb, "Size", String.valueOf(o.size()));
            tag(sb, "StorageClass", "STANDARD");
            sb.append("</Contents>");
        }
        for (String cp : commonPrefixes) {
            sb.append("<CommonPrefixes>");
            tag(sb, "Prefix", enc(p, cp));
            sb.append("</CommonPrefixes>");
        }
        return sb.append("</ListBucketResult>").toString();
    }

    private static String enc(ListParams p, String s) {
        return p.urlEncode() ? S3SigV4Verifier.encode(s, false) : s;
    }

    static String initiateMultipart(String bucket, String key, String uploadId) {
        StringBuilder sb = new StringBuilder(DECL + "<InitiateMultipartUploadResult xmlns=\"" + NS + "\">");
        tag(sb, "Bucket", bucket);
        tag(sb, "Key", key);
        tag(sb, "UploadId", uploadId);
        return sb.append("</InitiateMultipartUploadResult>").toString();
    }

    static String completeMultipart(String location, String bucket, String key, String eTag) {
        StringBuilder sb = new StringBuilder(DECL + "<CompleteMultipartUploadResult xmlns=\"" + NS + "\">");
        tag(sb, "Location", location);
        tag(sb, "Bucket", bucket);
        tag(sb, "Key", key);
        tag(sb, "ETag", eTag == null ? "" : eTag);
        return sb.append("</CompleteMultipartUploadResult>").toString();
    }

    static String copyResult(String eTag, Instant lastModified) {
        StringBuilder sb = new StringBuilder(DECL + "<CopyObjectResult xmlns=\"" + NS + "\">");
        tag(sb, "LastModified", iso(lastModified));
        tag(sb, "ETag", eTag == null ? "" : eTag);
        return sb.append("</CopyObjectResult>").toString();
    }

    record DeleteOutcome(String key, String code, String message) {
    }

    static String deleteResult(List<String> deleted, List<DeleteOutcome> errors, boolean quiet) {
        StringBuilder sb = new StringBuilder(DECL + "<DeleteResult xmlns=\"" + NS + "\">");
        if (!quiet) {
            for (String k : deleted) {
                sb.append("<Deleted>");
                tag(sb, "Key", k);
                sb.append("</Deleted>");
            }
        }
        for (DeleteOutcome e : errors) {
            sb.append("<Error>");
            tag(sb, "Key", e.key());
            tag(sb, "Code", e.code());
            tag(sb, "Message", e.message());
            sb.append("</Error>");
        }
        return sb.append("</DeleteResult>").toString();
    }

    static String location(String region) {
        return DECL + "<LocationConstraint xmlns=\"" + NS + "\">"
                + ("us-east-1".equals(region) ? "" : escape(region)) + "</LocationConstraint>";
    }

    // ---- request parsing ----------------------------------------------------------------------

    record Delete(List<String> keys, boolean quiet) {
    }

    static Delete parseDelete(byte[] body) {
        Element root = parse(body);
        if (!"Delete".equals(name(root))) {
            throw new S3WireException(400, "MalformedXML",
                    "The XML you provided was not well-formed or did not validate against our published schema.");
        }
        List<String> keys = new ArrayList<>();
        for (Element o : children(root, "Object")) {
            String k = text(o, "Key");
            if (k == null) {
                throw new S3WireException(400, "MalformedXML", "Object without Key.");
            }
            keys.add(k);
        }
        return new Delete(keys, "true".equalsIgnoreCase(text(root, "Quiet")));
    }

    record Part(int number, String eTag) {
    }

    static List<Part> parseCompleteMultipart(byte[] body) {
        Element root = parse(body);
        List<Part> parts = new ArrayList<>();
        for (Element p : children(root, "Part")) {
            String n = text(p, "PartNumber");
            String e = text(p, "ETag");
            if (n == null || e == null) {
                throw new S3WireException(400, "MalformedXML", "Part requires PartNumber and ETag.");
            }
            try {
                parts.add(new Part(Integer.parseInt(n.trim()), e.trim()));
            } catch (NumberFormatException ex) {
                throw new S3WireException(400, "MalformedXML", "Bad PartNumber.");
            }
        }
        if (parts.isEmpty()) {
            throw new S3WireException(400, "MalformedXML", "CompleteMultipartUpload requires at least one Part.");
        }
        return parts;
    }

    private static Element parse(byte[] body) {
        try {
            DocumentBuilderFactory f = DocumentBuilderFactory.newInstance();
            f.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
            f.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
            f.setXIncludeAware(false);
            f.setExpandEntityReferences(false);
            Document d = f.newDocumentBuilder().parse(new ByteArrayInputStream(body));
            return d.getDocumentElement();
        } catch (Exception e) {
            throw new S3WireException(400, "MalformedXML",
                    "The XML you provided was not well-formed or did not validate against our published schema.");
        }
    }

    private static String name(Element e) {
        return e.getLocalName() == null ? e.getNodeName() : e.getLocalName();
    }

    private static List<Element> children(Element parent, String name) {
        List<Element> out = new ArrayList<>();
        NodeList nl = parent.getChildNodes();
        for (int i = 0; i < nl.getLength(); i++) {
            if (nl.item(i) instanceof Element e && name.equals(name(e))) {
                out.add(e);
            }
        }
        return out;
    }

    private static String text(Element parent, String name) {
        List<Element> c = children(parent, name);
        return c.isEmpty() ? null : c.get(0).getTextContent();
    }

    static byte[] utf8(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }
}
