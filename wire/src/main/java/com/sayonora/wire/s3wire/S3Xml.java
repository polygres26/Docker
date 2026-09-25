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
    static final String DECL = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n";
    private static final DateTimeFormatter ISO =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", Locale.ROOT).withZone(ZoneOffset.UTC);
    private static final DateTimeFormatter HTTP_DATE =
            DateTimeFormatter.ofPattern("EEE, dd MMM yyyy HH:mm:ss 'GMT'", Locale.US).withZone(ZoneOffset.UTC);

    private S3Xml() {
    }

    record ObjectEntry(String key, Instant lastModified, String eTag, long size, String storageClass,
            List<String> checksumAlgos, String checksumType) {
        ObjectEntry(String key, Instant lastModified, String eTag, long size) {
            this(key, lastModified, eTag, size, null, List.of(), null);
        }
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

    static void tag(StringBuilder sb, String name, String value) {
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

    static Element parse(byte[] body) {
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

    static String name(Element e) {
        return e.getLocalName() == null ? e.getNodeName() : e.getLocalName();
    }

    static List<Element> children(Element parent, String name) {
        List<Element> out = new ArrayList<>();
        NodeList nl = parent.getChildNodes();
        for (int i = 0; i < nl.getLength(); i++) {
            if (nl.item(i) instanceof Element e && name.equals(name(e))) {
                out.add(e);
            }
        }
        return out;
    }

    static String text(Element parent, String name) {
        List<Element> c = children(parent, name);
        return c.isEmpty() ? null : c.get(0).getTextContent();
    }

    static byte[] utf8(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    // ====================================================================================================
    // Rich (Postgres mode) renderers / parsers
    // ====================================================================================================

    private static void ownerXml(StringBuilder sb, String tag) {
        sb.append('<').append(tag).append('>');
        tag(sb, "ID", S3Cfg.OWNER_ID);
        tag(sb, "DisplayName", S3Cfg.OWNER_NAME);
        sb.append("</").append(tag).append('>');
    }

    private static void checksumTags(StringBuilder sb, java.util.Map<String, String> checksums, String type, boolean withType) {
        for (String a : Checksums.ALL) {
            String v = checksums == null ? null : checksums.get(a);
            if (v != null) {
                tag(sb, Checksums.xmlName(a), v);
            }
        }
        if (withType && type != null && checksums != null && !checksums.isEmpty()) {
            tag(sb, "ChecksumType", type);
        }
    }

    static String listBucketsRich(List<S3Model.BucketInfo> buckets, String prefix, String continuationToken,
            boolean truncated, String nextToken) {
        StringBuilder sb = new StringBuilder(DECL + "<ListAllMyBucketsResult xmlns=\"" + NS + "\">");
        ownerXml(sb, "Owner");
        sb.append("<Buckets>");
        for (S3Model.BucketInfo b : buckets) {
            sb.append("<Bucket>");
            tag(sb, "Name", b.name());
            tag(sb, "CreationDate", iso(b.created()));
            if (b.region() != null) {
                tag(sb, "BucketRegion", b.region());
            }
            sb.append("</Bucket>");
        }
        sb.append("</Buckets>");
        if (truncated && nextToken != null) {
            tag(sb, "ContinuationToken", nextToken);
        }
        if (prefix != null && !prefix.isEmpty()) {
            tag(sb, "Prefix", prefix);
        }
        return sb.append("</ListAllMyBucketsResult>").toString();
    }

    /** ListObjects v1/v2 with owner, storage class, checksum algorithms. */
    static String listObjectsRich(ListParams p, List<ObjectEntry> objects, List<String> commonPrefixes, boolean owner) {
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
            if (o.checksumAlgos() != null) {
                for (String a : o.checksumAlgos()) {
                    tag(sb, "ChecksumAlgorithm", a);
                }
                if (!o.checksumAlgos().isEmpty() && o.checksumType() != null) {
                    tag(sb, "ChecksumType", o.checksumType());
                }
            }
            tag(sb, "Size", String.valueOf(o.size()));
            tag(sb, "StorageClass", o.storageClass() == null ? "STANDARD" : o.storageClass());
            if (owner) {
                ownerXml(sb, "Owner");
            }
            sb.append("</Contents>");
        }
        for (String cp : commonPrefixes) {
            sb.append("<CommonPrefixes>");
            tag(sb, "Prefix", enc(p, cp));
            sb.append("</CommonPrefixes>");
        }
        return sb.append("</ListBucketResult>").toString();
    }

    static String listVersions(String bucket, String prefix, String delimiter, String keyMarker, String versionMarker,
            int maxKeys, boolean urlEncode, S3Model.VersionsListing l) {
        java.util.function.Function<String, String> e = urlEncode ? x -> S3SigV4Verifier.encode(x, false) : x -> x;
        StringBuilder sb = new StringBuilder(DECL + "<ListVersionsResult xmlns=\"" + NS + "\">");
        tag(sb, "Name", bucket);
        tag(sb, "Prefix", e.apply(prefix == null ? "" : prefix));
        tag(sb, "KeyMarker", e.apply(keyMarker == null ? "" : keyMarker));
        tag(sb, "VersionIdMarker", versionMarker == null ? "" : versionMarker);
        if (l.truncated()) {
            tag(sb, "NextKeyMarker", e.apply(l.nextKeyMarker() == null ? "" : l.nextKeyMarker()));
            if (l.nextVersionMarker() != null) {
                tag(sb, "NextVersionIdMarker", l.nextVersionMarker());
            }
        }
        tag(sb, "MaxKeys", String.valueOf(maxKeys));
        if (delimiter != null && !delimiter.isEmpty()) {
            tag(sb, "Delimiter", e.apply(delimiter));
        }
        if (urlEncode) {
            tag(sb, "EncodingType", "url");
        }
        tag(sb, "IsTruncated", String.valueOf(l.truncated()));
        for (S3Model.VersionEntry v : l.entries()) {
            String el = v.deleteMarker() ? "DeleteMarker" : "Version";
            sb.append('<').append(el).append('>');
            tag(sb, "Key", e.apply(v.key()));
            tag(sb, "VersionId", v.versionId());
            tag(sb, "IsLatest", String.valueOf(v.latest()));
            tag(sb, "LastModified", iso(v.lastModified()));
            if (!v.deleteMarker()) {
                tag(sb, "ETag", v.eTag());
                if (v.checksumAlgos() != null) {
                    for (String a : v.checksumAlgos()) {
                        tag(sb, "ChecksumAlgorithm", a);
                    }
                    if (!v.checksumAlgos().isEmpty() && v.checksumType() != null) {
                        tag(sb, "ChecksumType", v.checksumType());
                    }
                }
                tag(sb, "Size", String.valueOf(v.size()));
                tag(sb, "StorageClass", v.storageClass() == null ? "STANDARD" : v.storageClass());
            }
            ownerXml(sb, "Owner");
            sb.append("</").append(el).append('>');
        }
        for (String cp : l.commonPrefixes()) {
            sb.append("<CommonPrefixes>");
            tag(sb, "Prefix", e.apply(cp));
            sb.append("</CommonPrefixes>");
        }
        return sb.append("</ListVersionsResult>").toString();
    }

    static String initiateMultipartRich(String bucket, String key, String uploadId) {
        return initiateMultipart(bucket, key, uploadId);
    }

    static String completeMultipartRich(String location, String bucket, String key, S3Model.CompleteOut o) {
        StringBuilder sb = new StringBuilder(DECL + "<CompleteMultipartUploadResult xmlns=\"" + NS + "\">");
        tag(sb, "Location", location);
        tag(sb, "Bucket", bucket);
        tag(sb, "Key", key);
        tag(sb, "ETag", o.eTag());
        checksumTags(sb, o.checksums(), o.checksumType(), true);
        return sb.append("</CompleteMultipartUploadResult>").toString();
    }

    static String copyResultRich(S3Model.Meta m) {
        StringBuilder sb = new StringBuilder(DECL + "<CopyObjectResult xmlns=\"" + NS + "\">");
        tag(sb, "ETag", m.eTag());
        tag(sb, "LastModified", iso(m.lastModified()));
        checksumTags(sb, m.checksums(), m.checksumType(), true);
        return sb.append("</CopyObjectResult>").toString();
    }

    static String copyPartResult(S3Model.PartCopyOut o) {
        StringBuilder sb = new StringBuilder(DECL + "<CopyPartResult xmlns=\"" + NS + "\">");
        tag(sb, "ETag", o.eTag());
        tag(sb, "LastModified", iso(o.lastModified()));
        checksumTags(sb, o.checksums(), null, false);
        return sb.append("</CopyPartResult>").toString();
    }

    static String listParts(String bucket, String key, int marker, int maxParts, S3Model.PartListing l) {
        StringBuilder sb = new StringBuilder(DECL + "<ListPartsResult xmlns=\"" + NS + "\">");
        tag(sb, "Bucket", bucket);
        tag(sb, "Key", key);
        tag(sb, "UploadId", l.upload().uploadId());
        tag(sb, "PartNumberMarker", String.valueOf(marker));
        if (l.truncated()) {
            tag(sb, "NextPartNumberMarker", String.valueOf(l.nextMarker()));
        }
        tag(sb, "MaxParts", String.valueOf(maxParts));
        tag(sb, "IsTruncated", String.valueOf(l.truncated()));
        for (S3Model.PartRow p : l.parts()) {
            sb.append("<Part>");
            tag(sb, "PartNumber", String.valueOf(p.number()));
            tag(sb, "LastModified", iso(p.modified()));
            tag(sb, "ETag", p.eTag());
            tag(sb, "Size", String.valueOf(p.size()));
            checksumTags(sb, p.checksums(), null, false);
            sb.append("</Part>");
        }
        ownerXml(sb, "Initiator");
        ownerXml(sb, "Owner");
        tag(sb, "StorageClass", l.upload().extra().has("sc") ? l.upload().extra().get("sc").getAsString() : "STANDARD");
        if (l.upload().checksumAlgo() != null) {
            tag(sb, "ChecksumAlgorithm", l.upload().checksumAlgo());
        }
        if (l.upload().checksumType() != null) {
            tag(sb, "ChecksumType", l.upload().checksumType());
        }
        return sb.append("</ListPartsResult>").toString();
    }

    static String listUploads(String bucket, String prefix, String delimiter, String keyMarker, String uploadMarker,
            int max, boolean urlEncode, S3Model.UploadsListing l) {
        java.util.function.Function<String, String> e = urlEncode ? x -> S3SigV4Verifier.encode(x, false) : x -> x;
        StringBuilder sb = new StringBuilder(DECL + "<ListMultipartUploadsResult xmlns=\"" + NS + "\">");
        tag(sb, "Bucket", bucket);
        tag(sb, "KeyMarker", e.apply(keyMarker == null ? "" : keyMarker));
        tag(sb, "UploadIdMarker", uploadMarker == null ? "" : uploadMarker);
        if (l.truncated()) {
            tag(sb, "NextKeyMarker", e.apply(l.nextKeyMarker() == null ? "" : l.nextKeyMarker()));
            tag(sb, "NextUploadIdMarker", l.nextUploadIdMarker() == null ? "" : l.nextUploadIdMarker());
        }
        if (delimiter != null && !delimiter.isEmpty()) {
            tag(sb, "Delimiter", e.apply(delimiter));
        }
        tag(sb, "Prefix", e.apply(prefix == null ? "" : prefix));
        tag(sb, "MaxUploads", String.valueOf(max));
        tag(sb, "IsTruncated", String.valueOf(l.truncated()));
        if (urlEncode) {
            tag(sb, "EncodingType", "url");
        }
        for (S3Model.UploadInfo u : l.uploads()) {
            sb.append("<Upload>");
            tag(sb, "Key", e.apply(u.key()));
            tag(sb, "UploadId", u.uploadId());
            ownerXml(sb, "Initiator");
            ownerXml(sb, "Owner");
            tag(sb, "StorageClass", u.extra().has("sc") ? u.extra().get("sc").getAsString() : "STANDARD");
            tag(sb, "Initiated", iso(u.initiated()));
            if (u.checksumAlgo() != null) {
                tag(sb, "ChecksumAlgorithm", u.checksumAlgo());
            }
            if (u.checksumType() != null) {
                tag(sb, "ChecksumType", u.checksumType());
            }
            sb.append("</Upload>");
        }
        for (String cp : l.commonPrefixes()) {
            sb.append("<CommonPrefixes>");
            tag(sb, "Prefix", e.apply(cp));
            sb.append("</CommonPrefixes>");
        }
        return sb.append("</ListMultipartUploadsResult>").toString();
    }

    static String objectAttributes(S3Model.Meta m, java.util.Set<String> want, int maxParts, int partMarker) {
        StringBuilder sb = new StringBuilder(DECL + "<GetObjectAttributesResponse xmlns=\"" + NS + "\">");
        if (want.contains("ETag")) {
            tag(sb, "ETag", m.eTag().replace("\"", ""));
        }
        if (want.contains("Checksum") && !m.checksums().isEmpty()) {
            sb.append("<Checksum>");
            for (String a : Checksums.ALL) {
                String v = m.checksums().get(a);
                if (v != null) {
                    int dash = v.lastIndexOf('-');
                    tag(sb, Checksums.xmlName(a), dash > 0 ? v.substring(0, dash) : v);
                }
            }
            if (m.checksumType() != null) {
                tag(sb, "ChecksumType", m.checksumType());
            }
            sb.append("</Checksum>");
        }
        if (want.contains("ObjectParts") && m.multipart()) {
            sb.append("<ObjectParts>");
            tag(sb, "PartsCount", String.valueOf(m.parts().size()));
            tag(sb, "PartNumberMarker", String.valueOf(partMarker));
            List<S3Model.PartMeta> shown = new ArrayList<>();
            for (S3Model.PartMeta p : m.parts()) {
                if (p.number() > partMarker && shown.size() < maxParts) {
                    shown.add(p);
                }
            }
            boolean truncated = m.parts().stream().filter(p -> p.number() > partMarker).count() > shown.size();
            if (truncated) {
                tag(sb, "NextPartNumberMarker", String.valueOf(shown.get(shown.size() - 1).number()));
            }
            tag(sb, "MaxParts", String.valueOf(maxParts));
            tag(sb, "IsTruncated", String.valueOf(truncated));
            if ("COMPOSITE".equals(m.checksumType())) {
                for (S3Model.PartMeta p : shown) {
                    sb.append("<Part>");
                    tag(sb, "PartNumber", String.valueOf(p.number()));
                    tag(sb, "Size", String.valueOf(p.size()));
                    for (String a : Checksums.ALL) {
                        String v = p.checksums().get(a);
                        if (v != null && m.checksums().containsKey(a)) {
                            tag(sb, Checksums.xmlName(a), v);
                        }
                    }
                    sb.append("</Part>");
                }
            }
            sb.append("</ObjectParts>");
        }
        if (want.contains("StorageClass")) {
            tag(sb, "StorageClass", m.str("sc") == null ? "STANDARD" : m.str("sc"));
        }
        if (want.contains("ObjectSize")) {
            tag(sb, "ObjectSize", String.valueOf(m.size()));
        }
        return sb.append("</GetObjectAttributesResponse>").toString();
    }

    static String deleteResultRich(S3Model.DeleteManyOut out, boolean quiet) {
        StringBuilder sb = new StringBuilder(DECL + "<DeleteResult xmlns=\"" + NS + "\">");
        for (S3Model.DeleteManyOut.Item i : out.results()) {
            if (i.errCode() != null) {
                sb.append("<Error>");
                tag(sb, "Key", i.key());
                if (i.requestedVersion() != null) {
                    tag(sb, "VersionId", i.requestedVersion());
                }
                tag(sb, "Code", i.errCode());
                tag(sb, "Message", i.errMessage());
                sb.append("</Error>");
            } else if (!quiet) {
                sb.append("<Deleted>");
                tag(sb, "Key", i.key());
                if (i.requestedVersion() != null) {
                    tag(sb, "VersionId", i.requestedVersion());
                }
                if (i.deleteMarker()) {
                    tag(sb, "DeleteMarker", "true");
                    if (i.versionId() != null) {
                        tag(sb, "DeleteMarkerVersionId", i.versionId());
                    }
                }
                sb.append("</Deleted>");
            }
        }
        return sb.append("</DeleteResult>").toString();
    }

    static String locationRich(String region) {
        return DECL + "<LocationConstraint xmlns=\"" + NS + "\">"
                + (region == null || "us-east-1".equals(region) ? "" : escape(region)) + "</LocationConstraint>";
    }

    static String listAnnotations(String bucket, String key, String prefix, int max, String token,
            List<S3Model.Annotation> l, boolean truncated, String next) {
        StringBuilder sb = new StringBuilder(DECL + "<ListObjectAnnotationsOutput xmlns=\"" + NS + "\"><Annotations>");
        for (S3Model.Annotation a : l) {
            sb.append("<AnnotationEntry>");
            tag(sb, "AnnotationName", a.name());
            for (String algo : a.checksums().keySet()) {
                tag(sb, "ChecksumAlgorithm", algo);
            }
            tag(sb, "ETag", a.eTag());
            tag(sb, "LastModified", iso(a.modified()));
            tag(sb, "Size", String.valueOf(a.size()));
            sb.append("</AnnotationEntry>");
        }
        sb.append("</Annotations>");
        tag(sb, "Bucket", bucket);
        tag(sb, "Key", key);
        if (prefix != null) {
            tag(sb, "AnnotationPrefix", prefix);
        }
        tag(sb, "MaxAnnotationResults", String.valueOf(max));
        tag(sb, "AnnotationCount", String.valueOf(l.size()));
        tag(sb, "IsTruncated", String.valueOf(truncated));
        if (token != null) {
            tag(sb, "ContinuationToken", token);
        }
        if (truncated && next != null) {
            tag(sb, "NextContinuationToken", next);
        }
        return sb.append("</ListObjectAnnotationsOutput>").toString();
    }

    static String putAnnotationOutput(String key, String name) {
        StringBuilder sb = new StringBuilder(DECL + "<PutObjectAnnotationOutput xmlns=\"" + NS + "\">");
        tag(sb, "Key", key);
        tag(sb, "AnnotationName", name);
        return sb.append("</PutObjectAnnotationOutput>").toString();
    }

    /** Rich error including extra elements (Key, BucketName, ...). */
    static String errorRich(String code, String message, String resource, String requestId, String hostId,
            java.util.Map<String, String> details) {
        StringBuilder sb = new StringBuilder(DECL + "<Error>");
        tag(sb, "Code", code);
        tag(sb, "Message", message == null ? code : message);
        if (details != null) {
            for (java.util.Map.Entry<String, String> d : details.entrySet()) {
                tag(sb, d.getKey(), d.getValue());
            }
        }
        tag(sb, "Resource", resource == null ? "" : resource);
        tag(sb, "RequestId", requestId);
        tag(sb, "HostId", hostId == null ? "" : hostId);
        return sb.append("</Error>").toString();
    }

    // ---- request bodies -----------------------------------------------------------------------------

    record DeleteIds(List<S3Model.ObjId> ids, boolean quiet) {
    }

    static DeleteIds parseDeleteIds(byte[] body) {
        Element root = parse(body);
        if (!"Delete".equals(name(root))) {
            throw new S3WireException(400, "MalformedXML",
                    "The XML you provided was not well-formed or did not validate against our published schema.");
        }
        List<S3Model.ObjId> ids = new ArrayList<>();
        for (Element o : children(root, "Object")) {
            String k = text(o, "Key");
            if (k == null) {
                throw new S3WireException(400, "MalformedXML", "Object without Key.");
            }
            ids.add(new S3Model.ObjId(k, text(o, "VersionId")));
        }
        return new DeleteIds(ids, "true".equalsIgnoreCase(text(root, "Quiet")));
    }

    static List<S3Model.CompletePart> parseCompleteParts(byte[] body) {
        Element root = parse(body);
        if (!"CompleteMultipartUpload".equals(name(root))) {
            throw S3Cfg.malformed();
        }
        List<S3Model.CompletePart> parts = new ArrayList<>();
        for (Element p : children(root, "Part")) {
            String n = text(p, "PartNumber");
            String e = text(p, "ETag");
            if (n == null) {
                throw new S3WireException(400, "MalformedXML", "Part requires PartNumber.");
            }
            java.util.Map<String, String> ck = new java.util.LinkedHashMap<>();
            for (String a : Checksums.ALL) {
                String v = text(p, Checksums.xmlName(a));
                if (v != null && !v.isBlank()) {
                    ck.put(a, v.trim());
                }
            }
            try {
                int num = Integer.parseInt(n.trim());
                if (num < 1 || num > 10000) {
                    throw new S3WireException(400, "InvalidArgument", "Part number must be an integer between 1 and 10000, inclusive");
                }
                parts.add(new S3Model.CompletePart(num, e == null ? null : e.trim(), ck));
            } catch (NumberFormatException ex) {
                throw new S3WireException(400, "MalformedXML", "Bad PartNumber.");
            }
        }
        if (parts.isEmpty()) {
            throw new S3WireException(400, "MalformedXML", "The XML you provided was not well-formed or did not validate "
                    + "against our published schema");
        }
        return parts;
    }

    /** {@code <CreateBucketConfiguration><LocationConstraint>} or null. */
    static String parseLocationConstraint(byte[] body) {
        if (body == null || body.length == 0) {
            return null;
        }
        Element root = parse(body);
        if (!"CreateBucketConfiguration".equals(name(root))) {
            throw S3Cfg.malformed();
        }
        String lc = text(root, "LocationConstraint");
        return lc == null ? null : lc.trim();
    }
}
