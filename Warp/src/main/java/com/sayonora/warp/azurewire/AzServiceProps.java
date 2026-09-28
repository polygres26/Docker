package com.sayonora.warp.azurewire;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/** XML request-body parsing and service-properties rendering shared by the three services. */
final class AzServiceProps {

    private AzServiceProps() {
    }

    private static final Gson GSON = new Gson();
    private static final TypeToken<Map<String, String>> MAP_T = new TypeToken<>() { };

    static Document parse(byte[] xml) {
        try {
            DocumentBuilderFactory f = DocumentBuilderFactory.newInstance();
            f.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
            f.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
            f.setExpandEntityReferences(false);
            DocumentBuilder b = f.newDocumentBuilder();
            return b.parse(new ByteArrayInputStream(xml));
        } catch (Exception e) {
            throw new AzureException(400, "InvalidXmlDocument", "XML specified is not syntactically valid.");
        }
    }

    static List<Element> children(Element e) {
        List<Element> out = new ArrayList<>();
        NodeList nl = e.getChildNodes();
        for (int i = 0; i < nl.getLength(); i++) {
            if (nl.item(i) instanceof Element c) {
                out.add(c);
            }
        }
        return out;
    }

    static String text(Element e) {
        return e.getTextContent();
    }

    private static void write(Element e, StringBuilder sb) {
        sb.append('<').append(e.getTagName()).append('>');
        NodeList nl = e.getChildNodes();
        for (int i = 0; i < nl.getLength(); i++) {
            Node n = nl.item(i);
            if (n instanceof Element c) {
                write(c, sb);
            } else if (n.getNodeType() == Node.TEXT_NODE) {
                sb.append(AzXml.esc(n.getNodeValue()));
            }
        }
        sb.append("</").append(e.getTagName()).append('>');
    }

    static String serialize(Element e) {
        StringBuilder sb = new StringBuilder();
        write(e, sb);
        return sb.toString();
    }

    // ---- service properties

    private static final String LOGGING = "<Logging><Version>1.0</Version><Delete>true</Delete><Read>true</Read><Write>true</Write>"
            + "<RetentionPolicy><Enabled>false</Enabled></RetentionPolicy></Logging>";
    private static final String HOUR = "<HourMetrics><Version>1.0</Version><Enabled>false</Enabled>"
            + "<RetentionPolicy><Enabled>false</Enabled></RetentionPolicy></HourMetrics>";
    private static final String MINUTE = "<MinuteMetrics><Version>1.0</Version><Enabled>false</Enabled>"
            + "<RetentionPolicy><Enabled>false</Enabled></RetentionPolicy></MinuteMetrics>";

    private static final List<String> BLOB_SECTIONS = List.of("Logging", "HourMetrics", "MinuteMetrics", "Cors",
            "DefaultServiceVersion", "StaticWebsite", "DeleteRetentionPolicy");
    private static final List<String> SIMPLE_SECTIONS = List.of("Logging", "HourMetrics", "MinuteMetrics", "Cors");

    static String render(String json, boolean blob) {
        Map<String, String> stored = GSON.fromJson(json, MAP_T);
        if (stored == null) {
            stored = Map.of();
        }
        StringBuilder sb = new StringBuilder("<?xml version=\"1.0\" encoding=\"utf-8\"?><StorageServiceProperties>");
        for (String sec : blob ? BLOB_SECTIONS : SIMPLE_SECTIONS) {
            String v = stored.get(sec);
            if (v != null) {
                sb.append(v);
                continue;
            }
            switch (sec) {
                case "Logging" -> sb.append(LOGGING);
                case "HourMetrics" -> sb.append(HOUR);
                case "MinuteMetrics" -> sb.append(MINUTE);
                case "Cors" -> sb.append("<Cors/>");
                case "DefaultServiceVersion" -> sb.append("<DefaultServiceVersion>" + BlobService.MAX_VERSION + "</DefaultServiceVersion>");
                case "StaticWebsite" -> sb.append("<StaticWebsite><Enabled>false</Enabled></StaticWebsite>");
                default -> {
                    // DeleteRetentionPolicy only appears once set
                }
            }
        }
        return sb.append("</StorageServiceProperties>").toString();
    }

    /** Applies a Set Service Properties body on top of the stored sections; returns the new JSON. */
    static String merge(String json, byte[] body, boolean blob) {
        Document d = parse(body);
        Element root = d.getDocumentElement();
        if (!"StorageServiceProperties".equals(root.getTagName())) {
            throw new AzureException(400, "InvalidXmlDocument", "XML specified is not syntactically valid.");
        }
        Map<String, String> stored = new LinkedHashMap<>();
        Map<String, String> old = GSON.fromJson(json, MAP_T);
        if (old != null) {
            stored.putAll(old);
        }
        List<String> allowed = blob ? BLOB_SECTIONS : SIMPLE_SECTIONS;
        for (Element c : children(root)) {
            if (!allowed.contains(c.getTagName())) {
                throw new AzureException(400, "InvalidXmlDocument", "XML specified is not syntactically valid.");
            }
            if ("Cors".equals(c.getTagName())) {
                if (children(c).size() > 5) {
                    throw new AzureException(400, "InvalidXmlDocument", "XML specified is not syntactically valid.");
                }
                for (Element rule : children(c)) {
                    for (String req : List.of("AllowedOrigins", "AllowedMethods", "MaxAgeInSeconds", "ExposedHeaders",
                            "AllowedHeaders")) {
                        if (children(rule).stream().noneMatch(x -> x.getTagName().equals(req))) {
                            throw new AzureException(400, "InvalidXmlDocument", "XML specified is not syntactically valid.");
                        }
                    }
                }
            }
            stored.put(c.getTagName(), children(c).isEmpty() && c.getTextContent().isEmpty() && "Cors".equals(c.getTagName())
                    ? "<Cors/>" : serialize(c));
        }
        return GSON.toJson(stored);
    }

    static String cors(String json) {
        Map<String, String> stored = GSON.fromJson(json, MAP_T);
        return stored == null ? null : stored.get("Cors");
    }

    // ---- other bodies

    static List<AzureAuth.Policy> parseSignedIdentifiers(byte[] body) {
        List<AzureAuth.Policy> out = new ArrayList<>();
        if (body.length == 0) {
            return out;
        }
        Document d = parse(body);
        Element root = d.getDocumentElement();
        if (!"SignedIdentifiers".equals(root.getTagName())) {
            throw new AzureException(400, "InvalidXmlDocument", "XML specified is not syntactically valid.");
        }
        for (Element si : children(root)) {
            String id = null;
            String start = null;
            String expiry = null;
            String perm = null;
            for (Element c : children(si)) {
                if ("Id".equals(c.getTagName())) {
                    id = text(c);
                } else if ("AccessPolicy".equals(c.getTagName())) {
                    for (Element a : children(c)) {
                        switch (a.getTagName()) {
                            case "Start" -> start = text(a);
                            case "Expiry" -> expiry = text(a);
                            case "Permission" -> perm = text(a);
                            default -> {
                            }
                        }
                    }
                }
            }
            if (id == null || id.isEmpty() || id.length() > 64) {
                throw new AzureException(400, "InvalidXmlDocument", "XML specified is not syntactically valid.");
            }
            for (AzureAuth.Policy p : out) {
                if (p.id().equals(id)) {
                    throw new AzureException(400, "InvalidXmlDocument", "XML specified is not syntactically valid.");
                }
            }
            out.add(new AzureAuth.Policy(id, start, expiry, perm));
        }
        if (out.size() > 5) {
            throw new AzureException(400, "InvalidXmlDocument", "XML specified is not syntactically valid.");
        }
        return out;
    }

    /** @return entries [blockId, Latest|Committed|Uncommitted] in list order */
    static List<String[]> parseBlockList(byte[] body) {
        Document d = parse(body);
        Element root = d.getDocumentElement();
        if (!"BlockList".equals(root.getTagName())) {
            throw new AzureException(400, "InvalidXmlDocument", "XML specified is not syntactically valid.");
        }
        List<String[]> out = new ArrayList<>();
        for (Element c : children(root)) {
            String t = c.getTagName();
            if (!t.equals("Latest") && !t.equals("Committed") && !t.equals("Uncommitted")) {
                throw new AzureException(400, "InvalidXmlDocument", "XML specified is not syntactically valid.");
            }
            out.add(new String[] {text(c), t});
        }
        return out;
    }

    static Map<String, String> parseTags(byte[] body) {
        Document d = parse(body);
        Element root = d.getDocumentElement();
        if (!"Tags".equals(root.getTagName())) {
            throw new AzureException(400, "InvalidXmlDocument", "XML specified is not syntactically valid.");
        }
        Map<String, String> out = new LinkedHashMap<>();
        for (Element ts : children(root)) {
            if (!"TagSet".equals(ts.getTagName())) {
                continue;
            }
            for (Element tag : children(ts)) {
                String k = null;
                String v = null;
                for (Element c : children(tag)) {
                    if ("Key".equals(c.getTagName())) {
                        k = text(c);
                    } else if ("Value".equals(c.getTagName())) {
                        v = text(c);
                    }
                }
                if (k == null || v == null) {
                    throw new AzureException(400, "InvalidXmlDocument", "XML specified is not syntactically valid.");
                }
                out.put(k, v);
            }
        }
        return out;
    }
}
