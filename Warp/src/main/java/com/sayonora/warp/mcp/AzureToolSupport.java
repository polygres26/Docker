package com.sayonora.warp.mcp;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.sayonora.warp.azurewire.AzureEmbedded;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;
import javax.xml.parsers.DocumentBuilderFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/** Shared helpers of the three Azure Storage tool providers: account resolution, XML answers as JSON, error mapping. */
final class AzureToolSupport {

    private AzureToolSupport() {
    }

    /** The storage account a call addresses: the {@code account} argument, else the only configured account. */
    static String account(JsonObject a) {
        Set<String> configured = AzureEmbedded.configuredAccounts();
        String asked = ToolSchemas.optString(a, "account");
        if (configured.isEmpty()) {
            throw new IllegalArgumentException("no storage account is configured on this Warp (WARP_AZURE_ACCOUNTS or "
                    + "WARP_AZURE_DEV_ACCOUNT=true), so there is no account to address");
        }
        if (asked == null) {
            if (configured.size() == 1) {
                return configured.iterator().next();
            }
            throw new IllegalArgumentException("argument \"account\" is required: configured accounts are " + configured);
        }
        String acct = asked.toLowerCase(java.util.Locale.ROOT);
        if (!configured.contains(acct)) {
            throw new IllegalArgumentException("storage account \"" + asked + "\" is not configured (configured: " + configured + ")");
        }
        return acct;
    }

    static StoreToolProvider.Outcome failure(AzureEmbedded.Response r) {
        String code = r.headers().get("x-ms-error-code");
        String text = r.text();
        String message = text;
        try {
            if (text.trim().startsWith("<")) {
                Element root = parse(text).getDocumentElement();
                message = childText(root, "Message");
            } else if (text.trim().startsWith("{")) {
                var j = com.google.gson.JsonParser.parseString(text).getAsJsonObject();
                if (j.has("odata.error")) {
                    var m = j.getAsJsonObject("odata.error").getAsJsonObject("message");
                    message = m.get("value").getAsString();
                }
            }
        } catch (Exception ignored) {
            // keep the raw text
        }
        if (message != null && message.length() > 600) {
            message = message.substring(0, 600);
        }
        return StoreToolProvider.Outcome.error((code == null ? "HTTP " + r.status() : code) + " (HTTP " + r.status() + "): "
                + (message == null ? "" : message.replace("\n", " ").trim()));
    }

    static Document parse(String xml) throws Exception {
        DocumentBuilderFactory f = DocumentBuilderFactory.newInstance();
        f.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
        f.setExpandEntityReferences(false);
        return f.newDocumentBuilder().parse(new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)));
    }

    static String childText(Element e, String name) {
        NodeList l = e.getElementsByTagName(name);
        return l.getLength() == 0 ? null : l.item(0).getTextContent();
    }

    /** An element as JSON: leaf children become strings, nested elements objects (repeated names collapse to the last). */
    static JsonObject flat(Element e) {
        JsonObject o = new JsonObject();
        for (Node n = e.getFirstChild(); n != null; n = n.getNextSibling()) {
            if (n instanceof Element c) {
                boolean nested = false;
                for (Node k = c.getFirstChild(); k != null; k = k.getNextSibling()) {
                    if (k instanceof Element) {
                        nested = true;
                        break;
                    }
                }
                if (nested) {
                    o.add(c.getTagName(), flat(c));
                } else {
                    o.addProperty(c.getTagName(), c.getTextContent());
                }
            }
        }
        return o;
    }

    /** All descendants named {@code name}, each as {@link #flat}. */
    static JsonArray all(Document d, String name, int max) {
        JsonArray out = new JsonArray();
        NodeList l = d.getElementsByTagName(name);
        for (int i = 0; i < l.getLength() && out.size() < max; i++) {
            out.add(flat((Element) l.item(i)));
        }
        return out;
    }

    static String queryOf(List<String[]> pairs) {
        StringBuilder sb = new StringBuilder();
        for (String[] p : pairs) {
            if (p[1] == null) {
                continue;
            }
            if (sb.length() > 0) {
                sb.append('&');
            }
            sb.append(StoreToolProvider.enc(p[0])).append('=').append(StoreToolProvider.enc(p[1]));
        }
        return sb.toString();
    }

    static String[] q(String k, String v) {
        return new String[] {k, v};
    }

    static String xmlEscape(String s) {
        return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace("\"", "&quot;").replace("'", "&apos;");
    }
}
