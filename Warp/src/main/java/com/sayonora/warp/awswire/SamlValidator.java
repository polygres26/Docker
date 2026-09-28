package com.sayonora.warp.awswire;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.security.PublicKey;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import javax.xml.XMLConstants;
import javax.xml.crypto.dsig.Reference;
import javax.xml.crypto.dsig.Transform;
import javax.xml.crypto.dsig.XMLSignature;
import javax.xml.crypto.dsig.XMLSignatureFactory;
import javax.xml.crypto.dsig.dom.DOMValidateContext;
import javax.xml.crypto.KeySelector;
import javax.xml.parsers.DocumentBuilderFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

/**
 * Validates the SAML assertion of {@code AssumeRoleWithSAML} against the IAM SAML provider's metadata: the assertion must be
 * signed (XML-DSig, enveloped, over the assertion itself, only the enveloped-signature and canonicalization transforms) by the
 * certificate in the provider metadata, name that provider's entity as issuer, be within its validity window, be addressed to
 * {@code urn:amazon:webservices} and carry the requested role/provider pair in the Role attribute. DOCTYPEs are refused.
 */
final class SamlValidator {

    static final class Invalid extends Exception {
        final String code;

        Invalid(String code, String message) {
            super(message, null, false, false);
            this.code = code;
        }
    }

    record Assertion(String subject, String sessionName) {
    }

    private SamlValidator() {
    }

    private static Document parse(byte[] xml) throws Exception {
        DocumentBuilderFactory f = DocumentBuilderFactory.newInstance();
        f.setNamespaceAware(true);
        f.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
        f.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
        f.setXIncludeAware(false);
        f.setExpandEntityReferences(false);
        return f.newDocumentBuilder().parse(new ByteArrayInputStream(xml));
    }

    static PublicKey metadataKey(String metadata) throws Exception {
        Document d = parse(metadata.getBytes(StandardCharsets.UTF_8));
        NodeList certs = d.getElementsByTagNameNS("http://www.w3.org/2000/09/xmldsig#", "X509Certificate");
        if (certs.getLength() == 0) {
            throw new Invalid("InvalidIdentityToken", "The SAML provider metadata has no signing certificate");
        }
        byte[] der = Base64.getMimeDecoder().decode(certs.item(0).getTextContent().trim());
        X509Certificate c = (X509Certificate) CertificateFactory.getInstance("X.509").generateCertificate(new ByteArrayInputStream(der));
        return c.getPublicKey();
    }

    static String metadataEntity(String metadata) throws Exception {
        Document d = parse(metadata.getBytes(StandardCharsets.UTF_8));
        return d.getDocumentElement().getAttribute("entityID");
    }

    private static Element child(Element parent, String ns, String name) {
        NodeList l = parent.getElementsByTagNameNS(ns, name);
        return l.getLength() == 0 ? null : (Element) l.item(0);
    }

    static Assertion validate(String assertionB64, String metadata, String roleArn, String principalArn, Instant now) throws Invalid {
        try {
            byte[] xml = Base64.getMimeDecoder().decode(assertionB64);
            Document doc = parse(xml);
            String saml = "urn:oasis:names:tc:SAML:2.0:assertion";
            Element root = doc.getDocumentElement();
            Element assertion = "Assertion".equals(root.getLocalName()) ? root : child(root, saml, "Assertion");
            if (assertion == null) {
                throw new Invalid("InvalidIdentityToken", "No SAML assertion found");
            }
            NodeList sigs = assertion.getElementsByTagNameNS(XMLSignature.XMLNS, "Signature");
            if (sigs.getLength() != 1 || sigs.item(0).getParentNode() != assertion) {
                throw new Invalid("InvalidIdentityToken", "Response signature invalid");
            }
            assertion.setIdAttribute("ID", true);
            XMLSignatureFactory fac = XMLSignatureFactory.getInstance("DOM");
            DOMValidateContext ctx = new DOMValidateContext(KeySelector.singletonKeySelector(metadataKey(metadata)), sigs.item(0));
            ctx.setProperty("org.jcp.xml.dsig.secureValidation", Boolean.TRUE);
            XMLSignature sig = fac.unmarshalXMLSignature(ctx);
            List<?> refs = sig.getSignedInfo().getReferences();
            if (refs.size() != 1) {
                throw new Invalid("InvalidIdentityToken", "Response signature invalid");
            }
            Reference ref = (Reference) refs.get(0);
            if (!("#" + assertion.getAttribute("ID")).equals(ref.getURI())) {
                throw new Invalid("InvalidIdentityToken", "Response signature invalid");
            }
            for (Object t : ref.getTransforms()) {
                String alg = ((Transform) t).getAlgorithm();
                if (!alg.equals(Transform.ENVELOPED) && !alg.equals("http://www.w3.org/2001/10/xml-exc-c14n#")
                        && !alg.equals("http://www.w3.org/TR/2001/REC-xml-c14n-20010315")) {
                    throw new Invalid("InvalidIdentityToken", "Response signature invalid");
                }
            }
            if (!sig.validate(ctx)) {
                throw new Invalid("InvalidIdentityToken", "Response signature invalid");
            }
            Element issuer = child(assertion, saml, "Issuer");
            String entity = metadataEntity(metadata);
            if (issuer == null || (!entity.isEmpty() && !entity.equals(issuer.getTextContent().trim()))) {
                throw new Invalid("InvalidIdentityToken", "Issuer does not match the SAML provider");
            }
            Element conditions = child(assertion, saml, "Conditions");
            if (conditions != null) {
                String nb = conditions.getAttribute("NotBefore");
                String na = conditions.getAttribute("NotOnOrAfter");
                if (!nb.isEmpty() && now.isBefore(Instant.parse(nb).minusSeconds(300))) {
                    throw new Invalid("InvalidIdentityToken", "Assertion is not yet valid");
                }
                if (!na.isEmpty() && !now.isBefore(Instant.parse(na).plusSeconds(300))) {
                    throw new Invalid("ExpiredTokenException", "Token expired");
                }
                NodeList aud = conditions.getElementsByTagNameNS(saml, "Audience");
                boolean okAud = aud.getLength() == 0;
                for (int i = 0; i < aud.getLength(); i++) {
                    String a = aud.item(i).getTextContent().trim();
                    okAud |= a.equals("urn:amazon:webservices") || a.startsWith("https://signin.aws.amazon.com/saml");
                }
                if (!okAud) {
                    throw new Invalid("InvalidIdentityToken", "Audience is not urn:amazon:webservices");
                }
            }
            boolean roleOk = false;
            NodeList attrs = assertion.getElementsByTagNameNS(saml, "Attribute");
            String roleAttr = "https://aws.amazon.com/SAML/Attributes/Role";
            String sessionName = null;
            for (int i = 0; i < attrs.getLength(); i++) {
                Element a = (Element) attrs.item(i);
                NodeList vals = a.getElementsByTagNameNS(saml, "AttributeValue");
                if (a.getAttribute("Name").equals(roleAttr)) {
                    for (int j = 0; j < vals.getLength(); j++) {
                        List<String> parts = new ArrayList<>();
                        for (String p : vals.item(j).getTextContent().trim().split(",")) {
                            parts.add(p.trim());
                        }
                        roleOk |= parts.contains(roleArn) && parts.contains(principalArn);
                    }
                } else if (a.getAttribute("Name").equals("https://aws.amazon.com/SAML/Attributes/RoleSessionName") && vals.getLength() > 0) {
                    sessionName = vals.item(0).getTextContent().trim();
                }
            }
            if (!roleOk) {
                throw new Invalid("InvalidIdentityToken", "The Role attribute does not include the requested role and provider");
            }
            Element nameId = child(assertion, saml, "NameID");
            String subject = nameId == null ? "saml-user" : nameId.getTextContent().trim();
            return new Assertion(subject, sessionName != null ? sessionName : subject);
        } catch (Invalid e) {
            throw e;
        } catch (Exception e) {
            throw new Invalid("InvalidIdentityToken", "Response signature invalid");
        }
    }
}
