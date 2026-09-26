package com.sayonora.wire.azurewire;

import java.util.ArrayDeque;
import java.util.Deque;

/** Tiny XML writer: elements, escaped text, attributes. */
final class AzXml {

    private final StringBuilder sb = new StringBuilder(256);
    private final Deque<String> stack = new ArrayDeque<>();

    AzXml() {
        sb.append("<?xml version=\"1.0\" encoding=\"utf-8\"?>");
    }

    AzXml open(String tag) {
        sb.append('<').append(tag).append('>');
        stack.push(tag);
        return this;
    }

    AzXml openAttrs(String tag, String... attrs) {
        sb.append('<').append(tag);
        for (int i = 0; i + 1 < attrs.length; i += 2) {
            sb.append(' ').append(attrs[i]).append("=\"").append(esc(attrs[i + 1])).append('"');
        }
        sb.append('>');
        stack.push(tag);
        return this;
    }

    AzXml close() {
        sb.append("</").append(stack.pop()).append('>');
        return this;
    }

    AzXml text(String tag, String value) {
        if (value == null) {
            return this;
        }
        if (value.isEmpty()) {
            sb.append('<').append(tag).append("/>");
        } else {
            sb.append('<').append(tag).append('>').append(esc(value)).append("</").append(tag).append('>');
        }
        return this;
    }

    /** element that is always emitted, {@code <Tag/>} when empty */
    AzXml textOrEmpty(String tag, String value) {
        return text(tag, value == null ? "" : value);
    }

    AzXml raw(String s) {
        sb.append(s);
        return this;
    }

    @Override
    public String toString() {
        return sb.toString();
    }

    static String esc(String s) {
        if (s == null) {
            return "";
        }
        StringBuilder o = null;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            String r = switch (c) {
                case '&' -> "&amp;";
                case '<' -> "&lt;";
                case '>' -> "&gt;";
                case '"' -> "&quot;";
                case '\'' -> "&apos;";
                default -> null;
            };
            if (r != null && o == null) {
                o = new StringBuilder(s.length() + 16).append(s, 0, i);
            }
            if (o != null) {
                o.append(r != null ? r : String.valueOf(c));
            }
        }
        return o == null ? s : o.toString();
    }
}
