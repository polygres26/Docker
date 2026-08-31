package com.nexagres.dms.uploads;

import java.util.regex.Pattern;

/**
 * Strips markup from an AWR report (Oracle ships these as HTML) down to plain text before it's
 * handed to {@link com.nexagres.dms.llm.ReportAnalyzer} -- deliberately a simple regex-based
 * pass, not a real HTML parser (no new dependency for what only needs to make the text readable,
 * not render it). Non-HTML uploads (plain text, CSV) pass through {@link #maybeStrip} unchanged.
 */
public final class HtmlTextExtractor {

    private static final Pattern SCRIPT_OR_STYLE = Pattern.compile("(?is)<(script|style)[^>]*>.*?</\\1>");
    private static final Pattern TAG = Pattern.compile("(?s)<[^>]+>");
    private static final Pattern BLANK_LINES = Pattern.compile("\\n{3,}");

    private HtmlTextExtractor() {}

    public static String maybeStrip(String content) {
        String trimmed = content.stripLeading();
        boolean looksLikeHtml = trimmed.regionMatches(true, 0, "<!DOCTYPE html", 0, 14)
            || trimmed.regionMatches(true, 0, "<html", 0, 5)
            || content.toLowerCase().contains("<body");
        if (!looksLikeHtml) return content;

        String withoutScripts = SCRIPT_OR_STYLE.matcher(content).replaceAll(" ");
        String withoutTags = TAG.matcher(withoutScripts).replaceAll("\n");
        String decoded = withoutTags
            .replace("&nbsp;", " ").replace("&amp;", "&").replace("&lt;", "<")
            .replace("&gt;", ">").replace("&quot;", "\"").replace("&#39;", "'");
        return BLANK_LINES.matcher(decoded).replaceAll("\n\n").strip();
    }
}
