package com.sayonora.wire.oswire;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * A unified-highlighter look-alike: matched query terms of the analysed field text are wrapped in the pre/post tags
 * ({@code <em>} by default), fragments are sentence-bounded up to {@code fragment_size}. Handles term/match/phrase/
 * prefix/wildcard/regexp/fuzzy/multi_match/query_string queries (through {@link Query.Hl}); field-text offsets come from
 * the same analyzer used for indexing. {@code type} (unified/plain/fvh) is accepted and ignored.
 */
final class Highlighter {

    private Highlighter() {
    }

    static JsonObject highlight(Query.DocCtx ctx, JsonObject spec, Query.Hl hl) {
        JsonObject out = new JsonObject();
        JsonElement fieldsEl = spec.get("fields");
        if (fieldsEl == null) {
            throw new OpenSearchException("action_request_validation_exception", "Validation Failed: 1: at least one field must be specified;");
        }
        List<String[]> names = new ArrayList<>();
        List<JsonObject> opts = new ArrayList<>();
        if (fieldsEl.isJsonArray()) {
            for (JsonElement e : fieldsEl.getAsJsonArray()) {
                for (var en : e.getAsJsonObject().entrySet()) {
                    names.add(new String[] {en.getKey()});
                    opts.add(en.getValue().isJsonObject() ? en.getValue().getAsJsonObject() : new JsonObject());
                }
            }
        } else {
            for (var en : fieldsEl.getAsJsonObject().entrySet()) {
                names.add(new String[] {en.getKey()});
                opts.add(en.getValue().isJsonObject() ? en.getValue().getAsJsonObject() : new JsonObject());
            }
        }
        for (int i = 0; i < names.size(); i++) {
            String pattern = names.get(i)[0];
            JsonObject fo = opts.get(i);
            for (Mappings.Field f : ctx.ix.mappings.fields.values()) {
                boolean hit = pattern.contains("*") ? Mappings.wildcardMatch(pattern, f.path) : f.path.equals(pattern);
                if (!hit || !(f.isString()) || f.nestedPath != null && ctx.prefix.isEmpty()) {
                    continue;
                }
                JsonArray frags = fragments(ctx, f, spec, fo, hl);
                if (frags != null && frags.size() > 0) {
                    out.add(f.path, frags);
                }
            }
        }
        return out;
    }

    private static String opt(JsonObject fo, JsonObject spec, String k, String dflt) {
        if (fo.has(k)) {
            return fo.get(k).isJsonArray() ? fo.getAsJsonArray(k).get(0).getAsString() : fo.get(k).getAsString();
        }
        if (spec.has(k)) {
            return spec.get(k).isJsonArray() ? spec.getAsJsonArray(k).get(0).getAsString() : spec.get(k).getAsString();
        }
        return dflt;
    }

    private static JsonArray fragments(Query.DocCtx ctx, Mappings.Field f, JsonObject spec, JsonObject fo, Query.Hl hl) {
        String pre = opt(fo, spec, "pre_tags", "<em>");
        String post = opt(fo, spec, "post_tags", "</em>");
        int fragSize = Integer.parseInt(opt(fo, spec, "fragment_size", "100"));
        int numFrags = Integer.parseInt(opt(fo, spec, "number_of_fragments", "5"));
        int noMatch = Integer.parseInt(opt(fo, spec, "no_match_size", "0"));
        boolean requireFieldMatch = Boolean.parseBoolean(opt(fo, spec, "require_field_match", "true"));
        Set<String> terms = new java.util.HashSet<>();
        Set<String> prefixes = new java.util.HashSet<>();
        String base = f.path;
        String parent = f.subField ? f.path.substring(0, f.path.lastIndexOf('.')) : f.path;
        for (var e : hl.terms.entrySet()) {
            if (!requireFieldMatch || e.getKey().equals(base) || e.getKey().equals(parent) || e.getKey().equals("*") || Mappings.wildcardMatch(e.getKey(), base)) {
                terms.addAll(e.getValue());
            }
        }
        for (var e : hl.prefixes.entrySet()) {
            if (!requireFieldMatch || e.getKey().equals(base) || e.getKey().equals(parent) || e.getKey().equals("*") || Mappings.wildcardMatch(e.getKey(), base)) {
                prefixes.addAll(e.getValue());
            }
        }
        List<String> texts = new ArrayList<>();
        int offsetLimit = 1_000_000;
        try {
            var idxs = ctx.ix.settings.getAsJsonObject("index");
            if (idxs.has("highlight") && idxs.getAsJsonObject("highlight").has("max_analyzed_offset")) {
                offsetLimit = Integer.parseInt(idxs.getAsJsonObject("highlight").get("max_analyzed_offset").getAsString());
            }
        } catch (RuntimeException ignored) {
            // default limit
        }
        String reqOffset = opt(fo, spec, "max_analyzed_offset", null);
        for (JsonElement e : Mappings.values(ctx.obj, IndexCtx.sourcePath(f).startsWith(ctx.prefix) ? IndexCtx.sourcePath(f).substring(ctx.prefix.length()) : IndexCtx.sourcePath(f))) {
            if (e.isJsonPrimitive()) {
                String v = e.getAsString();
                if (f.isKeyword() && f.def != null && f.def.has("ignore_above") && v.length() > f.def.get("ignore_above").getAsInt()) {
                    continue;
                }
                if (v.length() > offsetLimit) {
                    if (reqOffset == null) {
                        throw OpenSearchException.illegalArgument("The length [" + v.length() + "] of field [" + f.path + "] in doc[" + ctx.doc.id
                                + "]/index[" + ctx.doc.index + "] exceeds the [index.highlight.max_analyzed_offset] limit [" + offsetLimit
                                + "]. To avoid this error, set the query parameter [max_analyzed_offset] to a value less than index setting ["
                                + offsetLimit + "] and this will tolerate long field values by truncating them.").asShardLevel();
                    }
                    v = v.substring(0, Math.min(v.length(), Integer.parseInt(reqOffset)));
                }
                texts.add(v);
            }
        }
        Analysis.Analyzer an = ctx.ix.indexAnalyzer(f);
        JsonArray out = new JsonArray();
        List<Object[]> scored = new ArrayList<>();
        for (String text : texts) {
            List<Analysis.Token> toks = f.isKeyword() ? List.of(new Analysis.Token(text.toLowerCase(Locale.ROOT), 0, text.length(), 0)) : an.analyze(text);
            List<int[]> spans = new ArrayList<>();
            for (Analysis.Token t : toks) {
                String term = t.term();
                boolean m = terms.contains(term) || terms.contains(term.toLowerCase(Locale.ROOT));
                if (!m) {
                    for (String p : prefixes) {
                        if (term.startsWith(p)) {
                            m = true;
                            break;
                        }
                    }
                }
                if (!m) {
                    for (var pat : hl.patterns) {
                        if (pat.matcher(term).matches()) {
                            m = true;
                            break;
                        }
                    }
                }
                if (m) {
                    spans.add(new int[] {t.start(), t.end()});
                }
            }
            if (spans.isEmpty()) {
                continue;
            }
            if (numFrags == 0) {
                scored.add(new Object[] {(double) spans.size(), wrap(text, 0, text.length(), spans, pre, post), 0});
                continue;
            }
            for (int[] seg : passages(text, fragSize)) {
                List<int[]> in = new ArrayList<>();
                for (int[] sp : spans) {
                    if (sp[0] >= seg[0] && sp[1] <= seg[1]) {
                        in.add(sp);
                    }
                }
                if (!in.isEmpty()) {
                    scored.add(new Object[] {(double) in.size(), wrap(text, seg[0], seg[1], in, pre, post), seg[0]});
                }
            }
        }
        boolean byScore = "score".equals(opt(fo, spec, "order", "none"));
        if (byScore) {
            scored.sort((a, b) -> Double.compare((Double) b[0], (Double) a[0]));
        }
        int limit = numFrags == 0 ? Integer.MAX_VALUE : numFrags;
        for (int i = 0; i < scored.size() && i < limit; i++) {
            out.add((String) scored.get(i)[1]);
        }
        if (out.size() == 0 && noMatch > 0 && !texts.isEmpty()) {
            String t = texts.get(0);
            out.add(t.length() <= noMatch ? t : t.substring(0, noMatch));
        }
        return out;
    }

    /** Sentence-bounded passages of at most {@code max} chars (long sentences are cut at a word boundary). */
    static List<int[]> passages(String text, int max) {
        List<int[]> out = new ArrayList<>();
        java.text.BreakIterator bi = java.text.BreakIterator.getSentenceInstance(Locale.ROOT);
        bi.setText(text);
        int start = bi.first();
        int passageStart = -1, passageEnd = -1;
        for (int end = bi.next(); end != java.text.BreakIterator.DONE; start = end, end = bi.next()) {
            int s = start, e = end;
            while (e > s && Character.isWhitespace(text.charAt(e - 1))) {
                e--;
            }
            if (e <= s) {
                continue;
            }
            if (e - s > max) {
                if (passageStart >= 0) {
                    out.add(new int[] {passageStart, passageEnd});
                    passageStart = -1;
                }
                int a = s;
                while (a < e) {
                    int b = Math.min(e, a + max);
                    if (b < e) {
                        int sp = text.lastIndexOf(' ', b);
                        if (sp > a) {
                            b = sp;
                        }
                    }
                    out.add(new int[] {a, b});
                    a = b;
                    while (a < e && Character.isWhitespace(text.charAt(a))) {
                        a++;
                    }
                }
            } else if (passageStart >= 0 && e - passageStart <= max) {
                passageEnd = e;
            } else {
                if (passageStart >= 0) {
                    out.add(new int[] {passageStart, passageEnd});
                }
                passageStart = s;
                passageEnd = e;
            }
        }
        if (passageStart >= 0) {
            out.add(new int[] {passageStart, passageEnd});
        }
        return out;
    }

    private static String wrap(String text, int from, int to, List<int[]> spans, String pre, String post) {
        StringBuilder sb = new StringBuilder();
        int cur = from;
        for (int[] sp : spans) {
            if (sp[0] < cur) {
                continue;
            }
            sb.append(text, cur, sp[0]).append(pre).append(text, sp[0], sp[1]).append(post);
            cur = sp[1];
        }
        sb.append(text, cur, to);
        return sb.toString();
    }
}
