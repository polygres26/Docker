package com.sayonora.warp.oswire;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.text.Normalizer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.function.UnaryOperator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Text analysis for oswire: an approximation of Lucene's built-in analyzers that runs in the JVM
 * (Postgres' own text search configs cannot reproduce Lucene tokenisation, so the engine tokenises
 * itself). Implemented: {@code standard} (UAX#29-style word breaking: letters/digits/underscore join,
 * {@code '} {@code .} {@code :} join letters, {@code . , ;} join digits, each CJK ideograph is its own
 * token; lowercased), {@code simple}, {@code whitespace}, {@code keyword}, {@code stop}, and custom
 * analyzers from index {@code settings.analysis} (tokenizers standard/whitespace/keyword/letter/
 * lowercase/classic/pattern/ngram/edge_ngram/path_hierarchy; filters lowercase/uppercase/stop/
 * asciifolding/trim/unique/length/truncate/reverse/ngram/edge_ngram/apostrophe; char_filters
 * html_strip/pattern_replace). Stemming analyzers ({@code english}, snowball, ...) and
 * synonym/phonetic filters are NOT available and fail with a clear {@code illegal_argument_exception}.
 */
final class Analysis {

    record Token(String term, int start, int end, int pos) {
    }

    interface Analyzer {
        List<Token> analyze(String text);
    }

    static final Set<String> ENGLISH_STOP = Set.of("a", "an", "and", "are", "as", "at", "be", "but", "by", "for", "if",
            "in", "into", "is", "it", "no", "not", "of", "on", "or", "such", "that", "the", "their", "then", "there",
            "these", "they", "this", "to", "was", "will", "with");

    static final Analyzer STANDARD = text -> lower(standardTokens(text));
    static final Analyzer SIMPLE = text -> lower(letterTokens(text));
    static final Analyzer WHITESPACE = Analysis::whitespaceTokens;
    static final Analyzer KEYWORD = text -> List.of(new Token(text, 0, text.length(), 0));
    static final Analyzer STOP = text -> stop(lower(letterTokens(text)), ENGLISH_STOP);

    private Analysis() {
    }

    /** The {@code type} an {@code _analyze} token carries: standard-tokenizer analyzers label word/number tokens, others say "word". */
    static String tokenType(boolean standardTokenizer, String term) {
        if (!standardTokenizer) {
            return "word";
        }
        boolean hasDigit = false, hasLetter = false;
        for (int i = 0; i < term.length(); ) {
            int cp = term.codePointAt(i);
            i += Character.charCount(cp);
            if (Character.isDigit(cp)) {
                hasDigit = true;
            } else if (Character.isLetter(cp)) {
                hasLetter = true;
                if (Character.UnicodeScript.of(cp) == Character.UnicodeScript.HAN) {
                    return "<IDEOGRAPHIC>";
                }
                if (Character.UnicodeScript.of(cp) == Character.UnicodeScript.HIRAGANA) {
                    return "<HIRAGANA>";
                }
                if (Character.UnicodeScript.of(cp) == Character.UnicodeScript.KATAKANA) {
                    return "<KATAKANA>";
                }
            }
        }
        return hasDigit && !hasLetter ? "<NUM>" : "<ALPHANUM>";
    }

    /** Resolves an analyzer by name against the index's settings (custom analyzers) . */
    static Analyzer resolve(String name, JsonObject indexSettings) {
        JsonObject analysis = analysisSettings(indexSettings);
        if (analysis != null && analysis.has("analyzer") && analysis.getAsJsonObject("analyzer").has(name)) {
            return custom(analysis.getAsJsonObject("analyzer").getAsJsonObject(name), analysis);
        }
        return switch (name) {
            case "standard" -> STANDARD;
            case "simple" -> SIMPLE;
            case "whitespace" -> WHITESPACE;
            case "keyword" -> KEYWORD;
            case "stop" -> STOP;
            case "default" -> STANDARD;
            default -> throw OpenSearchException.illegalArgument("analyzer [" + name + "] has not been configured in mappings");
        };
    }

    static JsonObject analysisSettings(JsonObject settings) {
        if (settings == null) {
            return null;
        }
        JsonObject s = settings.has("index") && settings.get("index").isJsonObject() ? settings.getAsJsonObject("index") : settings;
        return s.has("analysis") && s.get("analysis").isJsonObject() ? s.getAsJsonObject("analysis") : null;
    }

    private static Analyzer custom(JsonObject def, JsonObject analysis) {
        String type = def.has("type") ? def.get("type").getAsString() : "custom";
        if (!type.equals("custom")) {
            return switch (type) {
                case "standard" -> STANDARD;
                case "simple" -> SIMPLE;
                case "whitespace" -> WHITESPACE;
                case "keyword" -> KEYWORD;
                case "stop" -> STOP;
                default -> throw OpenSearchException.illegalArgument("analyzer type [" + type + "] is not supported by oswire");
            };
        }
        String tokenizer = def.has("tokenizer") ? def.get("tokenizer").getAsString() : "standard";
        List<UnaryOperator<String>> charFilters = new ArrayList<>();
        if (def.has("char_filter")) {
            for (JsonElement e : asList(def.get("char_filter"))) {
                charFilters.add(charFilter(e.getAsString(), analysis));
            }
        }
        java.util.function.Function<String, List<Token>> tok = tokenizerFn(tokenizer, analysis);
        List<UnaryOperator<List<Token>>> filters = new ArrayList<>();
        if (def.has("filter")) {
            for (JsonElement e : asList(def.get("filter"))) {
                filters.add(tokenFilter(e.getAsString(), analysis));
            }
        }
        return text -> {
            String t = text;
            for (UnaryOperator<String> cf : charFilters) {
                t = cf.apply(t);
            }
            List<Token> tokens = tok.apply(t);
            for (UnaryOperator<List<Token>> f : filters) {
                tokens = f.apply(tokens);
            }
            return tokens;
        };
    }

    private static List<JsonElement> asList(JsonElement e) {
        List<JsonElement> l = new ArrayList<>();
        if (e.isJsonArray()) {
            e.getAsJsonArray().forEach(l::add);
        } else {
            l.add(e);
        }
        return l;
    }

    private static JsonObject def(JsonObject analysis, String section, String name) {
        if (analysis != null && analysis.has(section) && analysis.getAsJsonObject(section).has(name)) {
            return analysis.getAsJsonObject(section).getAsJsonObject(name);
        }
        return null;
    }

    private static int intOf(JsonObject d, String k, int dflt) {
        return d != null && d.has(k) ? d.get(k).getAsInt() : dflt;
    }

    private static java.util.function.Function<String, List<Token>> tokenizerFn(String name, JsonObject analysis) {
        JsonObject d = def(analysis, "tokenizer", name);
        String type = d != null && d.has("type") ? d.get("type").getAsString() : name;
        return switch (type) {
            case "standard", "classic", "uax_url_email" -> Analysis::standardTokens;
            case "whitespace" -> Analysis::whitespaceTokens;
            case "keyword" -> text -> List.of(new Token(text, 0, text.length(), 0));
            case "letter" -> Analysis::letterTokens;
            case "lowercase" -> text -> lower(letterTokens(text));
            case "pattern" -> {
                Pattern p = Pattern.compile(d != null && d.has("pattern") ? d.get("pattern").getAsString() : "\\W+");
                yield text -> {
                    List<Token> out = new ArrayList<>();
                    int last = 0, pos = 0;
                    Matcher m = p.matcher(text);
                    while (m.find()) {
                        if (m.start() > last) {
                            out.add(new Token(text.substring(last, m.start()), last, m.start(), pos++));
                        }
                        last = m.end();
                    }
                    if (last < text.length()) {
                        out.add(new Token(text.substring(last), last, text.length(), pos));
                    }
                    return out;
                };
            }
            case "ngram", "edge_ngram", "NGram", "EdgeNGram" -> {
                int min = intOf(d, "min_gram", 1), max = intOf(d, "max_gram", 2);
                boolean edge = type.toLowerCase(Locale.ROOT).startsWith("edge");
                yield text -> ngrams(List.of(new Token(text, 0, text.length(), 0)), min, max, edge);
            }
            case "path_hierarchy" -> text -> {
                List<Token> out = new ArrayList<>();
                int idx = 0, pos = 0;
                while (idx < text.length()) {
                    int n = text.indexOf('/', idx + 1);
                    if (n < 0) {
                        n = text.length();
                    }
                    out.add(new Token(text.substring(0, n), 0, n, pos));
                    idx = n;
                }
                return out;
            };
            default -> throw OpenSearchException.illegalArgument("Unknown tokenizer type [" + type + "] for [" + name + "]");
        };
    }

    private static UnaryOperator<String> charFilter(String name, JsonObject analysis) {
        JsonObject d = def(analysis, "char_filter", name);
        String type = d != null && d.has("type") ? d.get("type").getAsString() : name;
        return switch (type) {
            case "html_strip" -> s -> s.replaceAll("<[^>]*>", " ").replace("&amp;", "&").replace("&lt;", "<")
                    .replace("&gt;", ">").replace("&quot;", "\"").replace("&nbsp;", " ");
            case "pattern_replace" -> {
                Pattern p = Pattern.compile(d.get("pattern").getAsString());
                String rep = d.has("replacement") ? d.get("replacement").getAsString() : "";
                yield s -> p.matcher(s).replaceAll(rep);
            }
            default -> throw OpenSearchException.illegalArgument("char_filter [" + name + "] is not supported by oswire");
        };
    }

    private static UnaryOperator<List<Token>> tokenFilter(String name, JsonObject analysis) {
        JsonObject d = def(analysis, "filter", name);
        String type = d != null && d.has("type") ? d.get("type").getAsString() : name;
        return switch (type) {
            case "lowercase" -> Analysis::lower;
            case "uppercase" -> t -> map(t, s -> s.toUpperCase(Locale.ROOT));
            case "trim" -> t -> map(t, String::trim);
            case "reverse" -> t -> map(t, s -> new StringBuilder(s).reverse().toString());
            case "asciifolding" -> t -> map(t, s -> Normalizer.normalize(s, Normalizer.Form.NFD).replaceAll("\\p{M}+", ""));
            case "apostrophe" -> t -> map(t, s -> s.contains("'") ? s.substring(0, s.indexOf('\'')) : s);
            case "unique" -> t -> {
                Set<String> seen = new HashSet<>();
                List<Token> out = new ArrayList<>();
                for (Token x : t) {
                    if (seen.add(x.term())) {
                        out.add(x);
                    }
                }
                return out;
            };
            case "stop" -> {
                Set<String> words = ENGLISH_STOP;
                if (d != null && d.has("stopwords") && d.get("stopwords").isJsonArray()) {
                    words = new HashSet<>();
                    for (JsonElement e : d.getAsJsonArray("stopwords")) {
                        words.add(e.getAsString());
                    }
                }
                Set<String> w = words;
                yield t -> stop(t, w);
            }
            case "length" -> {
                int min = intOf(d, "min", 0), max = intOf(d, "max", Integer.MAX_VALUE);
                yield t -> t.stream().filter(x -> x.term().length() >= min && x.term().length() <= max).toList();
            }
            case "truncate" -> {
                int len = intOf(d, "length", 10);
                yield t -> map(t, s -> s.length() > len ? s.substring(0, len) : s);
            }
            case "ngram", "edge_ngram" -> {
                int min = intOf(d, "min_gram", 1), max = intOf(d, "max_gram", 2);
                yield t -> ngrams(t, min, max, type.equals("edge_ngram"));
            }
            default -> throw OpenSearchException.illegalArgument("token filter [" + name + "] is not supported by oswire");
        };
    }

    private static List<Token> ngrams(List<Token> in, int min, int max, boolean edge) {
        List<Token> out = new ArrayList<>();
        for (Token t : in) {
            String s = t.term();
            for (int i = 0; i < (edge ? 1 : s.length()); i++) {
                for (int len = min; len <= max && i + len <= s.length(); len++) {
                    out.add(new Token(s.substring(i, i + len), t.start(), t.end(), t.pos()));
                }
            }
        }
        return out;
    }

    private static List<Token> map(List<Token> in, UnaryOperator<String> f) {
        List<Token> out = new ArrayList<>(in.size());
        for (Token t : in) {
            out.add(new Token(f.apply(t.term()), t.start(), t.end(), t.pos()));
        }
        return out;
    }

    static List<Token> lower(List<Token> in) {
        return map(in, s -> s.toLowerCase(Locale.ROOT));
    }

    static List<Token> stop(List<Token> in, Set<String> words) {
        List<Token> out = new ArrayList<>();
        for (Token t : in) {
            if (!words.contains(t.term())) {
                out.add(t);
            }
        }
        return out;
    }

    static List<Token> whitespaceTokens(String text) {
        List<Token> out = new ArrayList<>();
        int i = 0, pos = 0, n = text.length();
        while (i < n) {
            while (i < n && Character.isWhitespace(text.charAt(i))) {
                i++;
            }
            int s = i;
            while (i < n && !Character.isWhitespace(text.charAt(i))) {
                i++;
            }
            if (i > s) {
                out.add(new Token(text.substring(s, i), s, i, pos++));
            }
        }
        return out;
    }

    static List<Token> letterTokens(String text) {
        List<Token> out = new ArrayList<>();
        int i = 0, pos = 0, n = text.length();
        while (i < n) {
            while (i < n && !Character.isLetter(text.charAt(i))) {
                i++;
            }
            int s = i;
            while (i < n && Character.isLetter(text.charAt(i))) {
                i++;
            }
            if (i > s) {
                out.add(new Token(text.substring(s, i), s, i, pos++));
            }
        }
        return out;
    }

    private static boolean wordChar(char c) {
        return Character.isLetterOrDigit(c) || c == '_' || Character.getType(c) == Character.NON_SPACING_MARK
                || Character.getType(c) == Character.COMBINING_SPACING_MARK;
    }

    private static boolean ideograph(int cp) {
        Character.UnicodeScript sc = Character.UnicodeScript.of(cp);
        return sc == Character.UnicodeScript.HAN || sc == Character.UnicodeScript.HIRAGANA;
    }

    /** Word breaking modelled on UAX#29 as Lucene's StandardTokenizer applies it. */
    static List<Token> standardTokens(String text) {
        List<Token> out = new ArrayList<>();
        int n = text.length(), i = 0, pos = 0;
        while (i < n) {
            char c = text.charAt(i);
            if (!wordChar(c)) {
                i++;
                continue;
            }
            int cp = text.codePointAt(i);
            if (ideograph(cp)) {
                int len = Character.charCount(cp);
                out.add(new Token(text.substring(i, i + len), i, i + len, pos++));
                i += len;
                continue;
            }
            int s = i;
            while (i < n) {
                char ch = text.charAt(i);
                if (wordChar(ch) && !ideograph(text.codePointAt(i))) {
                    i++;
                    continue;
                }
                if (i + 1 < n && i > s && wordChar(text.charAt(i + 1)) && !ideograph(text.codePointAt(i + 1))) {
                    char prev = text.charAt(i - 1), next = text.charAt(i + 1);
                    boolean letters = Character.isLetter(prev) && Character.isLetter(next);
                    boolean digits = Character.isDigit(prev) && Character.isDigit(next);
                    if ((letters && (ch == '\'' || ch == '’' || ch == '.' || ch == ':' || ch == '·'))
                            || (digits && (ch == '.' || ch == ',' || ch == ';' || ch == '\'' || ch == '’'))) {
                        i++;
                        continue;
                    }
                }
                break;
            }
            int end = i;
            // Lucene splits tokens longer than 255 chars.
            for (int a = s; a < end; a += 255) {
                int b = Math.min(end, a + 255);
                out.add(new Token(text.substring(a, b), a, b, pos++));
            }
        }
        return out;
    }

    /** Analyzes every value of a field (array values are separated by the default 100-position gap). */
    static List<Token> analyzeAll(Analyzer analyzer, List<String> values) {
        List<Token> all = new ArrayList<>();
        int base = 0;
        for (String v : values) {
            List<Token> ts = analyzer.analyze(v);
            int max = -1;
            for (Token t : ts) {
                all.add(new Token(t.term(), t.start(), t.end(), base + t.pos()));
                max = Math.max(max, t.pos());
            }
            base += max + 1 + 100;
        }
        return all;
    }

    static Set<String> terms(Analyzer a, String text) {
        Set<String> s = new LinkedHashSet<>();
        for (Token t : a.analyze(text)) {
            s.add(t.term());
        }
        return s;
    }
}
