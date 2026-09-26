package com.sayonora.warp.bigtablewire;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * RE2-flavoured regular expressions over bytes, as Bigtable's row key / family / qualifier / value regex filters use: the
 * expression must match the WHOLE input; {@code .} does not match a newline unless {@code (?s)}; {@code \C} matches any single
 * byte; POSIX classes {@code [[:alpha:]]}; no look-around or back-references (RE2 rejects them). Pattern and input are both
 * matched byte-wise (each byte is one character, ISO-8859-1), like the byte-oriented RE2 of the real service: a multi-byte UTF-8
 * character is several "characters" ({@code caf.} does not match "caf\u00e9"; {@code caf\C\C} does).
 */
final class BtRegex {

    private static final Map<String, String> POSIX = Map.ofEntries(Map.entry("alnum", "\\p{Alnum}"), Map.entry("alpha", "\\p{Alpha}"),
            Map.entry("ascii", "\\p{ASCII}"), Map.entry("blank", "\\p{Blank}"), Map.entry("cntrl", "\\p{Cntrl}"),
            Map.entry("digit", "\\p{Digit}"), Map.entry("graph", "\\p{Graph}"), Map.entry("lower", "\\p{Lower}"),
            Map.entry("print", "\\p{Print}"), Map.entry("punct", "\\p{Punct}"), Map.entry("space", "\\s"),
            Map.entry("upper", "\\p{Upper}"), Map.entry("word", "\\w"), Map.entry("xdigit", "\\p{XDigit}"));

    private BtRegex() {
    }

    /** Compiles {@code pattern}; INVALID_ARGUMENT "Error in field '<field>' : error parsing regexp: ..." when it is not RE2 syntax. */
    static Predicate<byte[]> compile(String field, byte[] pattern) {
        String text = decode(pattern);
        Pattern p;
        try {
            p = Pattern.compile(translate(text), Pattern.UNIX_LINES);
        } catch (PatternSyntaxException e) {
            throw BtException.invalid("Error in field '" + field + "' : error parsing regexp: " + e.getDescription() + ": `" + text + "`");
        }
        return in -> p.matcher(decode(in)).matches();
    }

    /** Bytes are characters (ISO-8859-1), as in the byte-oriented RE2 Bigtable uses: {@code .} and {@code \C} match one byte. */
    private static String decode(byte[] b) {
        return new String(b, StandardCharsets.ISO_8859_1);
    }

    /** RE2 to java.util.regex: POSIX classes, \C, (?P<name>, and the constructs RE2 refuses. */
    static String translate(String re) {
        StringBuilder out = new StringBuilder();
        boolean inClass = false;
        for (int i = 0; i < re.length(); i++) {
            char ch = re.charAt(i);
            if (ch == '\\' && i + 1 < re.length()) {
                char n = re.charAt(i + 1);
                if (n == 'C') {
                    out.append("(?s:.)");
                } else if (n >= '1' && n <= '9' && !inClass) {
                    throw new PatternSyntaxException("invalid escape sequence", re, i);
                } else if (n == 'z') {
                    out.append("\\z");
                } else {
                    out.append(ch).append(n);
                }
                i++;
                continue;
            }
            if (inClass) {
                if (ch == '[' && re.startsWith("[:", i)) {
                    int end = re.indexOf(":]", i + 2);
                    if (end > 0) {
                        String name = re.substring(i + 2, end);
                        boolean neg = name.startsWith("^");
                        String cls = POSIX.get(neg ? name.substring(1) : name);
                        if (cls == null) {
                            throw new PatternSyntaxException("invalid character class range", re, i);
                        }
                        out.append(neg ? cls.replace("\\p", "\\P").replace("\\s", "\\S").replace("\\w", "\\W") : cls);
                        i = end + 1;
                        continue;
                    }
                }
                if (ch == '[') {
                    out.append("\\[");
                    continue;
                }
                if (ch == ']') {
                    inClass = false;
                }
                out.append(ch);
                continue;
            }
            if (ch == '[') {
                inClass = true;
                out.append(ch);
                if (i + 1 < re.length() && re.charAt(i + 1) == '^') {
                    out.append('^');
                    i++;
                }
                if (i + 1 < re.length() && re.charAt(i + 1) == ']') {
                    out.append("\\]");
                    i++;
                }
                continue;
            }
            if (ch == '(' && re.startsWith("(?", i)) {
                if (re.startsWith("(?P<", i)) {
                    out.append("(?<");
                    i += 3;
                    continue;
                }
                if (re.startsWith("(?=", i) || re.startsWith("(?!", i) || re.startsWith("(?<=", i) || re.startsWith("(?<!", i)
                        || re.startsWith("(?>", i)) {
                    throw new PatternSyntaxException("invalid or unsupported Perl syntax", re, i);
                }
            }
            if ((ch == '+') && out.length() > 0 && "*+?}".indexOf(out.charAt(out.length() - 1)) >= 0
                    && !(out.length() > 1 && out.charAt(out.length() - 2) == '\\')) {
                throw new PatternSyntaxException("invalid nested repetition operator", re, i);
            }
            out.append(ch);
        }
        return out.toString();
    }
}
