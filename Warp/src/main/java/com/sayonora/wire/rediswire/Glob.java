package com.sayonora.wire.rediswire;

/** Redis glob matching (stringmatchlen): {@code * ? [abc] [^a] [a-z] \x}, byte-wise and case-sensitive. */
final class Glob {

    private Glob() {
    }

    static boolean matches(byte[] pattern, byte[] s) {
        if (pattern.length == 1 && pattern[0] == '*') {
            return true;
        }
        return match(pattern, 0, s, 0);
    }

    private static boolean match(byte[] p, int pi, byte[] s, int si) {
        int plen = p.length;
        int slen = s.length;
        while (pi < plen && si < slen) {
            switch (p[pi]) {
                case '*': {
                    while (pi + 1 < plen && p[pi + 1] == '*') {
                        pi++;
                    }
                    if (pi + 1 == plen) {
                        return true;
                    }
                    while (si < slen) {
                        if (match(p, pi + 1, s, si)) {
                            return true;
                        }
                        si++;
                    }
                    return false;
                }
                case '?':
                    si++;
                    break;
                case '[': {
                    pi++;
                    boolean not = pi < plen && p[pi] == '^';
                    if (not) {
                        pi++;
                    }
                    boolean m = false;
                    while (true) {
                        if (pi < plen && p[pi] == '\\' && pi + 1 < plen) {
                            pi++;
                            if (p[pi] == s[si]) {
                                m = true;
                            }
                        } else if (pi < plen && p[pi] == ']') {
                            break;
                        } else if (pi >= plen) {
                            pi--;
                            break;
                        } else if (pi + 2 < plen && p[pi + 1] == '-') {
                            int start = p[pi] & 0xff;
                            int end = p[pi + 2] & 0xff;
                            int c = s[si] & 0xff;
                            if (start > end) {
                                int t = start;
                                start = end;
                                end = t;
                            }
                            if (c >= start && c <= end) {
                                m = true;
                            }
                            pi += 2;
                        } else if (p[pi] == s[si]) {
                            m = true;
                        }
                        pi++;
                    }
                    if (not) {
                        m = !m;
                    }
                    if (!m) {
                        return false;
                    }
                    si++;
                    break;
                }
                case '\\':
                    if (pi + 1 < plen) {
                        pi++;
                    }
                    if (p[pi] != s[si]) {
                        return false;
                    }
                    si++;
                    break;
                default:
                    if (p[pi] != s[si]) {
                        return false;
                    }
                    si++;
                    break;
            }
            pi++;
            if (si >= slen) {
                while (pi < plen && p[pi] == '*') {
                    pi++;
                }
                break;
            }
        }
        return pi >= plen && si >= slen;
    }
}
