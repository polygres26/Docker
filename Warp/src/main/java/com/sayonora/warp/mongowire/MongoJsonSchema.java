package com.sayonora.warp.mongowire;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonValue;

/** The {@code $jsonSchema} subset MongoDB documents (draft-4 keywords plus {@code bsonType}). */
final class MongoJsonSchema {

    private MongoJsonSchema() {
    }

    private static final Set<String> IGNORED = Set.of("title", "description", "$comment", "$schema", "id", "definitions");

    static Predicate<BsonValue> compile(BsonValue spec) {
        if (!spec.isDocument()) {
            throw new MongoCmdException(14, "$jsonSchema must be an object");
        }
        List<Predicate<BsonValue>> checks = new ArrayList<>();
        BsonDocument s = spec.asDocument();
        for (Map.Entry<String, BsonValue> e : s.entrySet()) {
            String k = e.getKey();
            BsonValue v = e.getValue();
            if (IGNORED.contains(k)) {
                continue;
            }
            switch (k) {
                case "bsonType", "type" -> {
                    List<String> names = new ArrayList<>();
                    if (v.isString()) {
                        names.add(v.asString().getValue());
                    } else if (v.isArray()) {
                        v.asArray().forEach(x -> names.add(x.asString().getValue()));
                    } else {
                        throw new MongoCmdException(2, k + " must be either a string or an array of strings");
                    }
                    boolean json = k.equals("type");
                    for (String n : names) {
                        if (!json && MongoMatcher.typeNumber(n) == Integer.MIN_VALUE) {
                            throw new MongoCmdException(2, "Unknown type name alias: " + n);
                        }
                    }
                    checks.add(x -> {
                        for (String n : names) {
                            if (json ? jsonType(x, n) : bsonType(x, n)) {
                                return true;
                            }
                        }
                        return false;
                    });
                }
                case "properties" -> {
                    List<String> keys = new ArrayList<>();
                    List<Predicate<BsonValue>> subs = new ArrayList<>();
                    for (Map.Entry<String, BsonValue> p : v.asDocument().entrySet()) {
                        keys.add(p.getKey());
                        subs.add(compile(p.getValue()));
                    }
                    checks.add(x -> {
                        if (!x.isDocument()) {
                            return true;
                        }
                        for (int i = 0; i < keys.size(); i++) {
                            BsonValue c = x.asDocument().get(keys.get(i));
                            if (c != null && !subs.get(i).test(c)) {
                                return false;
                            }
                        }
                        return true;
                    });
                }
                case "patternProperties" -> {
                    List<Pattern> pats = new ArrayList<>();
                    List<Predicate<BsonValue>> subs = new ArrayList<>();
                    for (Map.Entry<String, BsonValue> p : v.asDocument().entrySet()) {
                        pats.add(Pattern.compile(p.getKey()));
                        subs.add(compile(p.getValue()));
                    }
                    checks.add(x -> {
                        if (!x.isDocument()) {
                            return true;
                        }
                        for (Map.Entry<String, BsonValue> f : x.asDocument().entrySet()) {
                            for (int i = 0; i < pats.size(); i++) {
                                if (pats.get(i).matcher(f.getKey()).find() && !subs.get(i).test(f.getValue())) {
                                    return false;
                                }
                            }
                        }
                        return true;
                    });
                }
                case "additionalProperties" -> {
                    Set<String> props = s.containsKey("properties") ? new HashSet<>(s.getDocument("properties").keySet()) : Set.of();
                    List<Pattern> pats = new ArrayList<>();
                    if (s.containsKey("patternProperties")) {
                        s.getDocument("patternProperties").keySet().forEach(p -> pats.add(Pattern.compile(p)));
                    }
                    Predicate<BsonValue> sub = v.isDocument() ? compile(v) : null;
                    boolean allowed = !v.isBoolean() || v.asBoolean().getValue();
                    checks.add(x -> {
                        if (!x.isDocument()) {
                            return true;
                        }
                        for (Map.Entry<String, BsonValue> f : x.asDocument().entrySet()) {
                            if (props.contains(f.getKey()) || pats.stream().anyMatch(p -> p.matcher(f.getKey()).find())) {
                                continue;
                            }
                            if (sub != null ? !sub.test(f.getValue()) : !allowed) {
                                return false;
                            }
                        }
                        return true;
                    });
                }
                case "required" -> {
                    List<String> req = new ArrayList<>();
                    v.asArray().forEach(r -> req.add(r.asString().getValue()));
                    checks.add(x -> !x.isDocument() || req.stream().allMatch(x.asDocument()::containsKey));
                }
                case "minProperties" -> {
                    long n = MongoNum.truncLong(v);
                    checks.add(x -> !x.isDocument() || x.asDocument().size() >= n);
                }
                case "maxProperties" -> {
                    long n = MongoNum.truncLong(v);
                    checks.add(x -> !x.isDocument() || x.asDocument().size() <= n);
                }
                case "items" -> {
                    if (v.isArray()) {
                        List<Predicate<BsonValue>> subs = new ArrayList<>();
                        v.asArray().forEach(i -> subs.add(compile(i)));
                        Predicate<BsonValue> extra = s.containsKey("additionalItems") && s.get("additionalItems").isDocument()
                                ? compile(s.get("additionalItems")) : null;
                        boolean extraOk = !s.containsKey("additionalItems") || !s.get("additionalItems").isBoolean()
                                || s.getBoolean("additionalItems").getValue();
                        checks.add(x -> {
                            if (!x.isArray()) {
                                return true;
                            }
                            BsonArray a = x.asArray();
                            for (int i = 0; i < a.size(); i++) {
                                if (i < subs.size()) {
                                    if (!subs.get(i).test(a.get(i))) {
                                        return false;
                                    }
                                } else if (extra != null ? !extra.test(a.get(i)) : !extraOk) {
                                    return false;
                                }
                            }
                            return true;
                        });
                    } else {
                        Predicate<BsonValue> sub = compile(v);
                        checks.add(x -> !x.isArray() || x.asArray().stream().allMatch(sub));
                    }
                }
                case "additionalItems" -> { }
                case "minItems" -> {
                    long n = MongoNum.truncLong(v);
                    checks.add(x -> !x.isArray() || x.asArray().size() >= n);
                }
                case "maxItems" -> {
                    long n = MongoNum.truncLong(v);
                    checks.add(x -> !x.isArray() || x.asArray().size() <= n);
                }
                case "uniqueItems" -> {
                    if (v.asBoolean().getValue()) {
                        checks.add(x -> {
                            if (!x.isArray()) {
                                return true;
                            }
                            Set<String> seen = new HashSet<>();
                            for (BsonValue i : x.asArray()) {
                                if (!seen.add(BsonCmp.key(i))) {
                                    return false;
                                }
                            }
                            return true;
                        });
                    }
                }
                case "minLength" -> {
                    long n = MongoNum.truncLong(v);
                    checks.add(x -> !x.isString() || x.asString().getValue().codePointCount(0, x.asString().getValue().length()) >= n);
                }
                case "maxLength" -> {
                    long n = MongoNum.truncLong(v);
                    checks.add(x -> !x.isString() || x.asString().getValue().codePointCount(0, x.asString().getValue().length()) <= n);
                }
                case "pattern" -> {
                    Pattern p = Pattern.compile(v.asString().getValue());
                    checks.add(x -> !x.isString() || p.matcher(x.asString().getValue()).find());
                }
                case "minimum", "maximum" -> {
                    boolean min = k.equals("minimum");
                    boolean excl = s.containsKey(min ? "exclusiveMinimum" : "exclusiveMaximum")
                            && s.get(min ? "exclusiveMinimum" : "exclusiveMaximum").isBoolean()
                            && s.getBoolean(min ? "exclusiveMinimum" : "exclusiveMaximum").getValue();
                    checks.add(x -> {
                        if (!BsonCmp.isNumber(x)) {
                            return true;
                        }
                        int c = BsonCmp.compareNumbers(x, v);
                        return min ? (excl ? c > 0 : c >= 0) : (excl ? c < 0 : c <= 0);
                    });
                }
                case "exclusiveMinimum", "exclusiveMaximum" -> { }
                case "multipleOf" -> checks.add(x -> !BsonCmp.isNumber(x)
                        || BsonCmp.toBigDecimal(x).remainder(BsonCmp.toBigDecimal(v)).signum() == 0);
                case "enum" -> checks.add(x -> {
                    for (BsonValue e2 : v.asArray()) {
                        if (BsonCmp.compare(e2, x) == 0) {
                            return true;
                        }
                    }
                    return false;
                });
                case "allOf", "anyOf", "oneOf" -> {
                    List<Predicate<BsonValue>> subs = new ArrayList<>();
                    v.asArray().forEach(i -> subs.add(compile(i)));
                    checks.add(x -> {
                        int n = 0;
                        for (Predicate<BsonValue> p : subs) {
                            if (p.test(x)) {
                                n++;
                            }
                        }
                        return k.equals("allOf") ? n == subs.size() : k.equals("anyOf") ? n >= 1 : n == 1;
                    });
                }
                case "not" -> {
                    Predicate<BsonValue> sub = compile(v);
                    checks.add(x -> !sub.test(x));
                }
                default -> throw new MongoCmdException(9, "Unknown $jsonSchema keyword: " + k);
            }
        }
        return x -> {
            for (Predicate<BsonValue> c : checks) {
                if (!c.test(x)) {
                    return false;
                }
            }
            return true;
        };
    }

    private static boolean bsonType(BsonValue x, String name) {
        if (name.equals("number")) {
            return BsonCmp.isNumber(x);
        }
        return MongoMatcher.typeName(x).equals(name);
    }

    private static boolean jsonType(BsonValue x, String name) {
        return switch (name) {
            case "object" -> x.isDocument();
            case "array" -> x.isArray();
            case "number" -> BsonCmp.isNumber(x);
            case "boolean" -> x.isBoolean();
            case "string" -> x.isString();
            case "null" -> x.isNull();
            default -> throw new MongoCmdException(2, "Unknown type name alias: " + name);
        };
    }
}
