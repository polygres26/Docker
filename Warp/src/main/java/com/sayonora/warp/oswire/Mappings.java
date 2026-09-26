package com.sayonora.warp.oswire;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import java.math.BigDecimal;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * An index's mapping (the {@code mappings} JSON real OpenSearch returns from {@code GET /idx/_mapping}) plus the
 * two things done with it: flattened field lookup for the query engine, and dynamic mapping / value validation at
 * index time (types inferred exactly like OpenSearch's defaults: string -> text + {@code keyword} sub-field
 * (ignore_above 256) or date, integer -> long, decimal -> float, boolean, object -> object, arrays by first element;
 * {@code dynamic}: true/false/strict/runtime at any object level; {@code dynamic_templates}; type conflicts ->
 * {@code mapper_parsing_exception}). Immutable: dynamic updates return a new instance.
 */
final class Mappings {

    static final class Field {
        String path;
        String type;
        JsonObject def;
        String analyzer;
        String searchAnalyzer;
        String format;
        String nestedPath;
        boolean subField;
        boolean indexed = true;
        String aliasTarget;

        boolean isText() {
            return type.equals("text") || type.equals("match_only_text");
        }

        boolean isKeyword() {
            return type.equals("keyword") || type.equals("constant_keyword") || type.equals("wildcard");
        }

        boolean isString() {
            return isText() || isKeyword();
        }

        boolean isNumeric() {
            return switch (type) {
                case "long", "integer", "short", "byte", "double", "float", "half_float", "scaled_float", "unsigned_long" -> true;
                default -> false;
            };
        }

        boolean isInteger() {
            return switch (type) {
                case "long", "integer", "short", "byte", "unsigned_long" -> true;
                default -> false;
            };
        }
    }

    private static final Pattern DYNAMIC_DATE = Pattern.compile(
            "^\\d{4}-\\d{2}-\\d{2}(T\\d{2}:\\d{2}(:\\d{2}([.,]\\d{1,9})?)?(Z|[+-]\\d{2}(:?\\d{2})?)?)?$"
                    + "|^\\d{4}/\\d{2}/\\d{2}( \\d{2}:\\d{2}:\\d{2})?( [+-]\\d{4}| Z)?$");

    final JsonObject raw;
    final Map<String, Field> fields = new LinkedHashMap<>();
    final java.util.Set<String> nestedPaths = new java.util.LinkedHashSet<>();

    Mappings(JsonObject raw) {
        this.raw = raw == null ? new JsonObject() : raw;
        index(this.raw, "", null);
    }

    static Mappings empty() {
        return new Mappings(new JsonObject());
    }

    private void index(JsonObject node, String prefix, String nested) {
        if (!node.has("properties") || !node.get("properties").isJsonObject()) {
            return;
        }
        for (Map.Entry<String, JsonElement> e : node.getAsJsonObject("properties").entrySet()) {
            if (!e.getValue().isJsonObject()) {
                continue;
            }
            JsonObject def = e.getValue().getAsJsonObject();
            String path = prefix + e.getKey();
            String type = def.has("type") ? def.get("type").getAsString() : "object";
            Field f = new Field();
            f.path = path;
            f.type = type;
            f.def = def;
            f.nestedPath = nested;
            f.analyzer = def.has("analyzer") ? def.get("analyzer").getAsString() : null;
            f.searchAnalyzer = def.has("search_analyzer") ? def.get("search_analyzer").getAsString() : f.analyzer;
            f.format = def.has("format") ? def.get("format").getAsString() : null;
            f.indexed = !def.has("index") || def.get("index").getAsBoolean();
            if (type.equals("alias") && def.has("path")) {
                f.aliasTarget = def.get("path").getAsString();
            }
            fields.put(path, f);
            if (def.has("fields") && def.get("fields").isJsonObject()) {
                for (Map.Entry<String, JsonElement> sf : def.getAsJsonObject("fields").entrySet()) {
                    JsonObject sd = sf.getValue().getAsJsonObject();
                    Field s = new Field();
                    s.path = path + "." + sf.getKey();
                    s.type = sd.has("type") ? sd.get("type").getAsString() : "keyword";
                    s.def = sd;
                    s.subField = true;
                    s.nestedPath = nested;
                    s.analyzer = sd.has("analyzer") ? sd.get("analyzer").getAsString() : null;
                    s.searchAnalyzer = sd.has("search_analyzer") ? sd.get("search_analyzer").getAsString() : s.analyzer;
                    s.format = sd.has("format") ? sd.get("format").getAsString() : null;
                    s.indexed = !sd.has("index") || sd.get("index").getAsBoolean();
                    fields.put(s.path, s);
                }
            }
            if (type.equals("nested")) {
                nestedPaths.add(path);
                index(def, path + ".", path);
            } else if (type.equals("object") || def.has("properties")) {
                index(def, path + ".", nested);
            }
        }
    }

    Field get(String path) {
        Field f = fields.get(path);
        if (f != null && f.aliasTarget != null) {
            return fields.get(f.aliasTarget);
        }
        return f;
    }

    boolean isObjectPath(String path) {
        Field f = fields.get(path);
        return f != null && (f.type.equals("object") || f.type.equals("nested"));
    }

    String dynamicMode() {
        return raw.has("dynamic") ? raw.get("dynamic").getAsString() : "true";
    }

    // ---------- values ----------

    /** All values of {@code path} in the (sub)document, flattening arrays; also honours keys containing dots. */
    static void collect(JsonElement el, String path, List<JsonElement> out) {
        if (el == null || el.isJsonNull()) {
            return;
        }
        if (el.isJsonArray()) {
            for (JsonElement e : el.getAsJsonArray()) {
                collect(e, path, out);
            }
            return;
        }
        if (path.isEmpty()) {
            out.add(el);
            return;
        }
        if (!el.isJsonObject()) {
            return;
        }
        JsonObject o = el.getAsJsonObject();
        int dot = -1;
        while (true) {
            dot = path.indexOf('.', dot + 1);
            String head = dot < 0 ? path : path.substring(0, dot);
            if (o.has(head)) {
                collect(o.get(head), dot < 0 ? "" : path.substring(dot + 1), out);
            }
            if (dot < 0) {
                break;
            }
        }
    }

    static List<JsonElement> values(JsonObject doc, String path) {
        List<JsonElement> out = new ArrayList<>();
        collect(doc, path, out);
        return out;
    }

    // ---------- dynamic mapping ----------

    /** Validates {@code doc} against this mapping and returns the mapping including any dynamically added fields. */
    Mappings applyDocument(JsonObject doc, String docId, JsonObject indexSettings) {
        Walk w = new Walk(docId);
        w.visitObject(raw, "", doc, mode(raw, dynamicMode()), false);
        if (!w.needsUpdate) {
            return this;
        }
        JsonObject copy = raw.deepCopy();
        Walk w2 = new Walk(docId);
        w2.visitObject(copy, "", doc, mode(copy, dynamicMode()), true);
        return new Mappings(copy);
    }

    private static String mode(JsonObject node, String inherited) {
        if (node.has("dynamic")) {
            JsonElement d = node.get("dynamic");
            return d.isJsonPrimitive() ? d.getAsString() : inherited;
        }
        return inherited;
    }

    private final class Walk {
        final String docId;
        boolean needsUpdate;

        Walk(String docId) {
            this.docId = docId;
        }

        void visitObject(JsonObject node, String prefix, JsonObject obj, String dynMode, boolean write) {
            for (Map.Entry<String, JsonElement> e : obj.entrySet()) {
                String key = e.getKey();
                JsonElement v = e.getValue();
                if (v.isJsonNull() || (v.isJsonArray() && v.getAsJsonArray().isEmpty())) {
                    continue;
                }
                if (key.isEmpty() || key.trim().isEmpty()) {
                    throw causedBy(new OpenSearchException("mapper_parsing_exception", "failed to parse"),
                            "illegal_argument_exception", "field name cannot be an empty string");
                }
                if (key.chars().allMatch(ch -> ch == '.')) {
                    throw causedBy(new OpenSearchException("mapper_parsing_exception", "failed to parse"),
                            "illegal_argument_exception", "field name cannot contain only the character [.]");
                }
                if (key.contains(".")) {
                    // "a.b": 1 is shorthand for {"a": {"b": 1}}
                    JsonObject expanded = new JsonObject();
                    JsonObject cur = expanded;
                    String[] parts = key.split("\\.");
                    for (int i = 0; i < parts.length - 1; i++) {
                        if (parts[i].isEmpty()) {
                            throw causedBy(new OpenSearchException("mapper_parsing_exception", "failed to parse"),
                                    "illegal_argument_exception", "field name cannot contain only the character [.]");
                        }
                        JsonObject n = new JsonObject();
                        cur.add(parts[i], n);
                        cur = n;
                    }
                    cur.add(parts[parts.length - 1], v);
                    visitObject(node, prefix, expanded, dynMode, write);
                    continue;
                }
                JsonObject props = node.has("properties") ? node.getAsJsonObject("properties") : null;
                JsonObject def = props != null && props.has(key) && props.get(key).isJsonObject() ? props.getAsJsonObject(key) : null;
                String path = prefix + key;
                if (def == null) {
                    switch (dynMode) {
                        case "false" -> {
                            continue;
                        }
                        case "strict_allow_templates" -> {
                            if (matchTemplate(prefix + key, key, jsonTypeOf(v)) == null) {
                                throw new OpenSearchException("strict_dynamic_mapping_exception", "mapping set to strict_allow_templates, dynamic introduction of ["
                                        + key + "] within [" + (prefix.isEmpty() ? "_doc" : prefix.substring(0, prefix.length() - 1)) + "] is not allowed");
                            }
                        }
                        case "strict" -> throw new OpenSearchException("strict_dynamic_mapping_exception",
                                "mapping set to strict, dynamic introduction of [" + key + "] within ["
                                        + (prefix.isEmpty() ? "_doc" : prefix.substring(0, prefix.length() - 1)) + "] is not allowed");
                        default -> {
                        }
                    }
                    needsUpdate = true;
                    if (!write) {
                        continue;
                    }
                    if (props == null) {
                        props = new JsonObject();
                        node.add("properties", props);
                    }
                    def = inferField(node, path, key, v);
                    if (def == null) {
                        continue;
                    }
                    props.add(key, def);
                }
                visitValue(def, path, v, mode(node, dynMode), write);
            }
        }

        void visitValue(JsonObject def, String path, JsonElement v, String inheritedDyn, boolean write) {
            String type = def.has("type") ? def.get("type").getAsString() : "object";
            if (type.equals("object") || type.equals("nested") || (!def.has("type") && def.has("properties"))) {
                for (JsonElement el : flatten(v)) {
                    if (el.isJsonNull()) {
                        continue;
                    }
                    if (!el.isJsonObject()) {
                        throw new OpenSearchException("mapper_parsing_exception",
                                "object mapping for [" + path + "] tried to parse field [" + path.substring(path.lastIndexOf('.') + 1)
                                        + "] as object, but found a concrete value");
                    }
                    visitObject(def, path + ".", el.getAsJsonObject(), mode(def, inheritedDyn), write);
                }
                return;
            }
            if (type.equals("flat_object") || type.equals("alias")) {
                return;
            }
            boolean ignoreMalformed = def.has("ignore_malformed") && def.get("ignore_malformed").getAsBoolean();
            boolean coerce = !def.has("coerce") || def.get("coerce").getAsBoolean();
            if (type.equals("knn_vector") || type.equals("geo_point") || type.equals("geo_shape") || type.equals("percolator")
                    || type.equals("join") || type.equals("rank_features") || type.equals("binary")) {
                return;
            }
            for (JsonElement el : flatten(v)) {
                if (el.isJsonNull()) {
                    continue;
                }
                try {
                    validate(type, def, el, coerce);
                } catch (IllegalArgumentException ex) {
                    if (ignoreMalformed) {
                        continue;
                    }
                    String preview = el.isJsonPrimitive() ? el.getAsString() : el.toString();
                    OpenSearchException oe = new OpenSearchException("mapper_parsing_exception",
                            "failed to parse field [" + path + "] of type [" + type + "] in document with id '" + docId
                                    + "'. Preview of field's value: '" + preview + "'");
                    JsonObject cause = new JsonObject();
                    cause.addProperty("type", "illegal_argument_exception");
                    cause.addProperty("reason", ex.getMessage());
                    oe.extra.add("caused_by", cause);
                    throw oe;
                }
            }
        }
    }

    static OpenSearchException causedBy(OpenSearchException e, String type, String reason) {
        JsonObject cb = new JsonObject();
        cb.addProperty("type", type);
        cb.addProperty("reason", reason);
        e.extra.add("caused_by", cb);
        return e;
    }

    private static List<JsonElement> flatten(JsonElement v) {
        List<JsonElement> out = new ArrayList<>();
        flattenInto(v, out);
        return out;
    }

    private static void flattenInto(JsonElement v, List<JsonElement> out) {
        if (v.isJsonArray()) {
            for (JsonElement e : v.getAsJsonArray()) {
                flattenInto(e, out);
            }
        } else {
            out.add(v);
        }
    }

    static void validate(String type, JsonObject def, JsonElement el, boolean coerce) {
        if (el.isJsonObject() || el.isJsonArray()) {
            throw new IllegalArgumentException("Can't get text on a START_OBJECT");
        }
        JsonPrimitive p = el.getAsJsonPrimitive();
        switch (type) {
            case "long", "integer", "short", "byte", "unsigned_long" -> {
                if (p.isString() && !coerce) {
                    throw new IllegalArgumentException("Integer value passed as String");
                }
                BigDecimal bd = p.isNumber() ? p.getAsBigDecimal() : parseNumber(p.getAsString());
                if (p.isBoolean()) {
                    throw new IllegalArgumentException("boolean not numeric");
                }
                long min = type.equals("integer") ? Integer.MIN_VALUE : type.equals("short") ? Short.MIN_VALUE : type.equals("byte") ? Byte.MIN_VALUE : Long.MIN_VALUE;
                long max = type.equals("integer") ? Integer.MAX_VALUE : type.equals("short") ? Short.MAX_VALUE : type.equals("byte") ? Byte.MAX_VALUE : Long.MAX_VALUE;
                if (type.equals("unsigned_long")) {
                    if (bd.signum() < 0 || bd.compareTo(new BigDecimal("18446744073709551615")) > 0) {
                        throw new IllegalArgumentException("Value [" + bd.toPlainString() + "] is out of range for an unsigned long");
                    }
                    return;
                }
                BigDecimal trunc = bd.setScale(0, java.math.RoundingMode.DOWN);
                if (trunc.compareTo(BigDecimal.valueOf(min)) < 0 || trunc.compareTo(BigDecimal.valueOf(max)) > 0) {
                    throw new IllegalArgumentException("Numeric value (" + trunc.toPlainString() + ") out of range of "
                            + (type.equals("integer") ? "int" : type.equals("long") ? "long" : type) + " (" + min + " - " + max + ")");
                }
                if (!coerce && bd.stripTrailingZeros().scale() > 0) {
                    throw new IllegalArgumentException("Value [" + bd + "] has a decimal part");
                }
            }
            case "double", "float", "half_float", "scaled_float" -> {
                if (p.isBoolean()) {
                    throw new IllegalArgumentException("boolean not numeric");
                }
                if (p.isString()) {
                    if (!coerce) {
                        throw new IllegalArgumentException("numeric value passed as String");
                    }
                    parseNumber(p.getAsString());
                }
            }
            case "boolean" -> {
                if (p.isBoolean()) {
                    return;
                }
                String s = p.getAsString();
                if (p.isString() && (s.equals("true") || s.equals("false") || s.isEmpty())) {
                    return;
                }
                throw new IllegalArgumentException("Failed to parse value [" + s + "] as only [true] or [false] are allowed.");
            }
            case "date", "date_nanos" -> {
                if (p.isBoolean()) {
                    throw new IllegalArgumentException("failed to parse date field [" + p.getAsString() + "]");
                }
                String fmt = def.has("format") ? def.get("format").getAsString() : null;
                Dates.parse(p.getAsString(), fmt, ZoneOffset.UTC);
            }
            case "ip" -> {
                String s = p.getAsString();
                if (!s.matches("[0-9a-fA-F:.]+(/\\d+)?") || (!s.contains(":") && !s.matches("\\d{1,3}(\\.\\d{1,3}){3}"))) {
                    throw new IllegalArgumentException("'" + s + "' is not an IP string literal.");
                }
            }
            default -> {
            }
        }
    }

    static BigDecimal parseNumber(String s) {
        try {
            return new BigDecimal(s.trim());
        } catch (NumberFormatException e) {
            throw new NumberFormatException("For input string: \"" + s + "\"");
        }
    }

    private String jsonTypeOf(JsonElement v) {
        JsonElement first = null;
        for (JsonElement e : flatten(v)) {
            if (!e.isJsonNull()) {
                first = e;
                break;
            }
        }
        if (first == null || first.isJsonObject()) {
            return "object";
        }
        JsonPrimitive p = first.getAsJsonPrimitive();
        if (p.isBoolean()) {
            return "boolean";
        }
        if (p.isNumber()) {
            String s = p.getAsString();
            return s.contains(".") || s.contains("e") || s.contains("E") ? "double" : "long";
        }
        return DYNAMIC_DATE.matcher(p.getAsString()).matches() && dateOk(p.getAsString()) ? "date" : "string";
    }

    private JsonObject inferField(JsonObject parentNode, String path, String key, JsonElement v) {
        JsonElement first = null;
        for (JsonElement e : flatten(v)) {
            if (!e.isJsonNull()) {
                first = e;
                break;
            }
        }
        if (first == null) {
            return null;
        }
        String jsonType;
        JsonObject def = new JsonObject();
        if (first.isJsonObject()) {
            jsonType = "object";
        } else {
            JsonPrimitive p = first.getAsJsonPrimitive();
            if (p.isBoolean()) {
                jsonType = "boolean";
            } else if (p.isNumber()) {
                String s = p.getAsString();
                jsonType = s.contains(".") || s.contains("e") || s.contains("E") ? "double" : "long";
            } else if (raw.has("date_detection") && !raw.get("date_detection").getAsBoolean()) {
                jsonType = "string";
            } else if (DYNAMIC_DATE.matcher(p.getAsString()).matches() && dateOk(p.getAsString())) {
                jsonType = "date";
            } else if (raw.has("numeric_detection") && raw.get("numeric_detection").getAsBoolean()
                    && p.getAsString().matches("-?\\d+(\\.\\d+)?")) {
                jsonType = p.getAsString().contains(".") ? "double" : "long";
            } else {
                jsonType = "string";
            }
        }
        JsonObject templ = matchTemplate(path, key, jsonType);
        if (templ != null) {
            return templ;
        }
        switch (jsonType) {
            case "object" -> def.add("properties", new JsonObject());
            case "long" -> def.addProperty("type", "long");
            case "double" -> def.addProperty("type", "float");
            case "boolean" -> def.addProperty("type", "boolean");
            case "date" -> def.addProperty("type", "date");
            default -> {
                def.addProperty("type", "text");
                JsonObject fs = new JsonObject();
                JsonObject kw = new JsonObject();
                kw.addProperty("type", "keyword");
                kw.addProperty("ignore_above", 256);
                fs.add("keyword", kw);
                def.add("fields", fs);
            }
        }
        return def;
    }

    private static boolean dateOk(String s) {
        try {
            if (s.contains("/")) {
                return true;
            }
            Dates.parse(s, "strict_date_optional_time", ZoneOffset.UTC);
            return true;
        } catch (RuntimeException e) {
            return false;
        }
    }

    private JsonObject matchTemplate(String path, String key, String jsonType) {
        if (!raw.has("dynamic_templates")) {
            return null;
        }
        for (JsonElement te : raw.getAsJsonArray("dynamic_templates")) {
            for (Map.Entry<String, JsonElement> t : te.getAsJsonObject().entrySet()) {
                JsonObject spec = t.getValue().getAsJsonObject();
                boolean regex = spec.has("match_pattern") && spec.get("match_pattern").getAsString().equals("regex");
                if (spec.has("match_mapping_type")) {
                    String mmt = spec.get("match_mapping_type").getAsString();
                    boolean ok = mmt.equals("*") || mmt.equals(jsonType) || (mmt.equals("double") && jsonType.equals("double"));
                    if (!ok) {
                        continue;
                    }
                }
                if (spec.has("match") && !globMatch(spec.get("match").getAsString(), key, regex)) {
                    continue;
                }
                if (spec.has("unmatch") && globMatch(spec.get("unmatch").getAsString(), key, regex)) {
                    continue;
                }
                if (spec.has("path_match") && !globMatch(spec.get("path_match").getAsString(), path, regex)) {
                    continue;
                }
                if (spec.has("path_unmatch") && globMatch(spec.get("path_unmatch").getAsString(), path, regex)) {
                    continue;
                }
                if (!spec.has("mapping")) {
                    continue;
                }
                String m = spec.getAsJsonObject("mapping").toString().replace("{name}", key)
                        .replace("{dynamic_type}", jsonType.equals("string") ? "text" : jsonType);
                return com.google.gson.JsonParser.parseString(m).getAsJsonObject();
            }
        }
        return null;
    }

    static boolean globMatch(String pattern, String s, boolean regex) {
        if (regex) {
            return Pattern.compile(pattern).matcher(s).matches();
        }
        return wildcardMatch(pattern, s);
    }

    static boolean wildcardMatch(String pattern, String s) {
        StringBuilder sb = new StringBuilder();
        for (char c : pattern.toCharArray()) {
            if (c == '*') {
                sb.append(".*");
            } else if (c == '?') {
                sb.append('.');
            } else {
                sb.append(Pattern.quote(String.valueOf(c)));
            }
        }
        return Pattern.compile(sb.toString(), Pattern.DOTALL).matcher(s).matches();
    }

    // ---------- explicit mapping merge (PUT mapping / create index / templates) ----------

    /** Expands dotted property names ({@code "a.b": {...}}) into nested {@code properties}, validating names. */
    static JsonObject expandDots(JsonObject mapping) {
        JsonObject out = mapping.deepCopy();
        expandNode(out);
        return out;
    }

    private static void expandNode(JsonObject node) {
        if (!node.has("properties") || !node.get("properties").isJsonObject()) {
            return;
        }
        JsonObject props = node.getAsJsonObject("properties");
        JsonObject rebuilt = new JsonObject();
        for (Map.Entry<String, JsonElement> e : new ArrayList<>(props.entrySet())) {
            String name = e.getKey();
            if (name.trim().isEmpty()) {
                throw OpenSearchException.illegalArgument("name cannot be empty string");
            }
            JsonElement def = e.getValue();
            if (def.isJsonObject()) {
                expandNode(def.getAsJsonObject());
                if (def.getAsJsonObject().has("fields") && def.getAsJsonObject().get("fields").isJsonObject()) {
                    // multi-field definitions are not expanded further
                }
            }
            if (name.contains(".") && !name.chars().allMatch(ch -> ch == '.')) {
                String[] parts = name.split("\\.");
                JsonObject cur = rebuilt;
                for (int i = 0; i < parts.length - 1; i++) {
                    JsonObject holder = cur.has(parts[i]) && cur.get(parts[i]).isJsonObject() ? cur.getAsJsonObject(parts[i]) : new JsonObject();
                    if (!holder.has("properties")) {
                        holder.add("properties", new JsonObject());
                    }
                    cur.add(parts[i], holder);
                    cur = holder.getAsJsonObject("properties");
                }
                cur.add(parts[parts.length - 1], def);
            } else if (rebuilt.has(name) && rebuilt.get(name).isJsonObject() && def.isJsonObject()) {
                merge(rebuilt.getAsJsonObject(name), def.getAsJsonObject(), "");
            } else {
                rebuilt.add(name, def);
            }
        }
        node.add("properties", rebuilt);
    }

    /** Merges {@code update} into {@code base} in place, rejecting type changes like OpenSearch does. */
    static void merge(JsonObject base, JsonObject update, String prefix) {
        for (Map.Entry<String, JsonElement> e : update.entrySet()) {
            String k = e.getKey();
            JsonElement nv = e.getValue();
            if (k.equals("properties") && nv.isJsonObject()) {
                JsonObject bp = base.has("properties") ? base.getAsJsonObject("properties") : new JsonObject();
                base.add("properties", bp);
                for (Map.Entry<String, JsonElement> pe : nv.getAsJsonObject().entrySet()) {
                    String fname = prefix + pe.getKey();
                    if (bp.has(pe.getKey()) && pe.getValue().isJsonObject() && bp.get(pe.getKey()).isJsonObject()) {
                        JsonObject existing = bp.getAsJsonObject(pe.getKey());
                        JsonObject upd = pe.getValue().getAsJsonObject();
                        String et = existing.has("type") ? existing.get("type").getAsString() : "object";
                        String ut = upd.has("type") ? upd.get("type").getAsString() : (upd.has("properties") ? "object" : et);
                        if (!et.equals(ut)) {
                            throw OpenSearchException.illegalArgument("mapper [" + fname + "] cannot be changed from type [" + et
                                    + "] to [" + ut + "]");
                        }
                        for (String immutable : new String[] {"analyzer", "format", "index", "doc_values", "dims"}) {
                            if (existing.has(immutable) && upd.has(immutable) && !existing.get(immutable).equals(upd.get(immutable))) {
                                throw OpenSearchException.illegalArgument("Mapper for [" + fname + "] conflicts with existing mapper:\n\tCannot update parameter ["
                                        + immutable + "] from [" + existing.get(immutable).getAsString() + "] to [" + upd.get(immutable).getAsString() + "]");
                            }
                        }
                        merge(existing, upd, fname + ".");
                    } else {
                        bp.add(pe.getKey(), pe.getValue());
                    }
                }
            } else if (k.equals("fields") && nv.isJsonObject() && base.has("fields")) {
                JsonObject bf = base.getAsJsonObject("fields");
                for (Map.Entry<String, JsonElement> fe : nv.getAsJsonObject().entrySet()) {
                    bf.add(fe.getKey(), fe.getValue());
                }
            } else {
                base.add(k, nv);
            }
        }
    }
}
