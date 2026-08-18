package com.polygres.wire.dynamowire;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class ItemPath {

    private static final Pattern SEGMENT = Pattern.compile("([^\\.\\[\\]]+)|\\[(\\d+)\\]");

    public final List<Object> segments = new ArrayList<>();

    private ItemPath() {}

    public static ItemPath parse(String rawPath, ExpressionContext ctx) {
        ItemPath p = new ItemPath();
        Matcher m = SEGMENT.matcher(rawPath);
        while (m.find()) {
            if (m.group(1) != null) {
                p.segments.add(ctx.resolveName(m.group(1)));
            } else {
                p.segments.add(Integer.parseInt(m.group(2)));
            }
        }
        if (p.segments.isEmpty()) throw new DynamoException("ValidationException", "Invalid path: " + rawPath);
        return p;
    }

    public String topLevelAttribute() {
        Object first = segments.get(0);
        if (!(first instanceof String s)) throw new DynamoException("ValidationException", "Path must start with an attribute name");
        return s;
    }

    public AttributeValue get(Map<String, AttributeValue> item) {
        Object cur = item;
        for (Object seg : segments) {
            if (seg instanceof String key) {
                if (!(cur instanceof Map<?, ?> mp)) return null;
                Object next = ((Map<?, ?>) mp).get(key);
                if (next == null) return null;
                cur = next;
            } else {
                int idx = (Integer) seg;
                AttributeValue av = asAv(cur);
                if (av == null || av.type != AttributeValue.Type.L || idx >= av.list.size()) return null;
                cur = av.list.get(idx);
            }
        }
        return asAv(cur);
    }

    private AttributeValue asAv(Object o) {
        if (o instanceof AttributeValue av) return av;
        return null;
    }

    @SuppressWarnings("unchecked")
    public void set(Map<String, AttributeValue> item, AttributeValue value) {
        Map<String, AttributeValue> curMap = item;
        List<AttributeValue> curList = null;
        for (int i = 0; i < segments.size() - 1; i++) {
            Object seg = segments.get(i);
            if (seg instanceof String key) {
                AttributeValue existing = curMap.get(key);
                if (existing == null || existing.type != AttributeValue.Type.M) {
                    existing = AttributeValue.ofM(new LinkedHashMap<>());
                    curMap.put(key, existing);
                }
                curMap = existing.map;
                curList = null;
            } else {
                int idx = (Integer) seg;
                if (curList == null) throw new DynamoException("ValidationException", "Cannot index into non-list at path segment " + i);
                AttributeValue existing = curList.get(idx);
                if (existing.type != AttributeValue.Type.M) throw new DynamoException("ValidationException", "Cannot descend into non-map list element");
                curMap = existing.map;
            }
        }
        Object last = segments.get(segments.size() - 1);
        if (last instanceof String key) {
            curMap.put(key, value);
        } else {
            throw new DynamoException("ValidationException", "SET into a bare list index at the top of a path is not supported");
        }
    }

    public void remove(Map<String, AttributeValue> item) {
        if (segments.size() == 1) {
            item.remove(segments.get(0));
            return;
        }
        
        Map<String, AttributeValue> curMap = item;
        for (int i = 0; i < segments.size() - 1; i++) {
            Object seg = segments.get(i);
            if (!(seg instanceof String key)) return;
            AttributeValue existing = curMap.get(key);
            if (existing == null || existing.type != AttributeValue.Type.M) return;
            curMap = existing.map;
        }
        Object last = segments.get(segments.size() - 1);
        if (last instanceof String key) curMap.remove(key);
    }
}
