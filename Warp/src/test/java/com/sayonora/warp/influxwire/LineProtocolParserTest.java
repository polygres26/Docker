package com.sayonora.warp.influxwire;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;

/** Line protocol: accepted syntax, value typing and the exact InfluxDB 1.x error texts. */
class LineProtocolParserTest {

    private static LineProtocolParser.Parsed parse(String body) {
        return LineProtocolParser.parseLenient(body, null);
    }

    @Test
    void fieldTypesAndBooleanSpellings() {
        InfluxPoint p = parse("m,t=1 f=1.5,i=2i,s=\"x y\",b=true,c=T,d=False 1000").points().get(0);
        assertEquals(1.5, p.fields().get("f"));
        assertEquals(2L, p.fields().get("i"));
        assertEquals("x y", p.fields().get("s"));
        assertEquals(true, p.fields().get("b"));
        assertEquals(true, p.fields().get("c"));
        assertEquals(false, p.fields().get("d"));
        assertEquals(1000L, p.timestampNanos());
        assertEquals("invalid boolean", messageOf("m v=tRUE 1"));
    }

    @Test
    void escapingInNamesTagsAndStrings() {
        InfluxPoint p = parse("we\\ ird\\,m,a\\ b=c\\,d,e\\=f=g v=1,\"k\\\\\"=\"q\\\"uote\\\\\" 5").points().get(0);
        assertEquals("we ird,m", p.measurement());
        assertEquals("c,d", p.tags().get("a b"));
        assertEquals("g", p.tags().get("e=f"));
        assertEquals(1.0, p.fields().get("v"));
    }

    @Test
    void precisionScalesTimestamps() {
        assertEquals(1_500_000_000L, LineProtocolParser.parseLenient("m v=1 1500", "ms").points().get(0).timestampNanos());
        assertEquals(60_000_000_000L, LineProtocolParser.parseLenient("m v=1 1", "m").points().get(0).timestampNanos());
        assertTrue(LineProtocolParser.validPrecision("ns") && LineProtocolParser.validPrecision("u") && !LineProtocolParser.validPrecision("us"));
    }

    private static String messageOf(String line) {
        LineProtocolParser.Parsed r = parse(line);
        assertFalse(r.errors().isEmpty(), line);
        return r.errors().get(0).replaceFirst("(?s)^unable to parse '.*?': ", "");
    }

    @Test
    void errorTextsMatchInfluxDb() {
        assertEquals("missing fields", messageOf("m"));
        assertEquals("invalid field format", messageOf("m v"));
        assertEquals("missing field value", messageOf("m v="));
        assertEquals("missing field key", messageOf("m =1"));
        assertEquals("missing tag value", messageOf("m,a v=1"));
        assertEquals("missing tag key", messageOf("m,=b v=1"));
        assertEquals("missing measurement", messageOf(",a=1 v=1"));
        assertEquals("unbalanced quotes", messageOf("m v=\"x"));
        assertEquals("bad timestamp", messageOf("m v=1 12a"));
        assertEquals("bad timestamp", messageOf("m v=1 1\r"));
        assertEquals("point is invalid", messageOf("m v=1 1 1"));
        assertEquals("duplicate tags", messageOf("m,a=1,a=2 v=1"));
        assertEquals("invalid number", messageOf("m v=1u 1"));
        assertEquals("invalid number", messageOf("m v=1.5.5 1"));
        assertEquals("invalid float", messageOf("m v=1e999 1"));
        assertEquals("invalid boolean", messageOf("m v=+1 1"));
        assertEquals("time outside range -9223372036854775806 - 9223372036854775806", messageOf("m v=1 9223372036854775807"));
        assertTrue(messageOf("m v=9223372036854775808i 1").startsWith("unable to parse integer 9223372036854775808"));
    }

    @Test
    void partialParsingKeepsTheGoodLines() {
        LineProtocolParser.Parsed r = parse("m v=1 1\n# comment\n\nbad\nm v=2 2\n");
        assertEquals(2, r.points().size());
        assertEquals(List.of("unable to parse 'bad': missing fields"), r.errors());
        assertEquals(1, parse("m s=\"multi\nline\" 1").points().size());
    }
}
