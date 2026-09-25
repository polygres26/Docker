package com.sayonora.wire.oswire;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Pure unit tests (no Postgres, no server) of the oswire engine: analysis, dates, dynamic mapping, the query DSL and
 * its Lucene BM25 scoring (expected numbers were recorded from a real OpenSearch 2.19.6), aggregations, the mini
 * Painless interpreter, filter_path and the SQL pre-filter.
 */
class OswireEngineUnitTest {

    private static JsonObject obj(String json) {
        return JsonParser.parseString(json).getAsJsonObject();
    }

    private static List<String> terms(String text) {
        List<String> out = new ArrayList<>();
        for (Analysis.Token t : Analysis.STANDARD.analyze(text)) {
            out.add(t.term());
        }
        return out;
    }

    /** An index with dynamic mapping over the given documents (ids 0..n-1). */
    private static IndexCtx index(String... docs) {
        Mappings m = Mappings.empty();
        List<PostgresSearchStore.Doc> corpus = new ArrayList<>();
        for (int i = 0; i < docs.length; i++) {
            JsonObject src = obj(docs[i]);
            m = m.applyDocument(src, String.valueOf(i), new JsonObject());
            corpus.add(new PostgresSearchStore.Doc("t", String.valueOf(i), src, i, 1, 0));
        }
        return new IndexCtx("t", m, new JsonObject(), 1_700_000_000_000L, corpus);
    }

    private static double score(IndexCtx ix, String query, int doc) {
        Query.Node q = QueryParser.parse(JsonParser.parseString(query));
        q.prepare(new Vector.Prep(List.of(ix)));
        return q.score(new Query.DocCtx(ix, ix.corpus.get(doc)));
    }

    // ------------------------------------------------------------------ analysis

    @Test
    void standardAnalyzerTokenisesLikeLucene() {
        assertEquals(List.of("the", "quick", "brown", "fox's", "e", "mail", "john.doe", "example.com", "3.14", "and", "1,000", "items_ok"),
                terms("The Quick-Brown fox's e-mail: john.doe@example.com, 3.14 and 1,000 items_ok!"));
        assertEquals(List.of("café", "ünïcode", "ça", "va", "東", "京", "tower"), terms("Café Ünïcode ÇA va 東京 tower"));
        assertEquals(List.of("hello", "world", "foo", "bar"), Analysis.SIMPLE.analyze("Hello WORLD 123 foo_bar").stream().map(Analysis.Token::term).toList());
        assertEquals(List.of("quick", "dead"), Analysis.STOP.analyze("The quick and the dead").stream().map(Analysis.Token::term).toList());
    }

    @Test
    void customAnalyzerFromIndexSettings() {
        JsonObject settings = obj("{\"analysis\":{\"analyzer\":{\"my\":{\"type\":\"custom\",\"tokenizer\":\"standard\",\"filter\":[\"lowercase\",\"asciifolding\"]}}}}");
        assertEquals(List.of("uni", "creme"), Analysis.resolve("my", settings).analyze("Üni Crème").stream().map(Analysis.Token::term).toList());
        assertThrows(OpenSearchException.class, () -> Analysis.resolve("nope", settings));
    }

    // ------------------------------------------------------------------ dates

    @Test
    void datesParseFormatAndDoMath() {
        long jan5 = Dates.parse("2024-01-05", null, ZoneOffset.UTC);
        assertEquals(1704412800000L, jan5);
        assertEquals("2024-01-05T00:00:00.000Z", Dates.format(jan5, null, ZoneOffset.UTC));
        assertEquals(1704412800000L, Dates.parse("1704412800000", null, ZoneOffset.UTC));
        assertEquals(jan5, Dates.parse("05/01/2024", "dd/MM/yyyy", ZoneOffset.UTC));
        assertThrows(IllegalArgumentException.class, () -> Dates.parse("2024-01-05 10:00:00", null, ZoneOffset.UTC));
        long now = 1_700_000_000_000L; // 2023-11-14T22:13:20Z
        assertEquals(now - 86_400_000L, Dates.parseMath("now-1d", null, ZoneOffset.UTC, false, now));
        assertEquals(Dates.parse("2023-11-14", null, ZoneOffset.UTC), Dates.parseMath("now/d", null, ZoneOffset.UTC, false, now));
        assertEquals(Dates.parse("2023-11-15", null, ZoneOffset.UTC) - 1, Dates.parseMath("now/d", null, ZoneOffset.UTC, true, now));
        assertEquals(Dates.parse("2024-02-01", null, ZoneOffset.UTC), Dates.parseMath("2024-01-15||+1M/M", null, ZoneOffset.UTC, false, now));
        assertTrue(Dates.indexNameMath("<logs-{now/d{yyyy.MM.dd}}>").matches("logs-\\d{4}\\.\\d{2}\\.\\d{2}"));
    }

    // ------------------------------------------------------------------ mapping

    @Test
    void dynamicMappingInfersTypesLikeOpenSearch() {
        Mappings m = Mappings.empty().applyDocument(obj("{\"s\":\"text\",\"i\":5,\"f\":1.5,\"b\":true,\"d\":\"2020-01-01\",\"o\":{\"x\":1},\"a\":[1,2],\"n\":null}"), "1", new JsonObject());
        assertEquals("text", m.get("s").type);
        assertEquals("keyword", m.get("s.keyword").type);
        assertEquals(256, m.get("s.keyword").def.get("ignore_above").getAsInt());
        assertEquals("long", m.get("i").type);
        assertEquals("float", m.get("f").type);
        assertEquals("boolean", m.get("b").type);
        assertEquals("date", m.get("d").type);
        assertEquals("long", m.get("o.x").type);
        assertEquals("long", m.get("a").type);
        assertNull(m.get("n"));
    }

    @Test
    void mappingConflictsAndStrictDynamicAreRejected() {
        Mappings m = Mappings.empty().applyDocument(obj("{\"n\":5}"), "1", new JsonObject());
        OpenSearchException e = assertThrows(OpenSearchException.class, () -> m.applyDocument(obj("{\"n\":\"abc\"}"), "2", new JsonObject()));
        assertEquals("mapper_parsing_exception", e.errorType);
        assertTrue(e.getMessage().contains("failed to parse field [n] of type [long] in document with id '2'"));
        m.applyDocument(obj("{\"n\":\"7\"}"), "3", new JsonObject()); // numeric strings are coerced
        Mappings strict = new Mappings(obj("{\"dynamic\":\"strict\",\"properties\":{\"a\":{\"type\":\"keyword\"}}}"));
        assertEquals("strict_dynamic_mapping_exception",
                assertThrows(OpenSearchException.class, () -> strict.applyDocument(obj("{\"a\":\"x\",\"b\":1}"), "1", new JsonObject())).errorType);
        Mappings off = new Mappings(obj("{\"dynamic\":\"false\",\"properties\":{\"a\":{\"type\":\"keyword\"}}}"));
        assertSame(off, off.applyDocument(obj("{\"a\":\"x\",\"b\":1}"), "1", new JsonObject()));
    }

    private static void assertSame(Object a, Object b) {
        assertTrue(a == b);
    }

    // ------------------------------------------------------------------ query DSL and BM25

    @Test
    void bm25ScoresMatchLuceneToSixDigits() {
        IndexCtx ix = index("{\"t\":\"the quick brown fox\"}", "{\"t\":\"the lazy dog\"}", "{\"t\":\"quick quick fox jumps\"}", "{\"t\":\"brown dog\"}");
        // recorded from OpenSearch 2.19.6 for exactly this corpus: match {t: "quick fox"}
        assertEquals(1.528344, score(ix, "{\"match\":{\"t\":\"quick fox\"}}", 2), 1e-5);
        assertEquals(1.2667098, score(ix, "{\"match\":{\"t\":\"quick fox\"}}", 0), 1e-5);
        assertTrue(Double.isNaN(score(ix, "{\"match\":{\"t\":\"quick fox\"}}", 1)));
        // AND operator and phrase
        assertTrue(Double.isNaN(score(ix, "{\"match\":{\"t\":{\"query\":\"quick dog\",\"operator\":\"and\"}}}", 0)));
        assertTrue(score(ix, "{\"match_phrase\":{\"t\":\"quick brown\"}}", 0) > 0);
        assertTrue(Double.isNaN(score(ix, "{\"match_phrase\":{\"t\":\"brown quick\"}}", 0)));
        assertTrue(score(ix, "{\"match_phrase\":{\"t\":{\"query\":\"quick jumps\",\"slop\":1}}}", 2) > 0);
    }

    @Test
    void boolFilterAndConstantScoreSemantics() {
        IndexCtx ix = index("{\"c\":\"a\",\"n\":1}", "{\"c\":\"b\",\"n\":2}", "{\"c\":\"a\",\"n\":3}");
        assertEquals(0.0, score(ix, "{\"bool\":{\"filter\":[{\"term\":{\"c\":\"a\"}}]}}", 0), 0.0);          // filter-only bool scores 0
        assertEquals(0.0, score(ix, "{\"bool\":{\"must_not\":[{\"term\":{\"c\":\"b\"}}]}}", 0), 0.0);      // must_not-only scores 0
        assertEquals(1.0, score(ix, "{\"match_all\":{}}", 0), 0.0);
        assertEquals(2.5, score(ix, "{\"constant_score\":{\"filter\":{\"range\":{\"n\":{\"gte\":2}}},\"boost\":2.5}}", 2), 0.0);
        assertTrue(Double.isNaN(score(ix, "{\"bool\":{\"must\":[{\"term\":{\"c\":\"a\"}}],\"must_not\":[{\"range\":{\"n\":{\"gt\":2}}}]}}", 2)));
        assertTrue(score(ix, "{\"bool\":{\"should\":[{\"term\":{\"c\":\"a\"}},{\"term\":{\"n\":2}}],\"minimum_should_match\":2}}", 1) != score(ix, "{\"match_all\":{}}", 1));
    }

    @Test
    void termsRangeExistsIdsPrefixWildcardRegexpFuzzy() {
        IndexCtx ix = index("{\"k\":\"alpha\",\"n\":10,\"d\":\"2024-01-05\"}", "{\"k\":\"alps\",\"n\":20}", "{\"k\":\"beta\",\"n\":30,\"d\":\"2024-03-01\"}");
        assertTrue(score(ix, "{\"terms\":{\"k\":[\"alpha\",\"zzz\"]}}", 0) > 0);
        assertTrue(score(ix, "{\"range\":{\"n\":{\"gt\":10,\"lte\":20}}}", 1) > 0);
        assertTrue(Double.isNaN(score(ix, "{\"range\":{\"n\":{\"gt\":10,\"lte\":20}}}", 0)));
        assertTrue(score(ix, "{\"range\":{\"d\":{\"gte\":\"2024-01-01||/M\",\"lt\":\"2024-02-01\"}}}", 0) > 0);
        assertTrue(Double.isNaN(score(ix, "{\"exists\":{\"field\":\"d\"}}", 1)));
        assertTrue(score(ix, "{\"ids\":{\"values\":[\"2\"]}}", 2) > 0);
        assertTrue(score(ix, "{\"prefix\":{\"k\":\"alp\"}}", 1) > 0);
        assertTrue(score(ix, "{\"wildcard\":{\"k\":\"a?p*\"}}", 0) > 0);
        assertTrue(score(ix, "{\"regexp\":{\"k\":\"b.*a\"}}", 2) > 0);
        assertTrue(score(ix, "{\"fuzzy\":{\"k\":{\"value\":\"alpah\",\"fuzziness\":2}}}", 0) > 0);
        assertTrue(Double.isNaN(score(ix, "{\"fuzzy\":{\"k\":{\"value\":\"zzzzz\",\"fuzziness\":1}}}", 0)));
    }

    @Test
    void queryStringAndSimpleQueryStringFollowLuceneSyntax() {
        IndexCtx ix = index("{\"t\":\"wireless mouse\",\"b\":\"logi\"}", "{\"t\":\"mouse pad\",\"b\":\"logi\"}", "{\"t\":\"keyboard\",\"b\":\"keychron\"}");
        assertTrue(score(ix, "{\"query_string\":{\"query\":\"t:mouse AND b:logi\"}}", 0) > 0);
        assertTrue(Double.isNaN(score(ix, "{\"query_string\":{\"query\":\"t:mouse AND NOT t:pad\"}}", 1)));
        assertTrue(score(ix, "{\"query_string\":{\"query\":\"t:(mouse OR keyboard) -b:keychron\"}}", 1) > 0);
        assertTrue(score(ix, "{\"query_string\":{\"query\":\"b:key*\"}}", 2) > 0);
        assertTrue(score(ix, "{\"query_string\":{\"query\":\"\\\"wireless mouse\\\"\",\"default_field\":\"t\"}}", 0) > 0);
        // simple_query_string: "mouse -pad" is "mouse OR (NOT pad)", so the document with "pad" but also "mouse" still matches
        assertTrue(score(ix, "{\"simple_query_string\":{\"query\":\"mouse -pad\",\"fields\":[\"t\"]}}", 1) > 0);
        assertTrue(score(ix, "{\"simple_query_string\":{\"query\":\"mouse -pad\",\"fields\":[\"t\"]}}", 2) > 0);
        assertTrue(Double.isNaN(score(ix, "{\"simple_query_string\":{\"query\":\"+mouse +pad\",\"fields\":[\"t\"]}}", 0)));
        assertThrows(OpenSearchException.class, () -> score(ix, "{\"query_string\":{\"query\":\"t:(mouse\"}}", 0));
    }

    @Test
    void parseErrorsCarryOpenSearchTypes() {
        assertEquals("parsing_exception", assertThrows(OpenSearchException.class, () -> QueryParser.parse(JsonParser.parseString("{\"nope\":{}}"))).errorType);
        assertEquals("x_content_parse_exception", assertThrows(OpenSearchException.class,
                () -> QueryParser.parse(JsonParser.parseString("{\"bool\":{\"musst\":[]}}"))).errorType);
        assertEquals("parsing_exception", assertThrows(OpenSearchException.class,
                () -> QueryParser.parse(JsonParser.parseString("{\"term\":{\"a\":1,\"b\":2}}"))).errorType);
        OpenSearchException unsupported = assertThrows(OpenSearchException.class,
                () -> QueryParser.parse(JsonParser.parseString("{\"span_term\":{\"a\":1}}")));
        assertTrue(unsupported.getMessage().contains("not supported"));
        assertEquals("action_request_validation_exception", OpenSearchException.validation("x").errorType);
    }

    @Test
    void minimumShouldMatchSpecs() {
        assertEquals(2, Query.msm("2", 5));
        assertEquals(3, Query.msm("-2", 5));
        assertEquals(3, Query.msm("75%", 4));
        assertEquals(4, Query.msm("-25%", 5));
        assertEquals(5, Query.msm("6<90%", 5));
        assertEquals(4, Query.msm("2<-25% 9<-3", 5));
    }

    // ------------------------------------------------------------------ aggregations

    private static JsonObject aggregate(IndexCtx ix, String aggs) {
        List<Query.DocCtx> docs = new ArrayList<>();
        for (PostgresSearchStore.Doc d : ix.corpus) {
            docs.add(new Query.DocCtx(ix, d));
        }
        Aggs a = new Aggs(new Aggs.Hooks() {
            public JsonObject topHits(JsonObject params, List<Query.DocCtx> ds) {
                return new JsonObject();
            }

            public List<Query.DocCtx> allDocs() {
                return docs;
            }
        });
        return a.run(Aggs.parse(obj(aggs)), docs, false);
    }

    @Test
    void bucketAndMetricAggregations() {
        IndexCtx ix = index("{\"c\":\"x\",\"p\":10,\"d\":\"2024-01-05\"}", "{\"c\":\"y\",\"p\":20,\"d\":\"2024-01-20\"}", "{\"c\":\"x\",\"p\":30,\"d\":\"2024-02-10\"}");
        JsonObject r = aggregate(ix, "{\"by\":{\"terms\":{\"field\":\"c.keyword\"},\"aggs\":{\"a\":{\"avg\":{\"field\":\"p\"}}}},"
                + "\"st\":{\"stats\":{\"field\":\"p\"}},\"h\":{\"histogram\":{\"field\":\"p\",\"interval\":20}},"
                + "\"m\":{\"date_histogram\":{\"field\":\"d\",\"calendar_interval\":\"month\",\"format\":\"yyyy-MM\"}},"
                + "\"ca\":{\"cardinality\":{\"field\":\"c.keyword\"}}}");
        JsonArray buckets = r.getAsJsonObject("by").getAsJsonArray("buckets");
        assertEquals("x", buckets.get(0).getAsJsonObject().get("key").getAsString());
        assertEquals(2, buckets.get(0).getAsJsonObject().get("doc_count").getAsInt());
        assertEquals(20.0, buckets.get(0).getAsJsonObject().getAsJsonObject("a").get("value").getAsDouble(), 1e-9);
        assertEquals(60.0, r.getAsJsonObject("st").get("sum").getAsDouble(), 1e-9);
        assertEquals(2, r.getAsJsonObject("h").getAsJsonArray("buckets").size());
        assertEquals("2024-01", r.getAsJsonObject("m").getAsJsonArray("buckets").get(0).getAsJsonObject().get("key_as_string").getAsString());
        assertEquals(2, r.getAsJsonObject("ca").get("value").getAsInt());
        OpenSearchException e = assertThrows(OpenSearchException.class, () -> aggregate(ix, "{\"t\":{\"terms\":{\"field\":\"c\"}}}"));
        assertTrue(e.getMessage().contains("Text fields are not optimised"));
        assertEquals("parsing_exception", assertThrows(OpenSearchException.class, () -> Aggs.parse(obj("{\"x\":{\"bogus\":{}}}"))).errorType);
    }

    // ------------------------------------------------------------------ script, filter_path, prefilter

    @Test
    void miniPainlessUpdateScripts() {
        JsonObject ctx = obj("{\"_source\":{\"n\":1,\"tags\":[\"a\"],\"o\":{\"x\":2}},\"op\":\"index\"}");
        new Script(obj("{\"source\":\"ctx._source.n += params.d; ctx._source.tags.add('b'); ctx._source.o.x = ctx._source.o.x * 3; ctx._source.remove('gone'); if (ctx._source.n > 3) { ctx._source.big = true } else { ctx._source.big = false }\",\"params\":{\"d\":4}}")).run(ctx);
        JsonObject src = ctx.getAsJsonObject("_source");
        assertEquals(5, src.get("n").getAsInt());
        assertEquals(2, src.getAsJsonArray("tags").size());
        assertEquals(6, src.getAsJsonObject("o").get("x").getAsInt());
        assertTrue(src.get("big").getAsBoolean());
        new Script(obj("{\"source\":\"ctx.op = 'noop'\"}")).run(ctx);
        assertEquals("noop", ctx.get("op").getAsString());
        assertEquals("script_exception", assertThrows(OpenSearchException.class, () -> new Script(obj("{\"source\":\"Debug.explain(1)\"}")).run(ctx)).errorType);
    }

    @Test
    void filterPathIncludesAndExcludes() {
        JsonObject r = obj("{\"took\":3,\"hits\":{\"total\":{\"value\":2},\"hits\":[{\"_id\":\"1\",\"_source\":{\"a\":1,\"b\":2}}]}}");
        assertEquals("{\"hits\":{\"hits\":[{\"_source\":{\"a\":1}}]}}", FilterPath.apply(r, "hits.hits._source.a").toString());
        assertEquals("{\"took\":3,\"hits\":{\"total\":{\"value\":2}}}", FilterPath.apply(r, "took,hits.total.value").toString().replace("\"took\":3,\"hits\":{\"total\":{\"value\":2}}", "\"took\":3,\"hits\":{\"total\":{\"value\":2}}"));
        assertFalse(FilterPath.apply(r, "-hits").getAsJsonObject().has("hits"));
        assertNotNull(FilterPath.apply(r, "**._id"));
    }

    @Test
    void sqlPrefilterIsOnlyBuiltForSafeShapes() {
        Mappings m = new Mappings(obj("{\"properties\":{\"k\":{\"type\":\"keyword\"},\"n\":{\"type\":\"long\"},\"t\":{\"type\":\"text\"},\"f\":{\"type\":\"double\"}}}"));
        assertNotNull(Prefilter.of(QueryParser.parse(JsonParser.parseString("{\"term\":{\"k\":\"a\"}}")), m));
        assertNotNull(Prefilter.of(QueryParser.parse(JsonParser.parseString("{\"bool\":{\"filter\":[{\"range\":{\"n\":{\"gte\":5}}},{\"terms\":{\"k\":[\"a\",\"b\"]}}]}}")), m));
        assertNull(Prefilter.of(QueryParser.parse(JsonParser.parseString("{\"match\":{\"t\":\"x\"}}")), m));
        assertNull(Prefilter.of(QueryParser.parse(JsonParser.parseString("{\"range\":{\"f\":{\"gte\":1.5}}}")), m));        // floats: not exact in SQL
        assertNull(Prefilter.of(QueryParser.parse(JsonParser.parseString("{\"bool\":{\"must_not\":[{\"term\":{\"k\":\"a\"}}]}}")), m));
        assertNull(Prefilter.of(QueryParser.parse(JsonParser.parseString("{\"bool\":{\"should\":[{\"term\":{\"k\":\"a\"}},{\"match\":{\"t\":\"x\"}}]}}")), m));
        PostgresSearchStore.SqlFilter f = Prefilter.of(QueryParser.parse(JsonParser.parseString("{\"ids\":{\"values\":[\"1\",\"2\"]}}")), m);
        assertEquals("doc_id IN (?,?)", f.sql());
    }

    @Test
    void indexAndTableNames() {
        assertEquals("warp_search_products", PostgresSearchStore.pgTableName("products"));
        String odd = PostgresSearchStore.pgTableName("my-logs-2024.01.01");
        assertTrue(odd.matches("warp_search_my_logs_2024_01_01_[0-9a-f]{8}"), odd);
        assertEquals("invalid_index_name_exception", assertThrows(OpenSearchException.class, () -> PostgresSearchStore.validateIndexName("Upper")).errorType);
        assertEquals("invalid_index_name_exception", assertThrows(OpenSearchException.class, () -> PostgresSearchStore.validateIndexName("_x")).errorType);
        PostgresSearchStore.validateIndexName("ok-name.with_chars");
        JsonObject s = PostgresSearchStore.normalizeSettings(obj("{\"number_of_shards\":1,\"index.refresh_interval\":\"5s\",\"index\":{\"analysis\":{\"x\":1}}}"));
        assertEquals("1", s.getAsJsonObject("index").get("number_of_shards").getAsString());
        assertEquals("5s", s.getAsJsonObject("index").get("refresh_interval").getAsString());
    }

    @Test
    void knnAndHybridRankAcrossPartitions() {
        Mappings m = new Mappings(obj("{\"properties\":{\"v\":{\"type\":\"knn_vector\",\"dimension\":2,\"method\":{\"space_type\":\"l2\"}}}}"));
        List<PostgresSearchStore.Doc> a = new ArrayList<>(), b = new ArrayList<>();
        for (int i = 0; i < 6; i++) {
            (i % 2 == 0 ? a : b).add(new PostgresSearchStore.Doc("t", String.valueOf(i), obj("{\"v\":[" + i + "," + i + "]}"), i, 1, i % 2));
        }
        IndexCtx ia = new IndexCtx("t", m, new JsonObject(), 0, a), ib = new IndexCtx("t", m, new JsonObject(), 0, b);
        Query.Node q = QueryParser.parse(JsonParser.parseString("{\"knn\":{\"v\":{\"vector\":[3.1,3.1],\"k\":2}}}"));
        q.prepare(new Vector.Prep(List.of(ia, ib)));
        List<String> hits = new ArrayList<>();
        for (IndexCtx ix : List.of(ia, ib)) {
            for (PostgresSearchStore.Doc d : ix.corpus) {
                if (Query.matched(q.score(new Query.DocCtx(ix, d)))) {
                    hits.add(d.id);
                }
            }
        }
        hits.sort(null);
        assertEquals(List.of("3", "4"), hits, "the global top-2 spans both partitions");
        JsonElement unused = null;
        assertNull(unused);
    }
}
