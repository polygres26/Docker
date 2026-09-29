package com.sayonora.warp.http.admin;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Compatibility scorecard for {@code GET /api/compat-scorecard} -- turns the real, third-party
 * floci SDK conformance suite results ({@code Warp/tests/python/floci_compat/}, the MIT-licensed
 * floci-io/floci test suites run against Warp's AWS wire protocols) into a published, per-protocol
 * pass-rate scorecard, instead of a developer-only script output that never left {@code tests/}.
 *
 * <p>Baked into the jar as classpath resources ({@code src/main/resources/compat-scorecard/*.json}
 * -- the exact files {@code run_floci_compat.py} writes, listed in {@code manifest.txt}) rather
 * than read from the filesystem at runtime: a deployed Warp process has no test checkout to read
 * from, and baking the data in makes the scorecard reproducible regardless of deployment
 * environment. Updating the scorecard means re-running the harness and re-copying its
 * {@code results/*.json} here -- this is deliberately static, versioned data, not a live probe.
 *
 * <p>For each (service, suite) pair there are exactly two captured runs from the same 2026-09-25
 * baseline session: an earlier one (before a round of fixes) and a later one (after). Rather than
 * trust the inconsistent {@code -baseline} filename convention (prefix for S3, suffix for every
 * other service), this groups runs by the JSON's own {@code service}/{@code suite} fields and picks
 * earliest-by-date as the baseline and latest-by-date as current -- robust to filename drift, and
 * exactly what a before/after regression-tracking scorecard needs.
 */
public final class CompatScorecard {

    private static final Logger log = LoggerFactory.getLogger(CompatScorecard.class);
    private static final String RESOURCE_DIR = "compat-scorecard/";

    private CompatScorecard() {
    }

    public record RunCounts(int pass, int fail, int error, int skip) {
        int total() {
            return pass + fail + error + skip;
        }

        double passRate() {
            int t = total();
            return t == 0 ? 0.0 : (double) pass / t;
        }
    }

    /** {@code heuristicClass}: a=not implemented, b=wrong behaviour/shape, c=floci-test-specific
     * (not a real Warp gap), d=environment -- the same heuristic {@code run_floci_compat.py}
     * itself assigns; a human pass can refine it (see the harness's own {@code results/*.md}). */
    public record Failure(String testFile, String test, String status, String message, String heuristicClass) {
    }

    public record Run(String service, String suite, String endpoint, String date, RunCounts counts, List<Failure> failures) {
    }

    /** {@code baseline} is {@code null} when only one run exists for this (service, suite) pair --
     * no before/after comparison is possible, not an error. */
    public record Scorecard(String service, String suite, Run baseline, Run current) {
    }

    public static JsonObject toJson() {
        JsonObject out = new JsonObject();
        JsonArray scorecards = new JsonArray();
        for (Scorecard c : load()) {
            JsonObject o = new JsonObject();
            o.addProperty("service", c.service());
            o.addProperty("suite", c.suite());
            o.add("current", runJson(c.current()));
            o.add("baseline", c.baseline() != c.current() ? runJson(c.baseline()) : null);
            scorecards.add(o);
        }
        out.add("scorecards", scorecards);
        out.addProperty("source", "Warp/tests/python/floci_compat -- floci-io/floci SDK compatibility "
                + "test suites (MIT), run against Warp's real AWS-service wire protocols");
        return out;
    }

    private static JsonObject runJson(Run r) {
        JsonObject o = new JsonObject();
        o.addProperty("endpoint", r.endpoint());
        o.addProperty("date", r.date());
        JsonObject counts = new JsonObject();
        counts.addProperty("pass", r.counts().pass());
        counts.addProperty("fail", r.counts().fail());
        counts.addProperty("error", r.counts().error());
        counts.addProperty("skip", r.counts().skip());
        counts.addProperty("total", r.counts().total());
        counts.addProperty("passRate", r.counts().passRate());
        o.add("counts", counts);
        JsonArray failures = new JsonArray();
        for (Failure f : r.failures()) {
            JsonObject fo = new JsonObject();
            fo.addProperty("testFile", f.testFile());
            fo.addProperty("test", f.test());
            fo.addProperty("status", f.status());
            fo.addProperty("message", f.message());
            fo.addProperty("class", f.heuristicClass());
            failures.add(fo);
        }
        o.add("failures", failures);
        return o;
    }

    private static List<Scorecard> load() {
        List<Run> runs = new ArrayList<>();
        for (String name : manifest()) {
            Run r = readRun(name);
            if (r != null) {
                runs.add(r);
            }
        }
        Map<String, List<Run>> byServiceSuite = new LinkedHashMap<>();
        for (Run r : runs) {
            byServiceSuite.computeIfAbsent(r.service() + "|" + r.suite(), k -> new ArrayList<>()).add(r);
        }
        List<Scorecard> out = new ArrayList<>();
        for (List<Run> group : byServiceSuite.values()) {
            group.sort((a, b) -> a.date().compareTo(b.date()));
            Run baseline = group.get(0);
            Run current = group.get(group.size() - 1);
            out.add(new Scorecard(current.service(), current.suite(), baseline, current));
        }
        out.sort((a, b) -> a.service().equals(b.service()) ? a.suite().compareTo(b.suite()) : a.service().compareTo(b.service()));
        return out;
    }

    private static List<String> manifest() {
        List<String> names = new ArrayList<>();
        try (InputStream in = CompatScorecard.class.getClassLoader().getResourceAsStream(RESOURCE_DIR + "manifest.txt")) {
            if (in == null) {
                log.warn("compat-scorecard: manifest.txt not found on classpath -- scorecard will be empty");
                return names;
            }
            for (String line : new String(in.readAllBytes(), StandardCharsets.UTF_8).split("\n")) {
                String trimmed = line.strip();
                if (!trimmed.isEmpty()) {
                    names.add(trimmed);
                }
            }
        } catch (IOException e) {
            log.warn("compat-scorecard: failed to read manifest.txt", e);
        }
        return names;
    }

    private static Run readRun(String resourceName) {
        try (InputStream in = CompatScorecard.class.getClassLoader().getResourceAsStream(RESOURCE_DIR + resourceName)) {
            if (in == null) {
                log.warn("compat-scorecard: resource {} listed in manifest but not found", resourceName);
                return null;
            }
            JsonObject j = JsonParser.parseString(new String(in.readAllBytes(), StandardCharsets.UTF_8)).getAsJsonObject();
            JsonObject countsJson = j.getAsJsonObject("counts");
            RunCounts counts = new RunCounts(
                    intOf(countsJson, "pass"), intOf(countsJson, "fail"), intOf(countsJson, "error"), intOf(countsJson, "skip"));
            List<Failure> failures = new ArrayList<>();
            if (j.has("failures")) {
                for (var el : j.getAsJsonArray("failures")) {
                    JsonObject fo = el.getAsJsonObject();
                    failures.add(new Failure(
                            strOf(fo, "file"), strOf(fo, "test"), strOf(fo, "status"), strOf(fo, "message"), strOf(fo, "class")));
                }
            }
            return new Run(strOf(j, "service"), strOf(j, "suite"), strOf(j, "endpoint"), strOf(j, "date"), counts, failures);
        } catch (Exception e) {
            log.warn("compat-scorecard: failed to parse resource {}", resourceName, e);
            return null;
        }
    }

    private static int intOf(JsonObject o, String key) {
        return o.has(key) && !o.get(key).isJsonNull() ? o.get(key).getAsInt() : 0;
    }

    private static String strOf(JsonObject o, String key) {
        return o.has(key) && !o.get(key).isJsonNull() ? o.get(key).getAsString() : "";
    }
}
