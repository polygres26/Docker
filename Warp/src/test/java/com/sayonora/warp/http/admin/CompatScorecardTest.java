package com.sayonora.warp.http.admin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.junit.jupiter.api.Test;

/** Unit coverage for {@link CompatScorecard} -- reads the real, checked-in floci_compat baseline
 * JSON baked into {@code src/main/resources/compat-scorecard/} (manifest-driven, see that
 * package's javadoc), so this exercises the actual production data, not a synthetic fixture. */
class CompatScorecardTest {

    @Test
    void loadsARealNonEmptyScorecardFromTheBakedInBaselines() {
        JsonObject out = CompatScorecard.toJson();
        JsonArray scorecards = out.getAsJsonArray("scorecards");
        assertTrue(scorecards.size() > 10, "the manifest lists many (service, suite) pairs");
        assertFalse(out.get("source").getAsString().isBlank());
    }

    @Test
    void everyScorecardHasARealCurrentRunWithCounts() {
        for (var el : CompatScorecard.toJson().getAsJsonArray("scorecards")) {
            JsonObject card = el.getAsJsonObject();
            assertFalse(card.get("service").getAsString().isBlank());
            assertFalse(card.get("suite").getAsString().isBlank());
            JsonObject current = card.getAsJsonObject("current");
            JsonObject counts = current.getAsJsonObject("counts");
            assertTrue(counts.get("total").getAsInt() > 0, "a real captured run must have run at least one test");
        }
    }

    @Test
    void dynamoDbJavaShowsARealBeforeAfterImprovement() {
        JsonObject match = null;
        for (var el : CompatScorecard.toJson().getAsJsonArray("scorecards")) {
            JsonObject card = el.getAsJsonObject();
            if ("dynamodb".equals(card.get("service").getAsString()) && "java".equals(card.get("suite").getAsString())) {
                match = card;
                break;
            }
        }
        assertTrue(match != null, "dynamodb/java must be in the manifest");
        JsonObject baseline = match.getAsJsonObject("baseline");
        JsonObject current = match.getAsJsonObject("current");
        assertTrue(baseline != null, "dynamodb/java has two captured runs -- baseline and current");
        double baselineRate = baseline.getAsJsonObject("counts").get("passRate").getAsDouble();
        double currentRate = current.getAsJsonObject("counts").get("passRate").getAsDouble();
        assertTrue(currentRate > baselineRate, "the later run should show real improvement over the earlier baseline");
    }

    @Test
    void failuresCarryTheHarnesssOwnHeuristicClass() {
        for (var el : CompatScorecard.toJson().getAsJsonArray("scorecards")) {
            JsonObject card = el.getAsJsonObject();
            for (String side : new String[] { "current", "baseline" }) {
                JsonObject run = card.get(side).isJsonNull() ? null : card.getAsJsonObject(side);
                if (run == null) {
                    continue;
                }
                for (var f : run.getAsJsonArray("failures")) {
                    JsonObject failure = f.getAsJsonObject();
                    String cls = failure.get("class").getAsString();
                    assertTrue(cls.isEmpty() || "abcd".contains(cls), "unexpected heuristic class: " + cls);
                }
            }
        }
    }

    @Test
    void neverThrowsEvenIfCalledRepeatedly() {
        assertEquals(CompatScorecard.toJson().getAsJsonArray("scorecards").size(),
                CompatScorecard.toJson().getAsJsonArray("scorecards").size());
    }
}
