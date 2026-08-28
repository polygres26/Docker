package com.nexagres.advisor.sizing;

import java.util.List;

/** Output of {@link SizingCalculator} -- a starting-point Postgres instance shape, not a substitute for real load testing before go-live. */
public record SizingRecommendation(
    String tier,           // SMALL | MEDIUM | LARGE | XLARGE
    int vCpus,
    int memoryGB,
    int storageGB,
    int storageIops,
    int maxConnections,
    List<String> rationale,
    List<String> caveats
) {}
