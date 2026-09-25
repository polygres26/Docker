package com.sayonora.wire.oswire;

import java.util.List;

/** Quantile / rank estimation over a value set (OpenSearch uses a t-digest; small sets are effectively exact interpolations). */
final class TDigest {

    private TDigest() {
    }

    static double quantile(List<Double> sorted, double q) {
        int n = sorted.size();
        if (n == 0) {
            return Double.NaN;
        }
        if (n == 1) {
            return sorted.get(0);
        }
        double index = q * n;
        if (index < 0.5) {
            return sorted.get(0);
        }
        if (index > n - 0.5) {
            return sorted.get(n - 1);
        }
        double pos = index - 0.5;
        int lo = (int) Math.floor(pos);
        int hi = Math.min(n - 1, lo + 1);
        double frac = pos - lo;
        return sorted.get(lo) + frac * (sorted.get(hi) - sorted.get(lo));
    }

    static double cdf(List<Double> sorted, double x) {
        int n = sorted.size();
        if (n == 0) {
            return Double.NaN;
        }
        if (x < sorted.get(0)) {
            return 0;
        }
        if (x > sorted.get(n - 1)) {
            return 1;
        }
        double below = 0;
        int equal = 0;
        for (double v : sorted) {
            if (v < x) {
                below++;
            } else if (v == x) {
                equal++;
            }
        }
        return (below + equal / 2.0) / n;
    }
}
