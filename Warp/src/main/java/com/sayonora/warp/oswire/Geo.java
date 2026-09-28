package com.sayonora.warp.oswire;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

/** geo_point parsing and distance helpers. */
final class Geo {

    private Geo() {
    }

    /** {@code [lat, lon]} from an object {lat, lon}, [lon, lat] array, "lat,lon" string; null if unparsable. */
    static double[] point(JsonElement e) {
        try {
            if (e.isJsonObject()) {
                JsonObject o = e.getAsJsonObject();
                if (o.has("lat") && o.has("lon")) {
                    return new double[] {o.get("lat").getAsDouble(), o.get("lon").getAsDouble()};
                }
            } else if (e.isJsonArray() && e.getAsJsonArray().size() >= 2) {
                return new double[] {e.getAsJsonArray().get(1).getAsDouble(), e.getAsJsonArray().get(0).getAsDouble()};
            } else if (e.isJsonPrimitive()) {
                String[] p = e.getAsString().split(",");
                if (p.length == 2) {
                    return new double[] {Double.parseDouble(p[0].trim()), Double.parseDouble(p[1].trim())};
                }
            }
        } catch (RuntimeException ex) {
            return null;
        }
        return null;
    }

    static double haversine(double lat1, double lon1, double lat2, double lon2) {
        double r = 6371008.7714;
        double p1 = Math.toRadians(lat1), p2 = Math.toRadians(lat2);
        double dp = p2 - p1, dl = Math.toRadians(lon2 - lon1);
        double a = Math.sin(dp / 2) * Math.sin(dp / 2) + Math.cos(p1) * Math.cos(p2) * Math.sin(dl / 2) * Math.sin(dl / 2);
        return 2 * r * Math.asin(Math.min(1, Math.sqrt(a)));
    }

    static double meters(String v) {
        String s = v.trim().toLowerCase(java.util.Locale.ROOT);
        java.util.regex.Matcher m = java.util.regex.Pattern.compile("([0-9.eE+-]+)\\s*(mi|miles|yd|yards|ft|feet|in|inch|km|kilometers|nmi|NM|nauticalmiles|m|meters|cm|centimeters|mm|millimeters)?").matcher(s);
        if (!m.matches()) {
            throw OpenSearchException.illegalArgument("unable to parse distance [" + v + "]");
        }
        double n = Double.parseDouble(m.group(1));
        String u = m.group(2) == null ? "m" : m.group(2);
        return n * switch (u) {
            case "mi", "miles" -> 1609.344;
            case "yd", "yards" -> 0.9144;
            case "ft", "feet" -> 0.3048;
            case "in", "inch" -> 0.0254;
            case "km", "kilometers" -> 1000.0;
            case "nmi", "nm", "nauticalmiles" -> 1852.0;
            case "cm", "centimeters" -> 0.01;
            case "mm", "millimeters" -> 0.001;
            default -> 1.0;
        };
    }
}
