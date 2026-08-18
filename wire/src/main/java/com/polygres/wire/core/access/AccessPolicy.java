package com.polygres.wire.core.access;

import java.util.List;
import java.util.regex.Pattern;

/**
 * End-user row/column access policy — see {@code docs/design/end-user-data-access-security.md}
 * §3.3. Modeled directly on Omni's {@code access_grants}/{@code access_filters} shape (field-level
 * grants keyed on a {@code user_attribute}, row filters that inject a {@code WHERE} predicate from
 * a matched attribute value), which is itself the closest existing pattern to what this codebase's
 * own {@code RouterStage}/{@code FirewallStage} rule lists already look like.
 */
public record AccessPolicy(List<ColumnGrant> columnGrants, List<RowFilter> rowFilters) {

    public AccessPolicy {
        columnGrants = columnGrants == null ? List.of() : List.copyOf(columnGrants);
        rowFilters = rowFilters == null ? List.of() : List.copyOf(rowFilters);
    }

    public static final AccessPolicy EMPTY = new AccessPolicy(List.of(), List.of());

    public boolean isEmpty() {
        return columnGrants.isEmpty() && rowFilters.isEmpty();
    }

    /** How a {@link ColumnGrant} that's violated is enforced — §3.3's {@code on_violation}. */
    public enum OnViolation { DENY, MASK }

    /**
     * A column (or set of columns) on tables matching {@code tablePattern} that's only visible
     * when {@code accessContext.attributes().get(requiredAttribute)} is one of
     * {@code allowedValues} — Omni's {@code required_access_grants}/{@code access_grants} shape.
     */
    public record ColumnGrant(Pattern tablePattern, List<String> columns, String requiredAttribute,
            List<String> allowedValues, OnViolation onViolation) {

        public ColumnGrant {
            if (tablePattern == null) {
                throw new IllegalArgumentException("ColumnGrant tablePattern must not be null");
            }
            if (columns == null || columns.isEmpty()) {
                throw new IllegalArgumentException("ColumnGrant columns must not be empty");
            }
            if (requiredAttribute == null || requiredAttribute.isBlank()) {
                throw new IllegalArgumentException("ColumnGrant requiredAttribute must not be blank");
            }
            columns = List.copyOf(columns);
            allowedValues = allowedValues == null ? List.of() : List.copyOf(allowedValues);
            onViolation = onViolation == null ? OnViolation.DENY : onViolation;
        }

        public boolean satisfiedBy(java.util.Map<String, String> attributes) {
            String actual = attributes.get(requiredAttribute);
            return actual != null && allowedValues.contains(actual);
        }
    }

    /**
     * A row-level filter on tables matching {@code tablePattern}: the value of
     * {@code accessContext.attributes().get(requiredAttribute)} is bound as
     * {@code filterColumn = ?} and appended to the statement's {@code WHERE} clause — Omni's
     * {@code access_filters} shape. {@code bypassRoles} — any role present on the
     * {@code AccessContext} skips the filter entirely (an admin/service account); Omni's
     * {@code values_for_unfiltered} plays the same role for attribute *values* rather than roles,
     * also supported here via {@code valuesForUnfiltered}.
     */
    public record RowFilter(Pattern tablePattern, String filterColumn, String requiredAttribute,
            List<String> bypassRoles, List<String> valuesForUnfiltered) {

        public RowFilter {
            if (tablePattern == null) {
                throw new IllegalArgumentException("RowFilter tablePattern must not be null");
            }
            if (filterColumn == null || filterColumn.isBlank()) {
                throw new IllegalArgumentException("RowFilter filterColumn must not be blank");
            }
            if (requiredAttribute == null || requiredAttribute.isBlank()) {
                throw new IllegalArgumentException("RowFilter requiredAttribute must not be blank");
            }
            bypassRoles = bypassRoles == null ? List.of() : List.copyOf(bypassRoles);
            valuesForUnfiltered = valuesForUnfiltered == null ? List.of() : List.copyOf(valuesForUnfiltered);
        }

        public boolean bypassedBy(com.polygres.wire.core.AccessContext accessContext) {
            if (accessContext.hasAnyRole(bypassRoles)) {
                return true;
            }
            String actual = accessContext.attributes().get(requiredAttribute);
            return actual != null && valuesForUnfiltered.contains(actual);
        }
    }
}
