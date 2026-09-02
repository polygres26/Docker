package com.sayonora.wire.core.access;

import java.util.List;
import java.util.regex.Pattern;

public record AccessPolicy(List<ColumnGrant> columnGrants, List<RowFilter> rowFilters) {

    public AccessPolicy {
        columnGrants = columnGrants == null ? List.of() : List.copyOf(columnGrants);
        rowFilters = rowFilters == null ? List.of() : List.copyOf(rowFilters);
    }

    public static final AccessPolicy EMPTY = new AccessPolicy(List.of(), List.of());

    public boolean isEmpty() {
        return columnGrants.isEmpty() && rowFilters.isEmpty();
    }

    public enum OnViolation { DENY, MASK }

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

        public boolean bypassedBy(com.sayonora.wire.core.AccessContext accessContext) {
            if (accessContext.hasAnyRole(bypassRoles)) {
                return true;
            }
            String actual = accessContext.attributes().get(requiredAttribute);
            return actual != null && valuesForUnfiltered.contains(actual);
        }
    }
}
