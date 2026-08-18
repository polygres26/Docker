package com.polygres.wire.acl;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Ordered {@code allow}/{@code reject} CIDR rules, evaluated top-to-bottom, first match wins --
 * deliberately the same shape as {@code pg_hba.conf}'s own {@code host} record list rather than a
 * novel rule language, since PolyWire already sits directly in front of a real Postgres and every
 * operator reading this config already knows that shape. Unset ({@link #DISABLED}) means "no ACL
 * at all" -- every connection allowed, identical to behavior before this feature existed.
 *
 * <p>Configured via {@code POLYWIRE_ACL_RULES}, {@code ;}-separated, each entry
 * {@code allow:<cidr>} or {@code reject:<cidr>}, e.g.
 * {@code "reject:10.0.0.0/8;allow:203.0.113.0/24;reject:0.0.0.0/0"} -- explicit trailing
 * reject-all is recommended (same convention {@code pg_hba.conf} guides recommend) since
 * {@link #DEFAULT_ACTION} only applies when {@code POLYWIRE_ACL_RULES} has at least one rule but
 * none of them matched.
 */
public final class ClientAcl {

    private static final Logger log = LoggerFactory.getLogger(ClientAcl.class);

    /** No rules configured at all -- every connection allowed, the same as before this feature existed. */
    public static final ClientAcl DISABLED = new ClientAcl(List.of(), true);

    public enum Action { ALLOW, REJECT }

    public record Rule(Action action, Cidr cidr) {
    }

    private final List<Rule> rules;
    // Applies only when rules is non-empty and none matched -- an operator who configured ANY
    // rules almost certainly wants a "deny by default" outcome for traffic none of their rules
    // anticipated, not silent allow; DISABLED (no rules at all) is the only case that allows
    // everything unconditionally.
    private static final boolean DEFAULT_ACTION_WHEN_RULES_CONFIGURED = false; // false = reject

    private ClientAcl(List<Rule> rules, boolean allowAllNoRules) {
        this.rules = rules;
    }

    public static ClientAcl fromEnv() {
        return parse(System.getenv("POLYWIRE_ACL_RULES"));
    }

    public static ClientAcl parse(String spec) {
        if (spec == null || spec.isBlank()) {
            return DISABLED;
        }
        List<Rule> parsed = new ArrayList<>();
        for (String entry : spec.split(";")) {
            String trimmed = entry.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            int colon = trimmed.indexOf(':');
            if (colon <= 0) {
                throw new IllegalArgumentException("malformed POLYWIRE_ACL_RULES entry (expected allow:<cidr> or "
                        + "reject:<cidr>): " + trimmed);
            }
            String actionWord = trimmed.substring(0, colon).trim().toLowerCase(java.util.Locale.ROOT);
            Action action = switch (actionWord) {
                case "allow" -> Action.ALLOW;
                case "reject", "deny" -> Action.REJECT;
                default -> throw new IllegalArgumentException(
                        "malformed POLYWIRE_ACL_RULES entry (action must be allow/reject): " + trimmed);
            };
            Cidr cidr = Cidr.parse(trimmed.substring(colon + 1));
            parsed.add(new Rule(action, cidr));
        }
        log.info("ClientAcl: {} rule(s) loaded from POLYWIRE_ACL_RULES", parsed.size());
        return new ClientAcl(parsed, false);
    }

    /** True if {@code address} should be allowed to connect. */
    public boolean isAllowed(InetAddress address) {
        if (rules.isEmpty()) {
            return true; // DISABLED, or a configured-but-empty spec
        }
        for (Rule rule : rules) {
            if (rule.cidr().contains(address)) {
                return rule.action() == Action.ALLOW;
            }
        }
        return DEFAULT_ACTION_WHEN_RULES_CONFIGURED;
    }
}
