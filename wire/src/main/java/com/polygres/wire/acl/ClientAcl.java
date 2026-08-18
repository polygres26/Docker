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
 * operator reading this config already knows that shape. No rules configured means "no ACL at
 * all" -- every connection allowed, identical to behavior before this feature existed.
 *
 * <p>Configured via {@code POLYWIRE_ACL_RULES} (bootstrap default) or {@code
 * polywire_config.aclRules} (hot-reloadable -- takes over once at least one row exists; see
 * {@code Main}'s config-apply callback), {@code ;}-separated, each entry {@code allow:<cidr>} or
 * {@code reject:<cidr>}, e.g. {@code "reject:10.0.0.0/8;allow:203.0.113.0/24;reject:0.0.0.0/0"} --
 * explicit trailing reject-all is recommended (same convention {@code pg_hba.conf} guides
 * recommend) since {@link #DEFAULT_ACTION_WHEN_RULES_CONFIGURED} only applies when there's at
 * least one rule but none of them matched.
 *
 * <p><b>{@link #DISABLED} is a plain default value, never mutated</b> -- {@link #reload} is only
 * ever called on an instance {@code Main} built explicitly for that purpose ({@link #parse}/
 * {@link #fromEnv} always return a fresh, independent instance, never the shared {@code DISABLED}
 * constant), so a caller using the {@code DISABLED} convenience default elsewhere in the codebase
 * (a minimal test setup, a legacy constructor overload) can never be surprised by a reload
 * originating from an unrelated part of the app. {@link #isAllowed} behavior only depends on
 * whether the current rule list is empty, never on object identity -- callers should stop
 * checking {@code acl == ClientAcl.DISABLED} for behavior (only ever for logging/display) since
 * that stops being meaningful once an instance can be reloaded after construction.
 */
public final class ClientAcl {

    private static final Logger log = LoggerFactory.getLogger(ClientAcl.class);

    /** A plain default value ("no ACL configured") -- see the class javadoc for why this is never itself reloaded. */
    public static final ClientAcl DISABLED = new ClientAcl(List.of());

    public enum Action { ALLOW, REJECT }

    public record Rule(Action action, Cidr cidr) {
    }

    // volatile, not final -- see AccessControlStage's identical "policy" field javadoc for why: a
    // reload (from Main's polywire_config LISTEN callback) and a concurrent connection's own read
    // both need to see a consistent, fully-built rule list, never a partially-applied one, without
    // a lock on the hot path.
    private volatile List<Rule> rules;
    // Applies only when rules is non-empty and none matched -- an operator who configured ANY
    // rules almost certainly wants a "deny by default" outcome for traffic none of their rules
    // anticipated, not silent allow; an empty rule list is the only case that allows everything
    // unconditionally.
    private static final boolean DEFAULT_ACTION_WHEN_RULES_CONFIGURED = false; // false = reject

    private ClientAcl(List<Rule> rules) {
        this.rules = rules;
    }

    public static ClientAcl fromEnv() {
        return parse(System.getenv("POLYWIRE_ACL_RULES"));
    }

    public static ClientAcl parse(String spec) {
        return new ClientAcl(parseRules(spec));
    }

    /** Swaps in a freshly-parsed rule list -- see {@code com.polygres.wire.config.FirewallRuleStore#listen}'s sibling pattern in {@code Main}'s config-apply callback. */
    public void reload(String spec) {
        this.rules = parseRules(spec);
        log.info("ClientAcl: reloaded {} rule(s)", this.rules.size());
    }

    private static List<Rule> parseRules(String spec) {
        if (spec == null || spec.isBlank()) {
            return List.of();
        }
        List<Rule> parsed = new ArrayList<>();
        for (String entry : spec.split(";")) {
            String trimmed = entry.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            int colon = trimmed.indexOf(':');
            if (colon <= 0) {
                throw new IllegalArgumentException("malformed ACL rules entry (expected allow:<cidr> or "
                        + "reject:<cidr>): " + trimmed);
            }
            String actionWord = trimmed.substring(0, colon).trim().toLowerCase(java.util.Locale.ROOT);
            Action action = switch (actionWord) {
                case "allow" -> Action.ALLOW;
                case "reject", "deny" -> Action.REJECT;
                default -> throw new IllegalArgumentException(
                        "malformed ACL rules entry (action must be allow/reject): " + trimmed);
            };
            Cidr cidr = Cidr.parse(trimmed.substring(colon + 1));
            parsed.add(new Rule(action, cidr));
        }
        return List.copyOf(parsed);
    }

    /** True if this instance currently has at least one rule configured -- for logging/display only, not a behavior gate (see class javadoc). */
    public boolean hasRules() {
        return !rules.isEmpty();
    }

    /** True if {@code address} should be allowed to connect. */
    public boolean isAllowed(InetAddress address) {
        List<Rule> currentRules = rules; // one volatile read -- see the field's javadoc
        if (currentRules.isEmpty()) {
            return true; // no rules configured, or a configured-but-empty spec
        }
        for (Rule rule : currentRules) {
            if (rule.cidr().contains(address)) {
                return rule.action() == Action.ALLOW;
            }
        }
        return DEFAULT_ACTION_WHEN_RULES_CONFIGURED;
    }
}
