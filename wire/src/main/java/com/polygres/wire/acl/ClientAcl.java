package com.polygres.wire.acl;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ClientAcl {

    private static final Logger log = LoggerFactory.getLogger(ClientAcl.class);

    public static final ClientAcl DISABLED = new ClientAcl(List.of());

    public enum Action { ALLOW, REJECT }

    public record Rule(Action action, Cidr cidr) {
    }

    private volatile List<Rule> rules;
    
    private static final boolean DEFAULT_ACTION_WHEN_RULES_CONFIGURED = false;

    private ClientAcl(List<Rule> rules) {
        this.rules = rules;
    }

    public static ClientAcl fromEnv() {
        return parse(System.getenv("POLYWIRE_ACL_RULES"));
    }

    public static ClientAcl parse(String spec) {
        return new ClientAcl(parseRules(spec));
    }

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

    public boolean hasRules() {
        return !rules.isEmpty();
    }

    public boolean isAllowed(InetAddress address) {
        List<Rule> currentRules = rules;
        if (currentRules.isEmpty()) {
            return true;
        }
        for (Rule rule : currentRules) {
            if (rule.cidr().contains(address)) {
                return rule.action() == Action.ALLOW;
            }
        }
        return DEFAULT_ACTION_WHEN_RULES_CONFIGURED;
    }
}
