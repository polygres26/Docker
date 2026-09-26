package com.sayonora.warp.kafkawire;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The classic consumer group protocol (JoinGroup / SyncGroup / Heartbeat / LeaveGroup) for the groups this node coordinates. The
 * broker only coordinates: partition assignment (range, roundrobin, sticky, cooperative-sticky ...) is computed by the group leader client
 * and relayed verbatim. Group state machine: Empty -> PreparingRebalance -> CompletingRebalance -> Stable. In-flight state lives in memory
 * (a group is coordinated by exactly one node, chosen deterministically from the live broker list); a snapshot is persisted so
 * DescribeGroups/ListGroups survive a restart and generations keep increasing (members must rejoin, as after a Kafka coordinator move).
 */
final class GroupCoordinator {

    static final String EMPTY = "Empty";
    static final String PREPARING = "PreparingRebalance";
    static final String COMPLETING = "CompletingRebalance";
    static final String STABLE = "Stable";
    static final String DEAD = "Dead";

    record Proto(String name, byte[] metadata) {
    }

    static final class Member {
        String id;
        String instanceId;
        String clientId = "";
        String clientHost = "";
        int sessionMs;
        int rebalanceMs;
        String protocolType;
        List<Proto> protocols = List.of();
        byte[] assignment = new byte[0];
        long lastBeat;
        boolean joined;
        JoinResult result;

        byte[] metadataFor(String protocol) {
            for (Proto p : protocols) {
                if (p.name().equals(protocol)) {
                    return p.metadata();
                }
            }
            return new byte[0];
        }
    }

    record MemberInfo(String memberId, String instanceId, byte[] metadata) {
    }

    record JoinResult(int error, int generation, String protocolType, String protocolName, String leader, String memberId, List<MemberInfo> members) {
        static JoinResult error(int code, String memberId) {
            return new JoinResult(code, -1, null, null, "", memberId, List.of());
        }
    }

    record SyncResult(int error, String protocolType, String protocolName, byte[] assignment) {
    }

    static final class Group {
        final String id;
        String state = EMPTY;
        int generation;
        String protocolType;
        String protocolName;
        String leader;
        final Map<String, Member> members = new LinkedHashMap<>();
        final Map<String, Long> pending = new HashMap<>();
        long deadline;
        long notBefore;
        boolean dirty;

        Group(String id) {
            this.id = id;
        }
    }

    private final Map<String, Group> groups = new ConcurrentHashMap<>();
    private final GroupStore store;
    final int minSessionMs;
    final int maxSessionMs;
    final long initialDelayMs;

    GroupCoordinator(GroupStore store, int minSessionMs, int maxSessionMs, long initialDelayMs) {
        this.store = store;
        this.minSessionMs = minSessionMs;
        this.maxSessionMs = maxSessionMs;
        this.initialDelayMs = initialDelayMs;
    }

    /** The group, restoring generation/protocol from the persisted snapshot (with no members) on first touch. */
    Group group(String id, boolean create) {
        Group g = groups.get(id);
        if (g != null) {
            return g;
        }
        KafkaStore.GroupRow row = store.loadGroup(id);
        if (row == null && !create && !store.groupExists(id)) {
            return null;
        }
        Group n = new Group(id);
        if (row != null) {
            n.generation = row.generation();
            n.protocolType = row.protocolType().isEmpty() ? null : row.protocolType();
        }
        Group prev = groups.putIfAbsent(id, n);
        return prev != null ? prev : n;
    }

    /** Group ids this node knows in memory (for ListGroups). */
    List<Group> memoryGroups() {
        return new ArrayList<>(groups.values());
    }

    // ------------------------------------------------------------------ JoinGroup

    JoinResult join(String groupId, int sessionMs, int rebalanceMs, String memberId, String instanceId, String protocolType,
            List<Proto> protocols, String clientId, String clientHost, int version) {
        if (groupId == null || groupId.isEmpty()) {
            return JoinResult.error(KafkaError.INVALID_GROUP_ID, memberId);
        }
        Group g = group(groupId, true);
        synchronized (g) {
            long now = System.currentTimeMillis();
            if (sessionMs < minSessionMs || sessionMs > maxSessionMs) {
                return JoinResult.error(KafkaError.INVALID_SESSION_TIMEOUT, memberId);
            }
            if (protocolType == null || protocolType.isEmpty() || protocols.isEmpty()) {
                return JoinResult.error(KafkaError.INCONSISTENT_GROUP_PROTOCOL, memberId);
            }
            if (!g.members.isEmpty() && !supports(g, protocolType, protocols)) {
                return JoinResult.error(KafkaError.INCONSISTENT_GROUP_PROTOCOL, memberId);
            }
            String mid = memberId == null ? "" : memberId;
            Member m;
            if (instanceId != null) {
                Member byInst = null;
                for (Member x : g.members.values()) {
                    if (instanceId.equals(x.instanceId)) {
                        byInst = x;
                    }
                }
                if (!mid.isEmpty() && byInst != null && !byInst.id.equals(mid)) {
                    return JoinResult.error(KafkaError.FENCED_INSTANCE_ID, mid);
                }
                if (mid.isEmpty() && byInst != null) {
                    mid = byInst.id;
                }
            }
            if (mid.isEmpty()) {
                mid = (clientId == null || clientId.isEmpty() ? "member" : clientId) + "-" + UUID.randomUUID();
                if (version >= 4 && instanceId == null) {
                    g.pending.put(mid, now + sessionMs);
                    return JoinResult.error(KafkaError.MEMBER_ID_REQUIRED, mid);
                }
                m = null;
            } else {
                m = g.members.get(mid);
                if (m == null && g.pending.remove(mid) == null) {
                    return JoinResult.error(KafkaError.UNKNOWN_MEMBER_ID, mid);
                }
            }
            g.pending.remove(mid);
            boolean isNew = m == null;
            boolean protocolsChanged = false;
            if (isNew) {
                m = new Member();
                m.id = mid;
                g.members.put(mid, m);
            } else {
                protocolsChanged = !sameProtocols(m.protocols, protocols);
            }
            m.instanceId = instanceId;
            m.clientId = clientId == null ? "" : clientId;
            m.clientHost = clientHost;
            m.sessionMs = sessionMs;
            m.rebalanceMs = rebalanceMs;
            m.protocolType = protocolType;
            m.protocols = protocols;
            m.lastBeat = now;
            if (g.protocolType == null || g.protocolType.isEmpty() || g.members.size() == 1) {
                g.protocolType = protocolType;
            }
            switch (g.state) {
                case EMPTY -> {
                    g.notBefore = now + initialDelayMs;
                    prepare(g, now);
                }
                case STABLE, COMPLETING -> {
                    boolean leader = mid.equals(g.leader);
                    if (isNew || protocolsChanged || (g.state.equals(STABLE) && leader)) {
                        prepare(g, now);
                    } else {
                        return current(g, m); // a known member re-sending the same join: no rebalance
                    }
                }
                default -> {
                    // already preparing
                }
            }
            m.joined = true;
            m.result = null;
            g.dirty = true;
            tick(g, now);
            while (m.result == null && g.members.get(mid) == m) {
                try {
                    g.wait(50);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return JoinResult.error(KafkaError.REBALANCE_IN_PROGRESS, mid);
                }
                m.lastBeat = System.currentTimeMillis();
                tick(g, m.lastBeat);
            }
            if (m.result == null) {
                return JoinResult.error(KafkaError.UNKNOWN_MEMBER_ID, mid);
            }
            JoinResult r = m.result;
            m.result = null;
            return r;
        }
    }

    private JoinResult current(Group g, Member m) {
        List<MemberInfo> infos = new ArrayList<>();
        if (m.id.equals(g.leader)) {
            for (Member x : g.members.values()) {
                infos.add(new MemberInfo(x.id, x.instanceId, x.metadataFor(g.protocolName)));
            }
        }
        return new JoinResult(KafkaError.NONE, g.generation, g.protocolType, g.protocolName, g.leader, m.id, infos);
    }

    private static boolean supports(Group g, String protocolType, List<Proto> protocols) {
        if (g.protocolType != null && !g.protocolType.equals(protocolType)) {
            return false;
        }
        for (Proto p : protocols) {
            boolean all = true;
            for (Member x : g.members.values()) {
                boolean has = false;
                for (Proto q : x.protocols) {
                    has |= q.name().equals(p.name());
                }
                all &= has;
            }
            if (all) {
                return true;
            }
        }
        return false;
    }

    private static boolean sameProtocols(List<Proto> a, List<Proto> b) {
        if (a.size() != b.size()) {
            return false;
        }
        for (int i = 0; i < a.size(); i++) {
            if (!a.get(i).name().equals(b.get(i).name()) || !java.util.Arrays.equals(a.get(i).metadata(), b.get(i).metadata())) {
                return false;
            }
        }
        return true;
    }

    /** Enters PreparingRebalance: every member must rejoin before the (longest) rebalance timeout. */
    private void prepare(Group g, long now) {
        int longest = 0;
        for (Member x : g.members.values()) {
            x.joined = false;
            x.result = null;
            longest = Math.max(longest, x.rebalanceMs);
        }
        g.state = PREPARING;
        g.deadline = now + Math.max(longest, 1);
        g.dirty = true;
        g.notifyAll();
    }

    /** Completes a rebalance when everyone rejoined (or the deadline passed), expires dead members. Caller holds the group lock. */
    void tick(Group g, long now) {
        // session expiry
        List<Member> expired = null;
        for (Member x : g.members.values()) {
            if (!x.joined && now - x.lastBeat > x.sessionMs) {
                if (expired == null) {
                    expired = new ArrayList<>();
                }
                expired.add(x);
            }
        }
        if (expired != null) {
            for (Member x : expired) {
                remove(g, x, now);
            }
        }
        g.pending.values().removeIf(t -> t < now);
        if (g.state.equals(PREPARING)) {
            boolean all = true;
            for (Member x : g.members.values()) {
                all &= x.joined;
            }
            if ((all && now >= g.notBefore) || now >= g.deadline) {
                completeJoin(g, now);
            }
        } else if (g.state.equals(COMPLETING) && now >= g.deadline) {
            // the leader never synced: rebalance again
            for (Member x : g.members.values()) {
                x.joined = false;
            }
            prepare(g, now);
        }
    }

    private void completeJoin(Group g, long now) {
        g.members.values().removeIf(x -> !x.joined);
        g.generation++;
        g.dirty = true;
        if (g.members.isEmpty()) {
            g.state = EMPTY;
            g.leader = null;
            g.protocolName = null;
            g.notifyAll();
            return;
        }
        // protocol: supported by all members, most votes among each member's own preference order
        List<String> candidates = new ArrayList<>();
        Member first = g.members.values().iterator().next();
        for (Proto p : first.protocols) {
            boolean all = true;
            for (Member x : g.members.values()) {
                boolean has = false;
                for (Proto q : x.protocols) {
                    has |= q.name().equals(p.name());
                }
                all &= has;
            }
            if (all) {
                candidates.add(p.name());
            }
        }
        // each member votes for its most preferred candidate; the winner is the first maximum in java.util.HashMap iteration order, which is
        // what Kafka's coordinator does (ties between protocols therefore resolve the same way as on a real broker)
        Map<String, Integer> votes = new HashMap<>();
        for (Member x : g.members.values()) {
            for (Proto p : x.protocols) {
                if (candidates.contains(p.name())) {
                    votes.merge(p.name(), 1, Integer::sum);
                    break;
                }
            }
        }
        String best = candidates.isEmpty() ? first.protocols.get(0).name() : null;
        int bv = -1;
        for (Map.Entry<String, Integer> e : votes.entrySet()) {
            if (e.getValue() > bv) {
                bv = e.getValue();
                best = e.getKey();
            }
        }
        g.protocolName = best;
        if (g.leader == null || !g.members.containsKey(g.leader)) {
            g.leader = g.members.keySet().iterator().next();
        }
        g.state = COMPLETING;
        int longest = 0;
        for (Member x : g.members.values()) {
            longest = Math.max(longest, x.rebalanceMs);
        }
        g.deadline = now + Math.max(longest, 1);
        for (Member x : g.members.values()) {
            x.joined = false;
            x.lastBeat = now;
            x.result = current(g, x);
        }
        g.notifyAll();
    }

    /** Removes a member (leave or expiry): a rebalance follows. Caller holds the group lock. */
    private void remove(Group g, Member m, long now) {
        g.members.remove(m.id);
        g.dirty = true;
        if (g.members.isEmpty()) {
            g.state = EMPTY;
            g.leader = null;
            g.protocolName = null;
            g.generation++;
        } else if (g.state.equals(PREPARING)) {
            // the remaining members may all have rejoined already: tick decides
            if (m.id.equals(g.leader)) {
                g.leader = null;
            }
        } else {
            if (m.id.equals(g.leader)) {
                g.leader = null;
            }
            prepare(g, now);
        }
        g.notifyAll();
    }

    // ------------------------------------------------------------------ SyncGroup

    SyncResult sync(String groupId, int generation, String memberId, String protocolType, String protocolName, Map<String, byte[]> assignments) {
        Group g = group(groupId, false);
        if (g == null) {
            return new SyncResult(KafkaError.UNKNOWN_MEMBER_ID, null, null, new byte[0]);
        }
        synchronized (g) {
            long now = System.currentTimeMillis();
            tick(g, now);
            Member m = g.members.get(memberId);
            if (m == null) {
                return new SyncResult(KafkaError.UNKNOWN_MEMBER_ID, null, null, new byte[0]);
            }
            if (generation != g.generation) {
                return new SyncResult(KafkaError.ILLEGAL_GENERATION, null, null, new byte[0]);
            }
            if (protocolType != null && !protocolType.equals(g.protocolType) || protocolName != null && !protocolName.equals(g.protocolName)) {
                return new SyncResult(KafkaError.INCONSISTENT_GROUP_PROTOCOL, null, null, new byte[0]);
            }
            switch (g.state) {
                case EMPTY, DEAD -> {
                    return new SyncResult(KafkaError.UNKNOWN_MEMBER_ID, null, null, new byte[0]);
                }
                case PREPARING -> {
                    return new SyncResult(KafkaError.REBALANCE_IN_PROGRESS, null, null, new byte[0]);
                }
                case COMPLETING -> {
                    m.lastBeat = now;
                    if (memberId.equals(g.leader)) {
                        for (Member x : g.members.values()) {
                            byte[] a = assignments.get(x.id);
                            x.assignment = a == null ? new byte[0] : a;
                        }
                        g.state = STABLE;
                        g.dirty = true;
                        g.notifyAll();
                    } else {
                        while (g.state.equals(COMPLETING) && g.members.get(memberId) == m && generation == g.generation) {
                            try {
                                g.wait(50);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                break;
                            }
                            m.lastBeat = System.currentTimeMillis();
                            tick(g, m.lastBeat);
                        }
                        if (!g.state.equals(STABLE) || g.generation != generation || g.members.get(memberId) != m) {
                            return new SyncResult(g.members.get(memberId) == null ? KafkaError.UNKNOWN_MEMBER_ID : KafkaError.REBALANCE_IN_PROGRESS,
                                    null, null, new byte[0]);
                        }
                    }
                    return new SyncResult(KafkaError.NONE, g.protocolType, g.protocolName, m.assignment);
                }
                default -> {
                    m.lastBeat = now;
                    return new SyncResult(KafkaError.NONE, g.protocolType, g.protocolName, m.assignment);
                }
            }
        }
    }

    // ------------------------------------------------------------------ Heartbeat / LeaveGroup

    int heartbeat(String groupId, int generation, String memberId, String instanceId) {
        Group g = group(groupId, false);
        if (g == null) {
            return KafkaError.UNKNOWN_MEMBER_ID;
        }
        synchronized (g) {
            long now = System.currentTimeMillis();
            tick(g, now);
            if (g.state.equals(EMPTY) || g.state.equals(DEAD)) {
                return KafkaError.UNKNOWN_MEMBER_ID;
            }
            Member m = g.members.get(memberId);
            if (m == null) {
                return KafkaError.UNKNOWN_MEMBER_ID;
            }
            if (instanceId != null && !instanceId.equals(m.instanceId)) {
                return KafkaError.FENCED_INSTANCE_ID;
            }
            if (generation != g.generation) {
                return KafkaError.ILLEGAL_GENERATION;
            }
            m.lastBeat = now;
            return g.state.equals(PREPARING) ? KafkaError.REBALANCE_IN_PROGRESS : KafkaError.NONE;
        }
    }

    /** @return the error of one member leaving; NONE when removed */
    int leave(String groupId, String memberId, String instanceId) {
        Group g = group(groupId, false);
        if (g == null) {
            return KafkaError.UNKNOWN_MEMBER_ID;
        }
        synchronized (g) {
            long now = System.currentTimeMillis();
            Member m = g.members.get(memberId);
            if (m == null && instanceId != null) {
                for (Member x : g.members.values()) {
                    if (instanceId.equals(x.instanceId)) {
                        m = x;
                    }
                }
            }
            if (m == null) {
                return g.pending.remove(memberId) != null ? KafkaError.NONE : KafkaError.UNKNOWN_MEMBER_ID;
            }
            if (instanceId != null && !instanceId.equals(m.instanceId)) {
                return KafkaError.FENCED_INSTANCE_ID;
            }
            remove(g, m, now);
            tick(g, now);
            return KafkaError.NONE;
        }
    }

    // ------------------------------------------------------------------ offset commit validation, describe, delete

    /** Validates who may commit offsets for the group; NONE when the commit is allowed. */
    int validateCommit(String groupId, int generation, String memberId, String instanceId) {
        Group g = group(groupId, false);
        String mid = memberId == null ? "" : memberId;
        if (g == null || g.state.equals(EMPTY)) {
            if (generation < 0 && mid.isEmpty() && instanceId == null) {
                return KafkaError.NONE;
            }
            if (g == null) {
                return KafkaError.UNKNOWN_MEMBER_ID;
            }
        }
        synchronized (g) {
            tick(g, System.currentTimeMillis());
            if (g.state.equals(EMPTY) && generation < 0 && mid.isEmpty() && instanceId == null) {
                return KafkaError.NONE;
            }
            if (generation >= 0 || !mid.isEmpty() || instanceId != null) {
                Member m = g.members.get(mid);
                if (m == null) {
                    return KafkaError.UNKNOWN_MEMBER_ID;
                }
                if (instanceId != null && !instanceId.equals(m.instanceId)) {
                    return KafkaError.FENCED_INSTANCE_ID;
                }
                if (generation != g.generation) {
                    return KafkaError.ILLEGAL_GENERATION;
                }
                if (g.state.equals(COMPLETING)) {
                    return KafkaError.REBALANCE_IN_PROGRESS;
                }
                m.lastBeat = System.currentTimeMillis();
                return KafkaError.NONE;
            }
            return KafkaError.UNKNOWN_MEMBER_ID;
        }
    }

    /** Snapshot for DescribeGroups. */
    Group snapshot(String groupId) {
        Group g = group(groupId, false);
        if (g == null) {
            return null;
        }
        synchronized (g) {
            tick(g, System.currentTimeMillis());
            Group c = new Group(g.id);
            c.state = g.state;
            c.generation = g.generation;
            c.protocolType = g.protocolType;
            c.protocolName = g.protocolName;
            c.leader = g.leader;
            c.members.putAll(g.members);
            return c;
        }
    }

    int delete(String groupId) {
        Group g = group(groupId, false);
        if (g != null) {
            synchronized (g) {
                tick(g, System.currentTimeMillis());
                if (!g.members.isEmpty()) {
                    return KafkaError.NON_EMPTY_GROUP;
                }
                g.state = DEAD;
                groups.remove(groupId);
            }
        }
        boolean existed = store.deleteGroup(groupId);
        return g == null && !existed ? KafkaError.GROUP_ID_NOT_FOUND : KafkaError.NONE;
    }

    /** Periodic maintenance: session expiry, rebalance deadlines, persisting changed groups. */
    void sweep() {
        long now = System.currentTimeMillis();
        for (Group g : groups.values()) {
            KafkaStore.GroupRow row = null;
            synchronized (g) {
                tick(g, now);
                if (g.dirty) {
                    g.dirty = false;
                    row = row(g);
                }
            }
            if (row != null) {
                try {
                    store.saveGroup(row);
                } catch (RuntimeException e) {
                    synchronized (g) {
                        g.dirty = true;
                    }
                }
            }
        }
    }

    private static KafkaStore.GroupRow row(Group g) {
        JsonArray arr = new JsonArray();
        for (Member m : g.members.values()) {
            JsonObject o = new JsonObject();
            o.addProperty("id", m.id);
            o.addProperty("instance", m.instanceId);
            o.addProperty("client", m.clientId);
            o.addProperty("host", m.clientHost);
            o.addProperty("assignment", Base64.getEncoder().encodeToString(m.assignment));
            arr.add(o);
        }
        return new KafkaStore.GroupRow(g.id, g.state, g.protocolType == null ? "" : g.protocolType, g.protocolName == null ? "" : g.protocolName,
                g.generation, g.leader == null ? "" : g.leader, arr.toString());
    }
}
