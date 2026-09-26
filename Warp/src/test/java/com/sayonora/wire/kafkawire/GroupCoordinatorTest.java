package com.sayonora.wire.kafkawire;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class GroupCoordinatorTest {

    static final class MemStore implements GroupStore {
        final Map<String, KafkaStore.GroupRow> rows = new ConcurrentHashMap<>();

        @Override
        public KafkaStore.GroupRow loadGroup(String id) {
            return rows.get(id);
        }

        @Override
        public void saveGroup(KafkaStore.GroupRow row) {
            rows.put(row.id(), row);
        }

        @Override
        public boolean deleteGroup(String id) {
            return rows.remove(id) != null;
        }

        @Override
        public boolean groupExists(String id) {
            return rows.containsKey(id);
        }
    }

    private static List<GroupCoordinator.Proto> protos(String... names) {
        return java.util.Arrays.stream(names).map(n -> new GroupCoordinator.Proto(n, ("meta-" + n).getBytes(StandardCharsets.UTF_8))).toList();
    }

    private static GroupCoordinator.JoinResult join(GroupCoordinator gc, String group, String member, int version, String... protocols) {
        return gc.join(group, 10000, 10000, member, null, "consumer", protos(protocols), "client", "/127.0.0.1", version);
    }

    @Test
    void singleMemberLifecycle() {
        GroupCoordinator gc = new GroupCoordinator(new MemStore(), 100, 60000, 0);
        GroupCoordinator.JoinResult first = join(gc, "g", "", 5, "range");
        assertEquals(KafkaError.MEMBER_ID_REQUIRED, first.error());
        assertNotNull(first.memberId());
        GroupCoordinator.JoinResult j = join(gc, "g", first.memberId(), 5, "range");
        assertEquals(0, j.error());
        assertEquals(1, j.generation());
        assertEquals("range", j.protocolName());
        assertEquals(first.memberId(), j.leader());
        assertEquals(1, j.members().size());
        assertEquals(GroupCoordinator.COMPLETING, gc.snapshot("g").state);
        assertEquals(0, gc.heartbeat("g", 1, j.memberId(), null));
        byte[] a = {1, 2, 3};
        GroupCoordinator.SyncResult s = gc.sync("g", 1, j.memberId(), "consumer", "range", Map.of(j.memberId(), a));
        assertEquals(0, s.error());
        assertArrayEquals(a, s.assignment());
        assertEquals(GroupCoordinator.STABLE, gc.snapshot("g").state);
        assertEquals(KafkaError.ILLEGAL_GENERATION, gc.heartbeat("g", 9, j.memberId(), null));
        assertEquals(KafkaError.UNKNOWN_MEMBER_ID, gc.heartbeat("g", 1, "ghost", null));
        assertEquals(KafkaError.ILLEGAL_GENERATION, gc.sync("g", 4, j.memberId(), null, null, Map.of()).error());
        assertEquals(0, gc.leave("g", j.memberId(), null));
        assertEquals(GroupCoordinator.EMPTY, gc.snapshot("g").state);
        assertEquals(KafkaError.UNKNOWN_MEMBER_ID, gc.heartbeat("g", 1, j.memberId(), null));
        assertEquals(KafkaError.UNKNOWN_MEMBER_ID, gc.leave("g", j.memberId(), null));
    }

    @Test
    void oldJoinVersionsAssignMemberIdsDirectly() {
        GroupCoordinator gc = new GroupCoordinator(new MemStore(), 100, 60000, 0);
        GroupCoordinator.JoinResult j = join(gc, "g", "", 2, "range");
        assertEquals(0, j.error());
        assertTrue(j.memberId().startsWith("client-"));
        assertEquals(KafkaError.UNKNOWN_MEMBER_ID, join(gc, "g", "not-a-member", 2, "range").error());
    }

    @Test
    void secondMemberTriggersOneRebalanceAndProtocolVote() throws Exception {
        GroupCoordinator gc = new GroupCoordinator(new MemStore(), 100, 60000, 0);
        GroupCoordinator.JoinResult a = join(gc, "g", "", 2, "range", "roundrobin");
        gc.sync("g", 1, a.memberId(), null, null, Map.of(a.memberId(), new byte[] {1}));
        CompletableFuture<GroupCoordinator.JoinResult> bf = CompletableFuture.supplyAsync(() -> join(gc, "g", "", 2, "roundrobin"));
        long t0 = System.currentTimeMillis();
        while (gc.heartbeat("g", 1, a.memberId(), null) != KafkaError.REBALANCE_IN_PROGRESS) {
            assertTrue(System.currentTimeMillis() - t0 < 5000, "leader never saw the rebalance");
            Thread.sleep(20);
        }
        GroupCoordinator.JoinResult a2 = join(gc, "g", a.memberId(), 2, "range", "roundrobin");
        GroupCoordinator.JoinResult b = bf.get(5, TimeUnit.SECONDS);
        assertEquals(2, a2.generation());
        assertEquals(2, b.generation());
        assertEquals("roundrobin", a2.protocolName());
        assertEquals(a.memberId(), a2.leader());
        assertEquals(2, a2.members().size());
        assertEquals(0, b.members().size());
        assertEquals(a2.leader(), b.leader());
        // the follower waits for the leader's assignment
        CompletableFuture<GroupCoordinator.SyncResult> bs = CompletableFuture.supplyAsync(() -> gc.sync("g", 2, b.memberId(), null, null, Map.of()));
        Thread.sleep(150);
        assertTrue(!bs.isDone());
        gc.sync("g", 2, a.memberId(), null, null, Map.of(a.memberId(), new byte[] {7}, b.memberId(), new byte[] {8}));
        assertArrayEquals(new byte[] {8}, bs.get(5, TimeUnit.SECONDS).assignment());
        // the other member leaves: the leader is told to rejoin and gets a new generation alone
        assertEquals(0, gc.leave("g", b.memberId(), null));
        assertEquals(KafkaError.REBALANCE_IN_PROGRESS, gc.heartbeat("g", 2, a.memberId(), null));
        GroupCoordinator.JoinResult a3 = join(gc, "g", a.memberId(), 2, "range", "roundrobin");
        assertEquals(3, a3.generation());
        assertEquals(1, a3.members().size());
        assertEquals("range", a3.protocolName());
    }

    @Test
    void inconsistentProtocolsAndBadSessionAreRefused() {
        GroupCoordinator gc = new GroupCoordinator(new MemStore(), 6000, 30000, 0);
        assertEquals(KafkaError.INVALID_SESSION_TIMEOUT, gc.join("g", 100, 100, "", null, "consumer", protos("range"), "c", "h", 2).error());
        assertEquals(KafkaError.INVALID_SESSION_TIMEOUT, gc.join("g", 99999, 100, "", null, "consumer", protos("range"), "c", "h", 2).error());
        assertEquals(KafkaError.INVALID_GROUP_ID, gc.join("", 10000, 100, "", null, "consumer", protos("range"), "c", "h", 2).error());
        assertEquals(KafkaError.INCONSISTENT_GROUP_PROTOCOL, gc.join("g", 10000, 100, "", null, "", protos("range"), "c", "h", 2).error());
        assertEquals(KafkaError.INCONSISTENT_GROUP_PROTOCOL, gc.join("g", 10000, 100, "", null, "consumer", List.of(), "c", "h", 2).error());
        GroupCoordinator.JoinResult a = gc.join("g", 10000, 10000, "", null, "consumer", protos("range"), "c", "h", 2);
        assertEquals(0, a.error());
        assertEquals(KafkaError.INCONSISTENT_GROUP_PROTOCOL, gc.join("g", 10000, 10000, "", null, "connect", protos("range"), "c", "h", 2).error());
        assertEquals(KafkaError.INCONSISTENT_GROUP_PROTOCOL, gc.join("g", 10000, 10000, "", null, "consumer", protos("sticky"), "c", "h", 2).error());
    }

    @Test
    void silentMembersExpire() throws Exception {
        GroupCoordinator gc = new GroupCoordinator(new MemStore(), 100, 60000, 0);
        GroupCoordinator.JoinResult a = gc.join("g", 300, 300, "", null, "consumer", protos("range"), "c", "h", 2);
        gc.sync("g", 1, a.memberId(), null, null, Map.of(a.memberId(), new byte[0]));
        assertEquals(GroupCoordinator.STABLE, gc.snapshot("g").state);
        Thread.sleep(150);
        assertEquals(0, gc.heartbeat("g", 1, a.memberId(), null));
        Thread.sleep(500);
        gc.sweep();
        assertEquals(GroupCoordinator.EMPTY, gc.snapshot("g").state);
        assertEquals(KafkaError.UNKNOWN_MEMBER_ID, gc.heartbeat("g", 1, a.memberId(), null));
    }

    @Test
    void commitValidationAndDeleteAndPersistence() {
        MemStore store = new MemStore();
        GroupCoordinator gc = new GroupCoordinator(store, 100, 60000, 0);
        assertEquals(0, gc.validateCommit("simple", -1, "", null));
        assertEquals(KafkaError.UNKNOWN_MEMBER_ID, gc.validateCommit("simple", 3, "ghost", null));
        GroupCoordinator.JoinResult a = join(gc, "g", "", 2, "range");
        assertEquals(KafkaError.REBALANCE_IN_PROGRESS, gc.validateCommit("g", 1, a.memberId(), null));
        gc.sync("g", 1, a.memberId(), null, null, Map.of(a.memberId(), new byte[0]));
        assertEquals(0, gc.validateCommit("g", 1, a.memberId(), null));
        assertEquals(KafkaError.ILLEGAL_GENERATION, gc.validateCommit("g", 2, a.memberId(), null));
        assertEquals(KafkaError.UNKNOWN_MEMBER_ID, gc.validateCommit("g", -1, "", null));
        assertEquals(KafkaError.NON_EMPTY_GROUP, gc.delete("g"));
        gc.leave("g", a.memberId(), null);
        gc.sweep();
        KafkaStore.GroupRow row = store.loadGroup("g");
        assertNotNull(row);
        assertEquals(GroupCoordinator.EMPTY, row.state());
        assertEquals(0, gc.delete("g"));
        assertEquals(KafkaError.GROUP_ID_NOT_FOUND, gc.delete("g"));
        // a restarted coordinator continues the generation numbering of the persisted group
        MemStore s2 = new MemStore();
        s2.saveGroup(new KafkaStore.GroupRow("old", GroupCoordinator.STABLE, "consumer", "range", 41, "x", "[]"));
        GroupCoordinator fresh = new GroupCoordinator(s2, 100, 60000, 0);
        GroupCoordinator.JoinResult j = join(fresh, "old", "", 2, "range");
        assertEquals(42, j.generation());
        assertNotEquals(KafkaError.UNKNOWN_MEMBER_ID, j.error());
    }
}
