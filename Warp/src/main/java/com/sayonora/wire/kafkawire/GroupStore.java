package com.sayonora.wire.kafkawire;

/** What the group coordinator needs from the store: the persisted snapshot of a group. */
interface GroupStore {

    KafkaStore.GroupRow loadGroup(String id);

    void saveGroup(KafkaStore.GroupRow row);

    boolean deleteGroup(String id);

    /** True when the group has a persisted snapshot or committed offsets (an offsets-only group exists as Empty). */
    boolean groupExists(String id);
}
