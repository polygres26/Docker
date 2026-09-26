package com.sayonora.wire.kafkawire;

/** A Kafka protocol error: the numeric error code a client sees and a human message. */
final class KafkaError extends RuntimeException {

    static final int UNKNOWN_SERVER_ERROR = -1;
    static final int NONE = 0;
    static final int OFFSET_OUT_OF_RANGE = 1;
    static final int CORRUPT_MESSAGE = 2;
    static final int UNKNOWN_TOPIC_OR_PARTITION = 3;
    static final int INVALID_FETCH_SIZE = 4;
    static final int LEADER_NOT_AVAILABLE = 5;
    static final int NOT_LEADER_OR_FOLLOWER = 6;
    static final int REQUEST_TIMED_OUT = 7;
    static final int MESSAGE_TOO_LARGE = 10;
    static final int OFFSET_METADATA_TOO_LARGE = 12;
    static final int NETWORK_EXCEPTION = 13;
    static final int COORDINATOR_LOAD_IN_PROGRESS = 14;
    static final int COORDINATOR_NOT_AVAILABLE = 15;
    static final int NOT_COORDINATOR = 16;
    static final int INVALID_TOPIC_EXCEPTION = 17;
    static final int RECORD_LIST_TOO_LARGE = 18;
    static final int INVALID_REQUIRED_ACKS = 21;
    static final int ILLEGAL_GENERATION = 22;
    static final int INCONSISTENT_GROUP_PROTOCOL = 23;
    static final int INVALID_GROUP_ID = 24;
    static final int UNKNOWN_MEMBER_ID = 25;
    static final int INVALID_SESSION_TIMEOUT = 26;
    static final int REBALANCE_IN_PROGRESS = 27;
    static final int INVALID_COMMIT_OFFSET_SIZE = 28;
    static final int UNSUPPORTED_SASL_MECHANISM = 33;
    static final int ILLEGAL_SASL_STATE = 34;
    static final int UNSUPPORTED_VERSION = 35;
    static final int TOPIC_ALREADY_EXISTS = 36;
    static final int INVALID_PARTITIONS = 37;
    static final int INVALID_REPLICATION_FACTOR = 38;
    static final int INVALID_REPLICA_ASSIGNMENT = 39;
    static final int INVALID_CONFIG = 40;
    static final int NOT_CONTROLLER = 41;
    static final int INVALID_REQUEST = 42;
    static final int UNSUPPORTED_FOR_MESSAGE_FORMAT = 43;
    static final int OUT_OF_ORDER_SEQUENCE_NUMBER = 45;
    static final int DUPLICATE_SEQUENCE_NUMBER = 46;
    static final int INVALID_PRODUCER_EPOCH = 47;
    static final int INVALID_TXN_STATE = 48;
    static final int INVALID_TIMESTAMP = 32;
    static final int TOPIC_AUTHORIZATION_FAILED = 29;
    static final int GROUP_AUTHORIZATION_FAILED = 30;
    static final int UNKNOWN_PRODUCER_ID = 59;
    static final int NON_EMPTY_GROUP = 68;
    static final int GROUP_ID_NOT_FOUND = 69;
    static final int UNKNOWN_LEADER_EPOCH = 74;
    static final int SASL_AUTHENTICATION_FAILED = 58;
    static final int INVALID_RECORD = 87;
    static final int UNSTABLE_OFFSET_COMMIT = 88;
    static final int MEMBER_ID_REQUIRED = 79;
    static final int FENCED_INSTANCE_ID = 82;
    static final int UNKNOWN_TOPIC_ID = 100;
    static final int GROUP_MAX_SIZE_REACHED = 81;

    final int code;

    KafkaError(int code, String message) {
        super(message, null, false, false);
        this.code = code;
    }
}
