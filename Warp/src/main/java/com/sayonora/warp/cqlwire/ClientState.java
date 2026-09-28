package com.sayonora.warp.cqlwire;

/** Per-connection state visible to the engine. */
final class ClientState {

    volatile String keyspace;
    volatile String user = "anonymous";
    volatile String localAddress = "127.0.0.1";
    volatile int protocolVersion = 4;
}
