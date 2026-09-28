package com.sayonora.warp.rediswire;

/** Dev harness: {@code RedisDevMain <port> <user> <password> <jdbcUrl>...} serves rediswire straight over Postgres. */
public final class RedisDevMain {
    public static void main(String[] args) throws Exception {
        int port = Integer.parseInt(args[0]);
        String[] urls = java.util.Arrays.copyOfRange(args, 3, args.length);
        DirectBackends b = new DirectBackends(args[1], args[2], urls);
        RedisWireServer s = new RedisWireServer(RedisOptions.fromEnv(port), b, null);
        s.start();
        System.out.println("rediswire dev server on " + s.port());
        Thread.currentThread().join();
    }
}
