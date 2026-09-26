package com.sayonora.warp.amqpwire;

/** An AMQP error: a channel exception (channel.close) or a connection exception (connection.close), with RabbitMQ's reply texts. */
final class AmqpException extends RuntimeException {

    final boolean connection;
    final int code;
    final String text;
    /** Class/method ids to report instead of the offending method's (-1 = use the offending method's). */
    int classId = -1;
    int methodId = -1;
    /** Close the socket without any close frame (what RabbitMQ does for an undecodable method). */
    boolean silent;

    AmqpException(boolean connection, int code, String name, String detail) {
        super(detail.startsWith(name + " - ") ? detail : name + " - " + detail, null, false, false);
        this.connection = connection;
        this.code = code;
        this.text = getMessage();
    }

    static AmqpException channel(int code, String detail) {
        return new AmqpException(false, code, name(code), detail);
    }

    static AmqpException conn(int code, String detail) {
        return new AmqpException(true, code, name(code), detail);
    }

    AmqpException at(int cls, int mth) {
        this.classId = cls;
        this.methodId = mth;
        return this;
    }

    static AmqpException silentClose() {
        AmqpException e = conn(541, "undecodable method");
        e.silent = true;
        return e;
    }

    static AmqpException notFound(String detail) {
        return channel(404, detail);
    }

    static AmqpException precondition(String detail) {
        return channel(406, detail);
    }

    static AmqpException accessRefused(String detail) {
        return channel(403, detail);
    }

    static AmqpException locked(String detail) {
        return channel(405, detail);
    }

    static AmqpException notAllowed(String detail) {
        return conn(530, detail);
    }

    static AmqpException notImplemented(String detail) {
        return conn(540, detail);
    }

    static String name(int code) {
        return switch (code) {
            case 311 -> "CONTENT_TOO_LARGE";
            case 312 -> "NO_ROUTE";
            case 313 -> "NO_CONSUMERS";
            case 320 -> "CONNECTION_FORCED";
            case 402 -> "INVALID_PATH";
            case 403 -> "ACCESS_REFUSED";
            case 404 -> "NOT_FOUND";
            case 405 -> "RESOURCE_LOCKED";
            case 406 -> "PRECONDITION_FAILED";
            case 501 -> "FRAME_ERROR";
            case 502 -> "SYNTAX_ERROR";
            case 503 -> "COMMAND_INVALID";
            case 504 -> "CHANNEL_ERROR";
            case 505 -> "UNEXPECTED_FRAME";
            case 506 -> "RESOURCE_ERROR";
            case 530 -> "NOT_ALLOWED";
            case 540 -> "NOT_IMPLEMENTED";
            case 541 -> "INTERNAL_ERROR";
            default -> "ERROR";
        };
    }
}
