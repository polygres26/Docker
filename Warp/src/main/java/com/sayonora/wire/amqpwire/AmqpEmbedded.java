package com.sayonora.wire.amqpwire;

import com.sayonora.wire.amqpwire.AmqpStore.BindingDef;
import com.sayonora.wire.amqpwire.AmqpStore.ExchangeDef;
import com.sayonora.wire.amqpwire.AmqpStore.MsgRow;
import com.sayonora.wire.amqpwire.AmqpStore.QueueDef;
import com.sayonora.wire.core.BackendRegistry;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * In-process access to the AMQP store for the MCP tools: the same broker core the wire protocol uses (routing, queue arguments,
 * dead-lettering, error texts), without a connection. Errors surface as {@link ApiError} carrying RabbitMQ's reply text.
 */
public final class AmqpEmbedded {

    public static final class ApiError extends RuntimeException {
        public final int code;

        ApiError(AmqpException e) {
            super(e.text, null, false, false);
            this.code = e.code;
        }
    }

    private final AmqpBroker br;
    private static final String OWNER = "mcp/0";

    public AmqpEmbedded(BackendRegistry registry) {
        this.br = new AmqpBroker(new AmqpShards(registry), AmqpConfig.fromEnv());
    }

    private <T> T run(java.util.function.Supplier<T> s) {
        try {
            return s.get();
        } catch (AmqpException e) {
            throw new ApiError(e);
        }
    }

    public List<Map<String, Object>> exchanges(String vhost) {
        return run(() -> {
            br.ensureVhost(vhost);
            List<Map<String, Object>> out = new ArrayList<>();
            for (ExchangeDef e : br.store.exchanges(vhost)) {
                Map<String, Object> m = new LinkedHashMap<>();
                m.put("name", e.name());
                m.put("type", e.type());
                m.put("durable", e.durable());
                m.put("auto_delete", e.autoDelete());
                m.put("internal", e.internal());
                m.put("arguments", e.args());
                out.add(m);
            }
            return out;
        });
    }

    public List<Map<String, Object>> queues(String vhost) {
        return run(() -> {
            List<Map<String, Object>> out = new ArrayList<>();
            for (String h : br.shards.allHosts()) {
                for (QueueDef q : br.store.queuesOn(h, vhost)) {
                    Map<String, Object> m = new LinkedHashMap<>();
                    m.put("name", q.name());
                    m.put("durable", q.durable());
                    m.put("exclusive", q.exclOwner() != null);
                    m.put("auto_delete", q.autoDelete());
                    m.put("arguments", q.args());
                    m.put("messages_ready", br.store.ready(vhost, q.name()));
                    m.put("messages_unacknowledged", br.store.unacked(vhost, q.name()));
                    m.put("host", h);
                    out.add(m);
                }
            }
            out.sort((a, b) -> ((String) a.get("name")).compareTo((String) b.get("name")));
            return out;
        });
    }

    public List<Map<String, Object>> bindings(String vhost) {
        return run(() -> {
            br.ensureVhost(vhost);
            List<Map<String, Object>> out = new ArrayList<>();
            for (BindingDef b : br.store.bindings(vhost)) {
                Map<String, Object> m = new LinkedHashMap<>();
                m.put("source", b.source());
                m.put("destination", b.dest());
                m.put("destination_type", b.destType() == 'q' ? "queue" : "exchange");
                m.put("routing_key", b.rkey());
                m.put("arguments", b.args());
                out.add(m);
            }
            return out;
        });
    }

    public void declareExchange(String vhost, String name, String type, boolean durable, boolean autoDelete, boolean internal,
            Map<String, Object> args) {
        run(() -> {
            br.declareExchange(vhost, name, type, false, durable, autoDelete, internal, args);
            return null;
        });
    }

    public void deleteExchange(String vhost, String name, boolean ifUnused) {
        run(() -> {
            br.deleteExchange(vhost, name, ifUnused);
            return null;
        });
    }

    public void declareQueue(String vhost, String name, boolean durable, boolean autoDelete, Map<String, Object> args) {
        run(() -> {
            br.declareQueue(vhost, name, false, durable, false, autoDelete, args, OWNER);
            return null;
        });
    }

    public long deleteQueue(String vhost, String name, boolean ifUnused, boolean ifEmpty) {
        return run(() -> br.deleteQueue(vhost, name, ifUnused, ifEmpty, OWNER));
    }

    public void bind(String vhost, String source, String dest, boolean destIsExchange, String rk, Map<String, Object> args) {
        run(() -> {
            br.bind(vhost, source, dest, destIsExchange ? 'e' : 'q', rk, args, OWNER);
            return null;
        });
    }

    public void unbind(String vhost, String source, String dest, boolean destIsExchange, String rk, Map<String, Object> args) {
        run(() -> {
            br.unbind(vhost, source, dest, destIsExchange ? 'e' : 'q', rk, args, OWNER);
            return null;
        });
    }

    public long purge(String vhost, String queue) {
        return run(() -> br.purge(vhost, queue, OWNER));
    }

    /** Publishes with the given properties (content_type, headers, priority, delivery_mode, expiration, ...). Returns the number of queues it reached. */
    public int publish(String vhost, String exchange, String rk, Map<String, Object> props, byte[] body) {
        return run(() -> {
            AmqpProps p = new AmqpProps();
            if (props != null) {
                if (props.get("content_type") instanceof String s) {
                    p.contentType = s;
                }
                if (props.get("content_encoding") instanceof String s) {
                    p.contentEncoding = s;
                }
                if (props.get("headers") instanceof Map<?, ?> m) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> h = new LinkedHashMap<>((Map<String, Object>) m);
                    p.headers = h;
                }
                p.deliveryMode = props.get("delivery_mode") instanceof Number n ? n.intValue() : null;
                p.priority = props.get("priority") instanceof Number n ? n.intValue() : null;
                p.correlationId = props.get("correlation_id") instanceof String s ? s : null;
                p.replyTo = props.get("reply_to") instanceof String s ? s : null;
                p.expiration = props.get("expiration") instanceof String s ? s : props.get("expiration") instanceof Number n ? String.valueOf(n.longValue()) : null;
                p.messageId = props.get("message_id") instanceof String s ? s : null;
                p.type = props.get("type") instanceof String s ? s : null;
                p.appId = props.get("app_id") instanceof String s ? s : null;
            }
            return br.publish(vhost, exchange, rk, p.toBytes(), p, body == null ? new byte[0] : body, null).routed();
        });
    }

    /** Non-destructive look at the messages of a queue (ready and unacknowledged), in delivery order. */
    public List<Map<String, Object>> peek(String vhost, String queue, int limit) {
        return run(() -> {
            if (br.store.queue(vhost, queue) == null) {
                throw AmqpException.notFound("no queue '" + queue + "' in vhost '" + vhost + "'");
            }
            List<Map<String, Object>> out = new ArrayList<>();
            for (MsgRow r : br.store.peek(vhost, queue, limit)) {
                Map<String, Object> m = new LinkedHashMap<>();
                m.put("exchange", r.exchange());
                m.put("routing_key", r.rkey());
                m.put("redelivered", r.redelivered());
                m.put("properties", AmqpProps.parse(r.props()).asMap());
                m.put("body", r.body());
                out.add(m);
            }
            return out;
        });
    }
}
