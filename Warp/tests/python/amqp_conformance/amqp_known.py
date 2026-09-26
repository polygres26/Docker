"""Documented divergences of amqpwire from RabbitMQ 4.3 as seen by the differential corpus (amqp_corpus.py, golden.json.gz). Every entry says WHY.
The replay test fails on an entry that no longer occurs, so this file cannot rot. Divergences that the corpus does not exercise (server
identification, unsupported plugins, ...) are listed in DOCUMENTED below for the guide."""

# reply_text rewrites applied to BOTH sides before comparing (regex -> replacement): cosmetic wording that carries no protocol meaning.
# None needed: every error code AND text of the corpus matches RabbitMQ's.
MESSAGE_PATTERNS = []

# (case, step index) -> reason: whole steps that legitimately differ
RESULT_DIVERGENCES = {
    ("q_quorum_type", 10): "RabbitMQ 4 refuses a non-durable, non-exclusive queue (feature transient_nonexcl_queues is deprecated, 541 and connection close). "
                           "Warp accepts transient queues (apps written for older RabbitMQ keep working); for a non-durable quorum queue it answers "
                           "406 'invalid property non-durable' like RabbitMQ 3.x did.",
    ("conn_bad_credentials_no_auth_on_warp", 0): "Warp authenticates AMQP logins only when WARP_AMQPWIRE_AUTH=true or WARP_AUTH_CREDENTIALS is set "
                                                "(the corpus runs against an open Warp); the refusal (403 ACCESS_REFUSED, same text) is tested with auth on.",
    ("basic_consumer_tag_reuse_across_channels", 9): "After a connection exception RabbitMQ tears the channels down (requeueing their unacknowledged messages) before the "
                                                   "connection.close frame goes out, so a consumer on another channel can already be handed the redelivery first; Warp sends "
                                                   "connection.close first and requeues while closing. The error code and text are identical (530, 'attempt to reuse consumer tag').",
    ("conn_bad_credentials_no_auth_on_warp", 1): "same as step 0: the login was accepted, so the channel opens",
}

# not exercised by the corpus, documented in the guide
DOCUMENTED = {
    "server-properties": "Warp reports product 'Warp AMQP' (no cluster_name, copyright, platform of RabbitMQ); the capability set is RabbitMQ's.",
    "mechanisms": "PLAIN and AMQPLAIN (RabbitMQ 4 also lists ANONYMOUS; EXTERNAL / OAuth 2 are not implemented).",
    "amqp-1.0": "A client that opens with the AMQP 1.0 header ('AMQP' 0 1 0 0 / 3 0 1 0) is answered with the 0-9-1 header and disconnected: phase 2 (AMQP 1.0) is not implemented.",
    "frame_max": "tune-ok frame_max 0 (no limit) is accepted and means the server's frame_max; RabbitMQ closes such a connection like one below 8192.",
    "plugins": "Exchange types of plugins (x-consistent-hash, x-delayed-message, ...) and streams (x-queue-type=stream) are not implemented (406).",
    "queue-arguments": "x-single-active-consumer, x-delivery-limit, x-message-deduplication, x-queue-mode / x-queue-version are accepted and ignored; quorum queues behave like "
                       "classic ones (durable required) without Raft.",
    "consumer-count": "consumer_count of queue.declare-ok counts the consumers attached to THIS Warp instance (consumers live in the connection's process).",
    "restart": "Non-durable queues and transient (delivery_mode 1) messages survive a restart of Warp: everything lives in Postgres.",
    "latency": "A message published through another Warp instance reaches an idle consumer within WARP_AMQPWIRE_POLL_MS (200 ms); through the same instance it is immediate. "
               "TTL expiry, x-expires and dead-lettering of expired messages run every WARP_AMQPWIRE_SWEEP_MS (1000 ms).",
    "connection.blocked": "Never sent: there is no memory or disk alarm; back-pressure is TCP plus the Postgres pool.",
    "max-length": "x-max-length / x-max-length-bytes drop-head is enforced right after the publish commits, not atomically with it, so concurrent publishers "
                  "can overshoot the limit briefly; reject-publish is checked inside the insert with the same caveat.",
    "channel.flow": "channel.flow active=false is refused with 540 like RabbitMQ; there is no server-side flow control.",
    "basic.recover": "requeue=false is refused with 540 like RabbitMQ.",
    "global-qos": "basic.qos global=true limits the unacknowledged messages per queue of the channel, which is what RabbitMQ's classic queues do.",
}

# AMQP 1.0 (amqp10_corpus.py, golden10.json.gz): (case, step) -> reason
RESULT_DIVERGENCES10 = {
    ("p10_open_without_sasl_and_anonymous", 0): "RabbitMQ refuses a plain AMQP 1.0 header (no SASL) and answers with the SASL header; Warp accepts it while no login is required "
                                               "(WARP_AMQPWIRE_AUTH / WARP_AUTH_CREDENTIALS unset) and behaves like RabbitMQ once logins are required (tested with auth on).",
}
