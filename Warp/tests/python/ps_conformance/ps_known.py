"""Known differences between Google's Pub/Sub emulator (the oracle) and Warp. Warp implements the real Cloud Pub/Sub behaviour
(Google's documented semantics and error texts); the official emulator is a lenient test double, so where it deviates the step is
listed here with the reason. Key "case#step" or "case#a-b" (inclusive); value (kind, reason): kind "msg" (status text differs),
"body" (response body differs, also covers msg), "code" (status code differs, also covers the others).

MSG_FAMILIES: status texts that are the same error in different words (emulator: "Topic not found"; real Pub/Sub: "Resource not
found (resource=...)."): two texts of one family are treated as equal and counted as "known".
WARP_STRIP: response fields Warp returns as real Pub/Sub does and the emulator omits; removed from Warp's side before comparing.
"""
import re

MSG_FAMILIES = [
    (re.compile(r"^(Topic|Subscription|Snapshot|Schema) already exists$|^Resource already exists in the project \(resource=.*\)\.$"),
     "ALREADY_EXISTS"),
    (re.compile(r"^(Topic not found\.?|Subscription does not exist( \(resource=.*\))?|Snapshot does not exist|Schema not found|"
                r"Subscription topic does not exist|Dead letter topic not found|Schema could not be found)$"
                r"|^Resource not found \(resource=.*\)\.$"), "NOT_FOUND"),
    (re.compile(r"^Invalid \[\w+\] name: \(name=.*\)$|^Invalid resource name given \(name=.*\)\. Refer to .*$"), "BAD_NAME"),
]

# real Pub/Sub returns the default 31 day expiration policy on every subscription; the emulator returns none
WARP_STRIP = [("expiration_policy", {"ttl": "2678400s"})]

_E = "emulator accepts what real Pub/Sub rejects"
KNOWN = {
    "topic_crud#10-11": ("any", "emulator rejects the valid update_mask path 'labels' (bug), so labels stay unchanged"),
    "topic_crud#15": ("body", "cascade of the emulator rejecting update_mask 'labels'"),
    "topic_list_paging#8": ("code", "emulator accepts a negative page_size"),
    "publish_basic#7-8": ("code", "emulator accepts reserved ('goog') and empty attribute keys; real Pub/Sub rejects them"),
    "publish_basic#10": ("msg", "real text: The value for message_count is too large..."),
    "sub_crud#5": ("msg", "ack deadline bounds text (emulator: ack_deadline_secs out of bounds)"),
    "sub_crud#6": ("code", "emulator accepts ack_deadline_seconds 5; real minimum is 10"),
    "sub_crud#8": ("msg", "retention bounds text"),
    "sub_crud#9": ("code", "emulator accepts an 8 day message_retention_duration; real maximum is 7 days"),
    "sub_crud#13": ("body", "cascade: emulator created the extra subscriptions of steps 6 and 9"),
    "sub_crud#14": ("body", "cascade: emulator created the extra subscriptions of steps 6 and 9"),
    "sub_crud#15": ("code", "ListTopicSubscriptions of a missing topic is NOT_FOUND on real Pub/Sub"),
    "sub_crud#17": ("msg", "ack deadline bounds text"),
    "sub_crud#18": ("code", "emulator rejects the valid update_mask path 'labels'"),
    "sub_crud#20": ("msg", "immutable-field text"),
    "sub_crud#21": ("body", "cascade of the emulator rejecting update_mask 'labels'"),
    "sub_crud#24": ("body", "cascade: emulator created the extra subscriptions of steps 6 and 9"),
    "sub_crud#27": ("body", "cascade: emulator created the extra subscriptions of steps 6 and 9"),
    "pull_ack#14": ("code", "emulator accepts ack_deadline_seconds 601; real maximum is 600"),
    "filter#6-8": ("code", "emulator answers UNKNOWN 'Application error processing RPC' for an invalid filter; real: INVALID_ARGUMENT"),
    "ordering_redelivery_order#3": ("body", "Warp keeps one outstanding message per ordering key; the emulator hands out the whole key at once"),
    "ordering_redelivery_order#5": ("body", "same: after a nack Warp redelivers the head of the key only"),
    "dead_letter#10": ("body", "Warp also sets CloudPubSubDeadLetterSourceSubscriptionProject; attribute set differs"),
    "retry_policy#6": ("body", "Warp sets delivery_attempt only with a dead letter policy (as documented); the emulator always"),
    "retry_policy#8-9": ("body", "Warp honours the retry policy's 2 s minimum backoff after a nack; the emulator redelivers at once"),
    "snapshot_seek#5": ("body", "emulator drops snapshot labels; real keeps them"),
    "snapshot_seek#8": ("body", "same"),
    "snapshot_seek#10": ("body", "same"),
    "snapshot_seek#19": ("code", "emulator: UpdateSnapshot UNIMPLEMENTED"),
    "snapshot_seek#20": ("body", "cascade: UpdateSnapshot"),
    "detach#3-4": ("code", "emulator: DetachSubscription UNIMPLEMENTED"),
    "detach#6": ("code", "after Detach a Pull is FAILED_PRECONDITION on real Pub/Sub; the emulator never detached"),
    "detach#5": ("body", "emulator: DetachSubscription UNIMPLEMENTED, so no detached flag"),
    "push_config#4": ("msg", "invalid push endpoint text"),
    "exactly_once#6-8": ("msg", "Warp returns the documented EXACTLY_ONCE_ACKID_FAILURE ErrorInfo with a message; emulator: empty text"),
    "streaming_pull#5": ("body", "status text family (stream error)"),
    "streaming_pull#6": ("body", "Warp sends the initial subscription_properties response as real Pub/Sub does; the emulator does not"),
    "streaming_flow_control#3": ("body", "emulator ignores max_outstanding_messages; Warp honours the client's flow control"),
    "iam#2-6": ("code", "emulator: IAM policy calls UNIMPLEMENTED; Warp stores policies (never evaluated)"),
    "schema#2": ("msg", "Avro parser text"),
    "schema#5": ("body", "ListSchemas default view is BASIC (no definition) on real Pub/Sub; emulator returns the definition"),
    "schema#7": ("msg", "Avro parser text"),
    "schema#9": ("msg", "Avro decoder text"),
    "schema#13": ("msg", "JSON decoder text"),
}
