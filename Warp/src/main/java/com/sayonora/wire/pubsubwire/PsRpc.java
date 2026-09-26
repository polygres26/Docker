package com.sayonora.wire.pubsubwire;

import com.sayonora.wire.pubsubwire.iam.GetIamPolicyRequest;
import com.sayonora.wire.pubsubwire.iam.SetIamPolicyRequest;
import com.sayonora.wire.pubsubwire.iam.TestIamPermissionsRequest;
import com.google.protobuf.Message;
import com.google.pubsub.v1.*;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;

/** The table of unary RPCs shared by the gRPC and REST transports: "Service/Method" to (request prototype, implementation). */
final class PsRpc {

    record Rpc(String name, boolean write, Message prototype, Function<Message, Message> impl) {
    }

    final Map<String, Rpc> byMethod = new LinkedHashMap<>();

    @SuppressWarnings("unchecked")
    private <Q extends Message> void reg(String service, String method, boolean write, Q proto, Function<Q, ? extends Message> f) {
        byMethod.put(service + "/" + method, new Rpc(method, write, proto, m -> f.apply((Q) m)));
    }

    PsRpc(PsService s) {
        String pub = "google.pubsub.v1.Publisher";
        reg(pub, "CreateTopic", true, Topic.getDefaultInstance(), s::createTopic);
        reg(pub, "UpdateTopic", true, UpdateTopicRequest.getDefaultInstance(), s::updateTopic);
        reg(pub, "Publish", true, PublishRequest.getDefaultInstance(), s::publish);
        reg(pub, "GetTopic", false, GetTopicRequest.getDefaultInstance(), s::getTopic);
        reg(pub, "ListTopics", false, ListTopicsRequest.getDefaultInstance(), s::listTopics);
        reg(pub, "ListTopicSubscriptions", false, ListTopicSubscriptionsRequest.getDefaultInstance(), s::listTopicSubscriptions);
        reg(pub, "ListTopicSnapshots", false, ListTopicSnapshotsRequest.getDefaultInstance(), s::listTopicSnapshots);
        reg(pub, "DeleteTopic", true, DeleteTopicRequest.getDefaultInstance(), s::deleteTopic);
        reg(pub, "DetachSubscription", true, DetachSubscriptionRequest.getDefaultInstance(), s::detachSubscription);
        String sub = "google.pubsub.v1.Subscriber";
        reg(sub, "CreateSubscription", true, Subscription.getDefaultInstance(), s::createSubscription);
        reg(sub, "GetSubscription", false, GetSubscriptionRequest.getDefaultInstance(), s::getSubscription);
        reg(sub, "UpdateSubscription", true, UpdateSubscriptionRequest.getDefaultInstance(), s::updateSubscription);
        reg(sub, "ListSubscriptions", false, ListSubscriptionsRequest.getDefaultInstance(), s::listSubscriptions);
        reg(sub, "DeleteSubscription", true, DeleteSubscriptionRequest.getDefaultInstance(), s::deleteSubscription);
        reg(sub, "ModifyAckDeadline", true, ModifyAckDeadlineRequest.getDefaultInstance(), s::modifyAckDeadline);
        reg(sub, "Acknowledge", true, AcknowledgeRequest.getDefaultInstance(), s::acknowledge);
        reg(sub, "Pull", true, PullRequest.getDefaultInstance(), s::pull);
        reg(sub, "ModifyPushConfig", true, ModifyPushConfigRequest.getDefaultInstance(), s::modifyPushConfig);
        reg(sub, "GetSnapshot", false, GetSnapshotRequest.getDefaultInstance(), s::getSnapshot);
        reg(sub, "ListSnapshots", false, ListSnapshotsRequest.getDefaultInstance(), s::listSnapshots);
        reg(sub, "CreateSnapshot", true, CreateSnapshotRequest.getDefaultInstance(), s::createSnapshot);
        reg(sub, "UpdateSnapshot", true, UpdateSnapshotRequest.getDefaultInstance(), s::updateSnapshot);
        reg(sub, "DeleteSnapshot", true, DeleteSnapshotRequest.getDefaultInstance(), s::deleteSnapshot);
        reg(sub, "Seek", true, SeekRequest.getDefaultInstance(), s::seek);
        String sch = "google.pubsub.v1.SchemaService";
        reg(sch, "CreateSchema", true, CreateSchemaRequest.getDefaultInstance(), s::createSchema);
        reg(sch, "GetSchema", false, GetSchemaRequest.getDefaultInstance(), s::getSchema);
        reg(sch, "ListSchemas", false, ListSchemasRequest.getDefaultInstance(), s::listSchemas);
        reg(sch, "DeleteSchema", true, DeleteSchemaRequest.getDefaultInstance(), s::deleteSchema);
        reg(sch, "ValidateSchema", false, ValidateSchemaRequest.getDefaultInstance(), s::validateSchema);
        reg(sch, "ValidateMessage", false, ValidateMessageRequest.getDefaultInstance(), s::validateMessage);
        reg(sch, "ListSchemaRevisions", false, ListSchemaRevisionsRequest.getDefaultInstance(), r -> {
            throw PsException.unimplemented("Schema revisions are not supported: a schema has a single revision.");
        });
        reg(sch, "CommitSchema", true, CommitSchemaRequest.getDefaultInstance(), r -> {
            throw PsException.unimplemented("Schema revisions are not supported: a schema has a single revision.");
        });
        reg(sch, "RollbackSchema", true, RollbackSchemaRequest.getDefaultInstance(), r -> {
            throw PsException.unimplemented("Schema revisions are not supported: a schema has a single revision.");
        });
        reg(sch, "DeleteSchemaRevision", true, DeleteSchemaRevisionRequest.getDefaultInstance(), r -> {
            throw PsException.unimplemented("Schema revisions are not supported: a schema has a single revision.");
        });
        String iam = "google.iam.v1.IAMPolicy";
        reg(iam, "GetIamPolicy", false, GetIamPolicyRequest.getDefaultInstance(), s::getIamPolicy);
        reg(iam, "SetIamPolicy", true, SetIamPolicyRequest.getDefaultInstance(), s::setIamPolicy);
        reg(iam, "TestIamPermissions", false, TestIamPermissionsRequest.getDefaultInstance(), s::testIamPermissions);
    }
}
