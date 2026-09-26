package com.sayonora.wire.pubsubwire;

import com.google.protobuf.Message;
import com.google.pubsub.v1.PublisherGrpc;
import com.google.pubsub.v1.SchemaServiceGrpc;
import com.google.pubsub.v1.StreamingPullRequest;
import com.google.pubsub.v1.StreamingPullResponse;
import com.google.pubsub.v1.SubscriberGrpc;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.ServerServiceDefinition;
import io.grpc.ServiceDescriptor;
import io.grpc.Status;
import io.grpc.stub.ServerCalls;
import io.grpc.stub.StreamObserver;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The gRPC surface: google.pubsub.v1.Publisher / Subscriber / SchemaService (using the descriptors generated from the vendored
 * protos) plus google.iam.v1.IAMPolicy (generated from the vendored IAM protos, whose java_package differs from the iam jar that needs a newer protobuf runtime). Every unary
 * call is dispatched through {@link PsRpc}; StreamingPull goes to {@link PsStreams}.
 */
final class PsGrpc {

    private static final Logger log = LoggerFactory.getLogger(PsGrpc.class);

    @FunctionalInterface
    interface Recorder {
        void record(String op, boolean write, long nanos);
    }

    private final PsRpc rpc;
    private final PsStreams streams;
    private final Recorder recorder;

    PsGrpc(PsRpc rpc, PsStreams streams, Recorder recorder) {
        this.rpc = rpc;
        this.streams = streams;
        this.recorder = recorder;
    }

    List<ServerServiceDefinition> services() {
        List<ServerServiceDefinition> l = new java.util.ArrayList<>();
        l.add(build(PublisherGrpc.getServiceDescriptor()));
        l.add(build(SubscriberGrpc.getServiceDescriptor()));
        l.add(build(SchemaServiceGrpc.getServiceDescriptor()));
        l.add(build(com.sayonora.wire.pubsubwire.iam.IAMPolicyGrpc.getServiceDescriptor()));
        return l;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private ServerServiceDefinition build(ServiceDescriptor sd) {
        ServerServiceDefinition.Builder b = ServerServiceDefinition.builder(sd);
        for (MethodDescriptor<?, ?> md : sd.getMethods()) {
            if (md.getType() == MethodDescriptor.MethodType.BIDI_STREAMING) {
                b.addMethod((MethodDescriptor<StreamingPullRequest, StreamingPullResponse>) md,
                        ServerCalls.asyncBidiStreamingCall(out -> streams.open(out)));
            } else {
                b.addMethod((MethodDescriptor) md, ServerCalls.asyncUnaryCall(unary(sd.getName() + "/" + md.getBareMethodName())));
            }
        }
        return b.build();
    }

    private ServerCalls.UnaryMethod<Message, Message> unary(String key) {
        PsRpc.Rpc r = rpc.byMethod.get(key);
        if (r == null) {
            return (req, obs) -> obs.onError(Status.UNIMPLEMENTED.withDescription("Method not implemented: " + key).asRuntimeException());
        }
        return (req, obs) -> {
            long t0 = System.nanoTime();
            try {
                Message resp = r.impl().apply(req);
                obs.onNext(resp);
                obs.onCompleted();
            } catch (PsException e) {
                obs.onError(e.toGrpc());
            } catch (RuntimeException e) {
                log.error("pubsubwire {} failed", key, e);
                obs.onError(Status.INTERNAL.withDescription("Internal error").asRuntimeException());
            } finally {
                recorder.record(r.name(), r.write(), System.nanoTime() - t0);
            }
        };
    }

    /** Requires {@code authorization: Bearer <token>} when tokens are configured. */
    static ServerInterceptor auth(java.util.Set<String> tokens) {
        return new ServerInterceptor() {
            @Override
            public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call, Metadata headers,
                    ServerCallHandler<ReqT, RespT> next) {
                if (!tokens.isEmpty()) {
                    String h = headers.get(Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER));
                    if (h == null || !h.regionMatches(true, 0, "Bearer ", 0, 7) || !tokens.contains(h.substring(7).trim())) {
                        call.close(Status.UNAUTHENTICATED.withDescription(
                                "Request had invalid authentication credentials. Expected OAuth 2 access token, login cookie or other "
                                        + "valid authentication credential."), new Metadata());
                        return new ServerCall.Listener<>() {
                        };
                    }
                }
                return next.startCall(call, headers);
            }
        };
    }
}
