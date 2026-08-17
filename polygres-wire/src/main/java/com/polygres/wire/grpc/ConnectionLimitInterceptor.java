package com.polygres.wire.grpc;

import com.polygres.wire.core.ConnectionLimiter;
import io.grpc.ForwardingServerCallListener;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;

/**
 * The gRPC equivalent of {@code ProxyServer.admitOrReject} for the three TCP wire frontends: the
 * native driver's {@code Execute} RPC is stateless-per-call rather than a long-lived session, so
 * there's no socket-accept moment to gate — instead this counts each in-flight call as one
 * connection against the same process-wide {@link ConnectionLimiter}, acquiring at call start and
 * releasing exactly once when the call completes or is cancelled.
 */
final class ConnectionLimitInterceptor implements ServerInterceptor {

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
            ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
        if (!ConnectionLimiter.tryAcquire()) {
            call.close(Status.RESOURCE_EXHAUSTED.withDescription(
                    "polywire edition connection limit reached"), headers);
            return new ServerCall.Listener<>() {
            };
        }
        ServerCall.Listener<ReqT> listener = next.startCall(call, headers);
        return new ForwardingServerCallListener.SimpleForwardingServerCallListener<>(listener) {
            private boolean released;

            @Override
            public void onComplete() {
                releaseOnce();
                super.onComplete();
            }

            @Override
            public void onCancel() {
                releaseOnce();
                super.onCancel();
            }

            private void releaseOnce() {
                if (!released) {
                    released = true;
                    ConnectionLimiter.release();
                }
            }
        };
    }
}
