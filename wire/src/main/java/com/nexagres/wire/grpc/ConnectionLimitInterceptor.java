package com.nexagres.wire.grpc;

import com.nexagres.wire.core.ConnectionLimiter;
import io.grpc.ForwardingServerCallListener;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;

final class ConnectionLimitInterceptor implements ServerInterceptor {

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
            ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
        if (!ConnectionLimiter.tryAcquire()) {
            call.close(Status.RESOURCE_EXHAUSTED.withDescription(
                    "warp edition connection limit reached"), headers);
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
