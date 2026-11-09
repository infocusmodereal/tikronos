package org.tikv.common;

import org.tikv.shade.io.grpc.Channel;

/** Package bridge for client-java 3.3.5's protected, leader-aware channel access. */
public final class TikronosPdAccess {
    private TikronosPdAccess() {}

    public static Channel channel(PDClient client) {
        // Preserve the SDK's TLS, host mapping and follower-forwarding interceptor.
        return client.getBlockingStub().getChannel();
    }
}
