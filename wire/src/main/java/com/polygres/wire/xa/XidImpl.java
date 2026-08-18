package com.polygres.wire.xa;

import java.security.SecureRandom;
import javax.transaction.xa.Xid;

/**
 * Minimal {@link Xid} implementation for the in-process XA coordinator
 * ({@link XaTransaction}). All branches of one global transaction share the
 * same {@code globalTransactionId}; each gets a distinct
 * {@code branchQualifier} (its index), which is the standard way resource
 * managers distinguish branches of the same distributed transaction.
 *
 * <p>Ported from {@code com.omnigate.xa.XidImpl} verbatim (backend-agnostic — no Oracle
 * dependency), package renamed only.
 */
final class XidImpl implements Xid {

    private static final int FORMAT_ID = 0x504F4C59; // "POLY" — arbitrary but distinct from other TMs sharing a resource manager

    private final byte[] globalTransactionId;
    private final byte[] branchQualifier;

    private XidImpl(byte[] globalTransactionId, byte[] branchQualifier) {
        this.globalTransactionId = globalTransactionId;
        this.branchQualifier = branchQualifier;
    }

    static byte[] newGlobalTransactionId() {
        byte[] gtrid = new byte[64];
        new SecureRandom().nextBytes(gtrid);
        return gtrid;
    }

    static XidImpl branch(byte[] globalTransactionId, int branchIndex) {
        return new XidImpl(globalTransactionId, new byte[] {(byte) branchIndex});
    }

    @Override
    public int getFormatId() {
        return FORMAT_ID;
    }

    @Override
    public byte[] getGlobalTransactionId() {
        return globalTransactionId;
    }

    @Override
    public byte[] getBranchQualifier() {
        return branchQualifier;
    }
}
