package com.nexagres.wire.xa;

import java.security.SecureRandom;
import javax.transaction.xa.Xid;

final class XidImpl implements Xid {

    private static final int FORMAT_ID = 0x504F4C59;

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

    /** True when {@code other} is the same global transaction and branch as this Xid -- used by
     * {@link XaRecovery} to find this branch among whatever a backend's {@code XAResource.recover()}
     * actually still has prepared, which may use its own concrete Xid implementation rather than
     * this class. */
    static boolean sameBranch(Xid mine, Xid other) {
        return mine.getFormatId() == other.getFormatId()
                && java.util.Arrays.equals(mine.getGlobalTransactionId(), other.getGlobalTransactionId())
                && java.util.Arrays.equals(mine.getBranchQualifier(), other.getBranchQualifier());
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
