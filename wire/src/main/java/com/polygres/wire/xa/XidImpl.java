package com.polygres.wire.xa;

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
