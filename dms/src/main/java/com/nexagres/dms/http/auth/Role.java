package com.nexagres.dms.http.auth;

/** Two roles, deliberately no more -- {@code ADMIN} (full access, the only role that exists on
 * the free/Developer tier) and {@code VIEWER} (read-only, an Enterprise-only second account type,
 * see {@code DmsLicensing#rbacAllowed}). Not an extensible permission system -- if finer-grained
 * roles become a real requirement later, this is the seam to grow, not to redesign around. */
public enum Role {
    ADMIN,
    VIEWER
}
