package com.sayonora.dms.http;

/** POST /api/scan body. Passwords never get logged or echoed back -- see ScanRoute. */
public class ScanRequest {
    public String name;
    public String jdbcUrl;
    public String user;
    public String password;
}
