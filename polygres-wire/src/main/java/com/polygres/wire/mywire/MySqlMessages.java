package com.polygres.wire.mywire;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Types;
import java.util.List;

/** Encodes MySQL server->client packets (handshake, OK/ERR, text resultset). */
final class MySqlMessages {

    static final int CLIENT_PROTOCOL_41 = 0x00000200;
    static final int CLIENT_SECURE_CONNECTION = 0x00008000;
    static final int CLIENT_PLUGIN_AUTH = 0x00080000;
    static final int CLIENT_CONNECT_WITH_DB = 0x00000008;
    // MySQL protocol's own in-band TLS flag: advertised in the server's initial Handshake packet
    // (below) only when TLS is actually configured; a client that wants TLS then sets this same
    // bit on a partial "SSLRequest" packet (capability flags + charset only, no username/auth --
    // see MySqlWireSessionHandler#performHandshake) before starting the TLS handshake in place on
    // the same socket, mirroring pgwire's SSLRequest and real Postgres/MySQL server behavior.
    static final int CLIENT_SSL = 0x00000800;

    static byte[] handshakeV10(long connectionId, byte[] scramble, boolean tlsSupported) {
        ByteArrayOutputStream b = new ByteArrayOutputStream();
        b.write(10); // protocol version
        MySqlPacket.writeNulString(b, "8.0.34-polywire");
        MySqlPacket.writeFixedInt(b, connectionId, 4);
        b.write(scramble, 0, 8); // auth-plugin-data-part-1
        b.write(0); // filler
        int capabilities = CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION | CLIENT_PLUGIN_AUTH | CLIENT_CONNECT_WITH_DB
                | (tlsSupported ? CLIENT_SSL : 0);
        MySqlPacket.writeFixedInt(b, capabilities & 0xFFFF, 2); // capability flags (lower)
        b.write(0x21); // character set: utf8_general_ci
        MySqlPacket.writeFixedInt(b, 0x0002, 2); // status flags: SERVER_STATUS_AUTOCOMMIT
        MySqlPacket.writeFixedInt(b, (capabilities >>> 16) & 0xFFFF, 2); // capability flags (upper)
        b.write(21); // length of auth-plugin-data (8 + 13)
        for (int i = 0; i < 10; i++) {
            b.write(0); // reserved
        }
        b.write(scramble, 8, 12); // auth-plugin-data-part-2
        b.write(0);
        MySqlPacket.writeNulString(b, "mysql_native_password");
        return b.toByteArray();
    }

    /** mysql_native_password: SHA1(password) XOR SHA1(scramble + SHA1(SHA1(password))). */
    static byte[] nativePasswordScramble(String password, byte[] scramble) {
        try {
            MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
            byte[] stage1 = sha1.digest(password.getBytes(StandardCharsets.UTF_8));
            sha1.reset();
            byte[] stage2 = sha1.digest(stage1);
            sha1.reset();
            sha1.update(scramble);
            sha1.update(stage2);
            byte[] stage3 = sha1.digest();
            byte[] result = new byte[stage1.length];
            for (int i = 0; i < result.length; i++) {
                result[i] = (byte) (stage1[i] ^ stage3[i]);
            }
            return result;
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    static byte[] okPacket(long affectedRows) {
        ByteArrayOutputStream b = new ByteArrayOutputStream();
        b.write(0x00);
        MySqlPacket.writeLenEncInt(b, affectedRows);
        MySqlPacket.writeLenEncInt(b, 0); // last insert id
        MySqlPacket.writeFixedInt(b, 0x0002, 2); // status flags: autocommit
        MySqlPacket.writeFixedInt(b, 0, 2); // warnings
        return b.toByteArray();
    }

    static byte[] eofPacket() {
        ByteArrayOutputStream b = new ByteArrayOutputStream();
        b.write(0xfe);
        MySqlPacket.writeFixedInt(b, 0, 2); // warnings
        MySqlPacket.writeFixedInt(b, 0x0002, 2); // status flags
        return b.toByteArray();
    }

    static byte[] errPacket(int code, String sqlState, String message) {
        ByteArrayOutputStream b = new ByteArrayOutputStream();
        b.write(0xff);
        MySqlPacket.writeFixedInt(b, code, 2);
        b.write('#');
        b.write(sqlState.getBytes(StandardCharsets.UTF_8), 0, Math.min(5, sqlState.length()));
        b.write(message.getBytes(StandardCharsets.UTF_8), 0, message.getBytes(StandardCharsets.UTF_8).length);
        return b.toByteArray();
    }

    static byte[] columnDefinition(String name, int jdbcType) {
        ByteArrayOutputStream b = new ByteArrayOutputStream();
        MySqlPacket.writeLenEncString(b, "def");
        MySqlPacket.writeLenEncString(b, ""); // schema
        MySqlPacket.writeLenEncString(b, ""); // table
        MySqlPacket.writeLenEncString(b, ""); // org_table
        MySqlPacket.writeLenEncString(b, name);
        MySqlPacket.writeLenEncString(b, name); // org_name
        b.write(0x0c); // length of fixed fields
        MySqlPacket.writeFixedInt(b, 0x21, 2); // character set: utf8_general_ci
        MySqlPacket.writeFixedInt(b, 0, 4); // column length
        b.write(mysqlTypeFor(jdbcType));
        MySqlPacket.writeFixedInt(b, 0, 2); // flags
        b.write(0); // decimals
        MySqlPacket.writeFixedInt(b, 0, 2); // filler
        return b.toByteArray();
    }

    static byte[] textRow(List<Object> row) {
        ByteArrayOutputStream b = new ByteArrayOutputStream();
        for (Object value : row) {
            if (value == null) {
                b.write(0xfb);
            } else {
                MySqlPacket.writeLenEncString(b, String.valueOf(value));
            }
        }
        return b.toByteArray();
    }

    /** Every column reported as VAR_STRING (text format is used for all values regardless), mirroring PgMessages's text-OID fallback. */
    private static int mysqlTypeFor(int jdbcType) {
        return switch (jdbcType) {
            case Types.TINYINT -> 0x01;
            case Types.SMALLINT -> 0x02;
            case Types.INTEGER -> 0x03;
            case Types.BIGINT -> 0x08;
            case Types.DOUBLE, Types.FLOAT, Types.REAL -> 0x05;
            case Types.DATE -> 0x0a;
            case Types.TIMESTAMP -> 0x0c;
            default -> 0xfd; // VAR_STRING
        };
    }

    private MySqlMessages() {
    }
}
