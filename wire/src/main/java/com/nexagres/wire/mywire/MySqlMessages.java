package com.nexagres.wire.mywire;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Types;
import java.util.List;

final class MySqlMessages {

    static final int CLIENT_PROTOCOL_41 = 0x00000200;
    static final int CLIENT_SECURE_CONNECTION = 0x00008000;
    static final int CLIENT_PLUGIN_AUTH = 0x00080000;
    static final int CLIENT_CONNECT_WITH_DB = 0x00000008;
    
    static final int CLIENT_SSL = 0x00000800;

    static byte[] handshakeV10(long connectionId, byte[] scramble, boolean tlsSupported) {
        ByteArrayOutputStream b = new ByteArrayOutputStream();
        b.write(10);
        MySqlPacket.writeNulString(b, "8.0.34-polywire");
        MySqlPacket.writeFixedInt(b, connectionId, 4);
        b.write(scramble, 0, 8);
        b.write(0);
        int capabilities = CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION | CLIENT_PLUGIN_AUTH | CLIENT_CONNECT_WITH_DB
                | (tlsSupported ? CLIENT_SSL : 0);
        MySqlPacket.writeFixedInt(b, capabilities & 0xFFFF, 2);
        b.write(0x21);
        MySqlPacket.writeFixedInt(b, 0x0002, 2);
        MySqlPacket.writeFixedInt(b, (capabilities >>> 16) & 0xFFFF, 2);
        b.write(21);
        for (int i = 0; i < 10; i++) {
            b.write(0);
        }
        b.write(scramble, 8, 12);
        b.write(0);
        MySqlPacket.writeNulString(b, "mysql_native_password");
        return b.toByteArray();
    }

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

    /** Forces a client (which may have optimistically guessed a different default plugin for its
     * first auth response -- see {@code MySqlWireSessionHandler.performHandshake}'s javadoc on
     * this) to redo authentication using {@code pluginName} against a fresh challenge. Every
     * spec-compliant client, upon receiving this, sends back a follow-up packet containing only
     * the raw auth-response bytes computed with the requested plugin. */
    static byte[] authSwitchRequest(String pluginName, byte[] pluginData) {
        ByteArrayOutputStream b = new ByteArrayOutputStream();
        b.write(0xfe);
        MySqlPacket.writeNulString(b, pluginName);
        b.write(pluginData, 0, pluginData.length);
        return b.toByteArray();
    }

    static byte[] okPacket(long affectedRows) {
        ByteArrayOutputStream b = new ByteArrayOutputStream();
        b.write(0x00);
        MySqlPacket.writeLenEncInt(b, affectedRows);
        MySqlPacket.writeLenEncInt(b, 0);
        MySqlPacket.writeFixedInt(b, 0x0002, 2);
        MySqlPacket.writeFixedInt(b, 0, 2);
        return b.toByteArray();
    }

    static byte[] eofPacket() {
        ByteArrayOutputStream b = new ByteArrayOutputStream();
        b.write(0xfe);
        MySqlPacket.writeFixedInt(b, 0, 2);
        MySqlPacket.writeFixedInt(b, 0x0002, 2);
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
        MySqlPacket.writeLenEncString(b, "");
        MySqlPacket.writeLenEncString(b, "");
        MySqlPacket.writeLenEncString(b, "");
        MySqlPacket.writeLenEncString(b, name);
        MySqlPacket.writeLenEncString(b, name);
        b.write(0x0c);
        MySqlPacket.writeFixedInt(b, 0x21, 2);
        MySqlPacket.writeFixedInt(b, 0, 4);
        b.write(mysqlTypeFor(jdbcType));
        MySqlPacket.writeFixedInt(b, 0, 2);
        b.write(0);
        MySqlPacket.writeFixedInt(b, 0, 2);
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

    private static int mysqlTypeFor(int jdbcType) {
        return switch (jdbcType) {
            case Types.TINYINT -> 0x01;
            case Types.SMALLINT -> 0x02;
            case Types.INTEGER -> 0x03;
            case Types.BIGINT -> 0x08;
            case Types.DOUBLE, Types.FLOAT, Types.REAL -> 0x05;
            case Types.DATE -> 0x0a;
            case Types.TIMESTAMP -> 0x0c;
            default -> 0xfd;
        };
    }

    private MySqlMessages() {
    }
}
