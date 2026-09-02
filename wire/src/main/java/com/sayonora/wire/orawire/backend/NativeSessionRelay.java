package com.sayonora.wire.orawire.backend;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class NativeSessionRelay {

    private static final Logger LOG = Logger.getLogger(NativeSessionRelay.class.getName());

    private NativeSessionRelay() {
    }

    public static void relay(Socket clientSocket, String backendHost, int backendPort) throws IOException {
        try (Socket backendSocket = new Socket(backendHost, backendPort)) {
            InputStream clientIn = clientSocket.getInputStream();
            OutputStream clientOut = clientSocket.getOutputStream();
            InputStream backendIn = backendSocket.getInputStream();
            OutputStream backendOut = backendSocket.getOutputStream();

            Thread c2b = new Thread(() -> pump(clientIn, backendOut, backendSocket), "native-session-relay-c2b");
            c2b.setDaemon(true);
            c2b.start();

            pump(backendIn, clientOut, clientSocket);
            try {
                c2b.join(2000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private static void pump(InputStream in, OutputStream out, Socket peerToCloseOnExit) {
        byte[] buf = new byte[16384];
        try {
            int n;
            while ((n = in.read(buf)) != -1) {
                out.write(buf, 0, n);
                out.flush();
            }
        } catch (IOException e) {
            LOG.log(Level.FINE, "native session relay side ended", e);
        } finally {
            try {
                peerToCloseOnExit.close();
            } catch (IOException ignored) {
            }
        }
    }
}
