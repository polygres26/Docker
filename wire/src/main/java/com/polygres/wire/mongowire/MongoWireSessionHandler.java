package com.polygres.wire.mongowire;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.OutputStream;
import java.net.Socket;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import org.bson.BsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Speaks MongoDB's modern wire protocol ({@code OP_MSG} only — see {@link OpMsgFrame}'s javadoc)
 * over a plain TCP socket, backed by real Postgres with documents stored as JSONB. Built as a
 * reference-following port of the design in mongo-java-server's {@code postgresql-backend} module
 * (BSD-3-Clause, https://github.com/bwaldvogel/mongo-java-server/tree/main/postgresql-backend) —
 * database -&gt; Postgres schema, collection -&gt; Postgres table — see {@link PostgresDocumentStore}'s
 * javadoc for exactly what was kept from that reference and what was deliberately changed
 * (real jsonb + indexed {@code _id} column + SQL-level filter pushdown instead of the reference's
 * opaque {@code json} blob + full-table Java-side filtering).
 *
 * <p><b>Deliberately NOT wired through {@code StatementPipeline}/{@code RouterStage}/
 * {@code QosControlStage}/{@code CacheStage}/etc.</b>, unlike every other frontend in this module.
 * That machinery is built entirely around SQL {@code Statement}s (text + bind params) flowing
 * through a pipeline that ends in a JDBC execute — a MongoDB command is not a SQL statement, and
 * forcing {@code insert}/{@code find}/{@code update}/{@code delete} commands through a
 * SQL-statement-shaped {@code Statement} object would mean fabricating SQL text purely so the
 * pipeline's stages have something to look at, with no real QoS/cache/routing semantics gained
 * (a MongoDB filter document isn't SQL the DialectTranslationStage or RollupStage could ever
 * usefully act on) — dishonest complexity for no real benefit. This mirrors the same judgment
 * call made for the sibling dynamowire frontend built in parallel. Instead this class owns a
 * simple, direct path: decode OP_MSG -&gt; {@link MongoCommandDispatcher} -&gt;
 * {@link PostgresDocumentStore} -&gt; plain JDBC against Postgres -&gt; encode OP_MSG reply. QoS/
 * caching/observability for mongowire specifically are consequently not implemented in this pass;
 * said so plainly rather than silently skipped.
 */
public final class MongoWireSessionHandler implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(MongoWireSessionHandler.class);

    private final Socket clientSocket;
    private final MongoCommandDispatcher dispatcher;

    public MongoWireSessionHandler(Socket clientSocket, String pgUrl, String pgUser, String pgPassword) {
        this.clientSocket = clientSocket;
        PostgresDocumentStore store = new PostgresDocumentStore(() -> openConnection(pgUrl, pgUser, pgPassword));
        this.dispatcher = new MongoCommandDispatcher(store);
    }

    private static Connection openConnection(String url, String user, String password) throws SQLException {
        return DriverManager.getConnection(url, user, password);
    }

    @Override
    public void run() {
        try (Socket socket = clientSocket) {
            DataInputStream in = new DataInputStream(socket.getInputStream());
            OutputStream out = socket.getOutputStream();
            while (true) {
                OpMsgFrame frame = OpMsgFrame.read(in);
                BsonDocument reply = dispatcher.dispatch(frame.body);
                OpMsgFrame.writeReply(out, frame.requestId, reply, frame.legacyQuery);
            }
        } catch (EOFException e) {
            // client disconnected — normal session end, not worth logging as a warning
        } catch (Exception e) {
            log.warn("mongowire session terminated: {}", e.getMessage(), e);
        }
    }
}
