package com.sayonora.wire.bigtablewire;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import com.sayonora.wire.bigtablewire.admin.v2.BigtableTableAdminGrpc;
import com.sayonora.wire.bigtablewire.admin.v2.CreateTableRequest;
import com.sayonora.wire.bigtablewire.admin.v2.DeleteTableRequest;
import com.sayonora.wire.bigtablewire.admin.v2.DropRowRangeRequest;
import com.sayonora.wire.bigtablewire.admin.v2.GetTableRequest;
import com.sayonora.wire.bigtablewire.admin.v2.ListTablesRequest;
import com.sayonora.wire.bigtablewire.admin.v2.ModifyColumnFamiliesRequest;
import com.sayonora.wire.bigtablewire.v2.BigtableGrpc;
import com.sayonora.wire.bigtablewire.v2.CheckAndMutateRowRequest;
import com.sayonora.wire.bigtablewire.v2.MutateRowRequest;
import com.sayonora.wire.bigtablewire.v2.MutateRowsRequest;
import com.sayonora.wire.bigtablewire.v2.MutateRowsResponse;
import com.sayonora.wire.bigtablewire.v2.ReadModifyWriteRowRequest;
import com.sayonora.wire.bigtablewire.v2.ReadRowsRequest;
import com.sayonora.wire.bigtablewire.v2.ReadRowsResponse;
import com.sayonora.wire.bigtablewire.v2.SampleRowKeysRequest;
import com.sayonora.wire.core.BackendRegistry;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Iterator;
import java.util.UUID;

/**
 * In-process access to the Bigtable data and table-admin APIs of bigtablewire for the MCP tools: the same {@link BtGrpc}
 * service definitions the network listener serves, bound to a private in-process gRPC server (no port, no auth), called
 * through blocking stubs with proto3 JSON requests. ReadRows chunks are re-assembled into rows here.
 */
public final class BigtableEmbedded implements AutoCloseable {

    /** A Bigtable API error carrying the gRPC status name. */
    public static final class ApiError extends RuntimeException {
        public final String status;

        ApiError(String status, String message) {
            super(status + ": " + message, null, false, false);
            this.status = status;
        }
    }

    private final Server server;
    private final ManagedChannel channel;
    private final BigtableGrpc.BigtableBlockingStub data;
    private final BigtableTableAdminGrpc.BigtableTableAdminBlockingStub admin;
    private final JsonFormat.Parser parser = JsonFormat.parser().ignoringUnknownFields();
    private final JsonFormat.Printer printer = JsonFormat.printer().omittingInsignificantWhitespace();

    public BigtableEmbedded(BackendRegistry registry) {
        BtConfig cfg = BtConfig.fromEnv();
        BtStore store = new BtStore(new BtShards(registry));
        BtAdmin btAdmin = new BtAdmin(store);
        BtData btData = new BtData(store, btAdmin, cfg);
        String name = "warp-mcp-bigtable-" + UUID.randomUUID();
        InProcessServerBuilder b = InProcessServerBuilder.forName(name).maxInboundMessageSize(256 * 1024 * 1024);
        new BtGrpc(btData, btAdmin, (op, write, nanos) -> { }).services().forEach(b::addService);
        try {
            this.server = b.build().start();
        } catch (java.io.IOException e) {
            throw new IllegalStateException(e);
        }
        this.channel = InProcessChannelBuilder.forName(name).build();
        this.data = BigtableGrpc.newBlockingStub(channel);
        this.admin = BigtableTableAdminGrpc.newBlockingStub(channel);
    }

    private <M extends Message.Builder> M parse(String json, M b) {
        try {
            parser.merge(json == null || json.isBlank() ? "{}" : json, b);
            return b;
        } catch (InvalidProtocolBufferException e) {
            throw new ApiError("INVALID_ARGUMENT", e.getMessage());
        }
    }

    private String print(Message m) {
        try {
            return printer.print(m);
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalStateException(e);
        }
    }

    private static ApiError map(StatusRuntimeException e) {
        String d = e.getStatus().getDescription();
        return new ApiError(e.getStatus().getCode().name(), d == null ? e.getStatus().getCode().name() : d);
    }

    /** Table admin: ListTables, GetTable, CreateTable, DeleteTable, DropRowRange, ModifyColumnFamilies (proto3 JSON in/out). */
    public String admin(String method, String json) {
        try {
            return switch (method) {
                case "ListTables" -> print(admin.listTables(parse(json, ListTablesRequest.newBuilder()).build()));
                case "GetTable" -> print(admin.getTable(parse(json, GetTableRequest.newBuilder()).build()));
                case "CreateTable" -> print(admin.createTable(parse(json, CreateTableRequest.newBuilder()).build()));
                case "DeleteTable" -> print(admin.deleteTable(parse(json, DeleteTableRequest.newBuilder()).build()));
                case "DropRowRange" -> print(admin.dropRowRange(parse(json, DropRowRangeRequest.newBuilder()).build()));
                case "ModifyColumnFamilies" -> print(admin.modifyColumnFamilies(parse(json, ModifyColumnFamiliesRequest.newBuilder()).build()));
                default -> throw new ApiError("UNIMPLEMENTED", "unknown Bigtable admin method " + method);
            };
        } catch (StatusRuntimeException e) {
            throw map(e);
        }
    }

    /** Data API (unary): MutateRow, MutateRows, CheckAndMutateRow, ReadModifyWriteRow, SampleRowKeys (JSON array). */
    public String data(String method, String json) {
        try {
            switch (method) {
                case "MutateRow":
                    return print(data.mutateRow(parse(json, MutateRowRequest.newBuilder()).build()));
                case "MutateRows": {
                    StringBuilder sb = new StringBuilder("[");
                    Iterator<MutateRowsResponse> it = data.mutateRows(parse(json, MutateRowsRequest.newBuilder()).build());
                    boolean first = true;
                    while (it.hasNext()) {
                        sb.append(first ? "" : ",").append(print(it.next()));
                        first = false;
                    }
                    return sb.append(']').toString();
                }
                case "CheckAndMutateRow":
                    return print(data.checkAndMutateRow(parse(json, CheckAndMutateRowRequest.newBuilder()).build()));
                case "ReadModifyWriteRow":
                    return print(data.readModifyWriteRow(parse(json, ReadModifyWriteRowRequest.newBuilder()).build()));
                case "SampleRowKeys": {
                    StringBuilder sb = new StringBuilder("[");
                    var it = data.sampleRowKeys(parse(json, SampleRowKeysRequest.newBuilder()).build());
                    boolean first = true;
                    while (it.hasNext()) {
                        sb.append(first ? "" : ",").append(print(it.next()));
                        first = false;
                    }
                    return sb.append(']').toString();
                }
                default:
                    throw new ApiError("UNIMPLEMENTED", "unknown Bigtable data method " + method);
            }
        } catch (StatusRuntimeException e) {
            throw map(e);
        }
    }

    /**
     * ReadRows with the chunk stream assembled into rows: {@code {"rows":[{"rowKey":..,"cells":[{"family","qualifier",
     * "timestampMicros","value"}]}]}}. {@code json} is a ReadRowsRequest in proto3 JSON (set rowsLimit to bound it).
     */
    public JsonObject readRows(String json) {
        JsonArray rows = new JsonArray();
        try {
            Iterator<ReadRowsResponse> it = data.readRows(parse(json, ReadRowsRequest.newBuilder()).build());
            JsonObject row = null;
            JsonArray cells = null;
            String family = "";
            ByteString qualifier = ByteString.EMPTY;
            long ts = 0;
            ByteString value = ByteString.EMPTY;
            boolean inCell = false;
            while (it.hasNext()) {
                for (ReadRowsResponse.CellChunk c : it.next().getChunksList()) {
                    if (c.getRowKey().size() > 0) {
                        row = new JsonObject();
                        cells = new JsonArray();
                        row.add("rowKey", text(c.getRowKey()));
                        row.add("cells", cells);
                    }
                    if (c.hasFamilyName()) {
                        family = c.getFamilyName().getValue();
                    }
                    if (c.hasQualifier()) {
                        qualifier = c.getQualifier().getValue();
                    }
                    if (!inCell) {
                        ts = c.getTimestampMicros();
                        inCell = true;
                    }
                    value = value.concat(c.getValue());
                    if (c.getValueSize() == 0 && cells != null) {
                        JsonObject cell = new JsonObject();
                        cell.addProperty("family", family);
                        cell.add("qualifier", text(qualifier));
                        cell.addProperty("timestampMicros", ts);
                        cell.add("value", text(value));
                        cells.add(cell);
                        value = ByteString.EMPTY;
                        inCell = false;
                    }
                    if (c.getResetRow()) {
                        row = null;
                        cells = null;
                        value = ByteString.EMPTY;
                        inCell = false;
                    } else if (c.getCommitRow() && row != null) {
                        rows.add(row);
                        row = null;
                        cells = null;
                    }
                }
            }
        } catch (StatusRuntimeException e) {
            throw map(e);
        }
        JsonObject out = new JsonObject();
        out.add("rows", rows);
        return out;
    }

    /** UTF-8 text when valid, else {"base64": "..."}. */
    public static com.google.gson.JsonElement text(ByteString v) {
        try {
            return new com.google.gson.JsonPrimitive(StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT).decode(v.asReadOnlyByteBuffer()).toString());
        } catch (CharacterCodingException e) {
            JsonObject o = new JsonObject();
            o.addProperty("base64", Base64.getEncoder().encodeToString(v.toByteArray()));
            return o;
        }
    }

    @Override
    public void close() {
        channel.shutdownNow();
        server.shutdownNow();
    }
}
