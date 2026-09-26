package com.sayonora.warp.mongowire;

import java.util.Map;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.BsonValue;

/** A MongoDB-style command failure: numeric code, codeName and message, plus optional extra reply fields. */
final class MongoCmdException extends RuntimeException {

    private static final Map<Integer, String> NAMES = Map.ofEntries(
            Map.entry(2, "BadValue"), Map.entry(9, "FailedToParse"), Map.entry(11, "UserNotFound"),
            Map.entry(13, "Unauthorized"), Map.entry(14, "TypeMismatch"), Map.entry(18, "AuthenticationFailed"),
            Map.entry(20, "IllegalOperation"), Map.entry(26, "NamespaceNotFound"), Map.entry(27, "IndexNotFound"),
            Map.entry(28, "PathNotViable"), Map.entry(31, "InvalidPipelineOperator"), Map.entry(40, "ConflictingUpdateOperators"),
            Map.entry(43, "CursorNotFound"), Map.entry(48, "NamespaceExists"), Map.entry(50, "MaxTimeMSExpired"),
            Map.entry(51, "InvalidLength"), Map.entry(59, "CommandNotFound"), Map.entry(61, "ShardKeyNotFound"),
            Map.entry(66, "ImmutableField"), Map.entry(67, "CannotCreateIndex"), Map.entry(68, "IndexAlreadyExists"),
            Map.entry(72, "InvalidOptions"), Map.entry(73, "InvalidNamespace"), Map.entry(85, "IndexOptionsConflict"),
            Map.entry(86, "IndexKeySpecsConflict"), Map.entry(89, "NetworkTimeout"), Map.entry(115, "CommandNotSupported"),
            Map.entry(112, "WriteConflict"), Map.entry(121, "DocumentValidationFailure"), Map.entry(125, "CommandFailed"),
            Map.entry(171, "CannotIndexParallelArrays"), Map.entry(16, "InvalidLength"), Map.entry(10334, "BSONObjectTooLarge"),
            Map.entry(11000, "DuplicateKey"), Map.entry(15, "Overflow"), 
            Map.entry(8, "UnknownError"),
            Map.entry(1, "InternalError"), Map.entry(96, "OperationFailed"),
            Map.entry(146, "ExceededMemoryLimit"), Map.entry(240, "DurationNotSupported"),
            Map.entry(6, "HostUnreachable"), Map.entry(40415, "IDLUnknownField"), Map.entry(168, "InvalidPipelineOperator"), Map.entry(224, "Location224"), Map.entry(51024, "Location51024"), Map.entry(91, "ShutdownInProgress"));

    final int code;
    final String codeName;
    final BsonDocument extra;

    MongoCmdException(int code, String message) {
        this(code, message, null);
    }

    MongoCmdException(int code, String message, BsonDocument extra) {
        super(message);
        this.code = code;
        this.codeName = NAMES.getOrDefault(code, "Location" + code);
        this.extra = extra;
    }

    static MongoCmdException badValue(String msg) {
        return new MongoCmdException(2, msg);
    }

    static MongoCmdException failedToParse(String msg) {
        return new MongoCmdException(9, msg);
    }

    static MongoCmdException typeMismatch(String msg) {
        return new MongoCmdException(14, msg);
    }

    static MongoCmdException location(int code, String msg) {
        return new MongoCmdException(code, msg);
    }

    BsonDocument toReply() {
        BsonDocument doc = new BsonDocument();
        doc.put("ok", new org.bson.BsonDouble(0.0));
        doc.put("errmsg", new BsonString(getMessage()));
        doc.put("code", new BsonInt32(code));
        doc.put("codeName", new BsonString(codeName));
        if (extra != null) {
            for (Map.Entry<String, BsonValue> e : extra.entrySet()) {
                doc.put(e.getKey(), e.getValue());
            }
        }
        return doc;
    }
}
