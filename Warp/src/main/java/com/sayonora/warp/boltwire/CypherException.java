package com.sayonora.warp.boltwire;

/** A Cypher failure carrying the Neo4j status code a Bolt FAILURE message needs
 * (Neo.ClientError.Statement.SyntaxError, ...TypeError, ...ArithmeticError, ...). */
final class CypherException extends RuntimeException {

    static final String SYNTAX = "Neo.ClientError.Statement.SyntaxError";
    static final String TYPE = "Neo.ClientError.Statement.TypeError";
    static final String ARITHMETIC = "Neo.ClientError.Statement.ArithmeticError";
    static final String ARGUMENT = "Neo.ClientError.Statement.ArgumentError";
    static final String PARAM_MISSING = "Neo.ClientError.Statement.ParameterMissing";
    static final String ENTITY_NOT_FOUND = "Neo.ClientError.Statement.EntityNotFound";
    static final String CONSTRAINT = "Neo.ClientError.Schema.ConstraintValidationFailed";
    static final String SEMANTIC = "Neo.ClientError.Statement.SemanticError";
    static final String EXEC_FAILED = "Neo.ClientError.Statement.ExecutionFailed";
    static final String INVALID_TX = "Neo.ClientError.Transaction.TransactionNotFound";
    static final String NOT_SUPPORTED = "Neo.DatabaseError.Statement.ExecutionFailed";

    private final String code;

    CypherException(String message) {
        this(SYNTAX, message);
    }

    CypherException(String code, String message) {
        super(message);
        this.code = code;
    }

    String code() {
        return code;
    }

    static CypherException syntax(String message) {
        return new CypherException(SYNTAX, message);
    }

    static CypherException type(String message) {
        return new CypherException(TYPE, message);
    }

    static CypherException arithmetic(String message) {
        return new CypherException(ARITHMETIC, message);
    }

    static CypherException argument(String message) {
        return new CypherException(ARGUMENT, message);
    }
}
