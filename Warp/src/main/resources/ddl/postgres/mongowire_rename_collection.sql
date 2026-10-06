-- mongowire: renameCollection (PostgresDocumentStore). ${from} is a qualified table name, ${to} a quoted unqualified one.
-- ### rename
ALTER TABLE ${from} RENAME TO ${to}
