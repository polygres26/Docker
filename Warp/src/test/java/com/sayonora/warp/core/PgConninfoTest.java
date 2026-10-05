package com.sayonora.warp.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class PgConninfoTest {

    @Test
    void replacesHostAndPortKeepingEverythingElse() {
        assertEquals("user=repl password=pw host=new port=5433 sslmode=require application_name=r1",
                PgConninfo.withHostPort("user=repl password=pw host=old port=5432 sslmode=require application_name=r1",
                        "new", 5433));
    }

    @Test
    void addsMissingHostAndPortAndDropsHostaddr() {
        assertEquals("user=repl host=new port=5432", PgConninfo.withHostPort("user=repl hostaddr=10.0.0.1", "new", 5432));
    }

    @Test
    void preservesQuotedValuesWithSpacesAndQuotes() {
        String out = PgConninfo.withHostPort("user=repl password='p w\\'d' host=old", "new", 5432);
        assertEquals("p w'd", PgConninfo.parse(out).stream().filter(kv -> kv[0].equals("password")).findFirst().get()[1]);
        assertEquals("new", PgConninfo.parse(out).stream().filter(kv -> kv[0].equals("host")).findFirst().get()[1]);
    }

    @Test
    void rejectsMalformedInput() {
        assertThrows(IllegalArgumentException.class, () -> PgConninfo.parse("nonsense"));
        assertThrows(IllegalArgumentException.class, () -> PgConninfo.parse("a='unterminated"));
    }
}
