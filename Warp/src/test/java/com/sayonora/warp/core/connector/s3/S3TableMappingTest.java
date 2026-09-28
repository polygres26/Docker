package com.sayonora.warp.core.connector.s3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;

/** Pure row/type-mapping coverage for {@link S3Table}'s CSV/XML paths -- no real S3 bucket needed,
 * a fake {@link ObjectFetcher} stands in for the download. Real Parquet pushdown (structural
 * {@code FilterPredicate} translation) is exercised in {@code S3SchemaTest} against a real MinIO
 * container instead -- generating a valid in-memory Parquet fixture without a container/writer
 * helper is out of scope for this pure unit test. */
class S3TableMappingTest {

    private static ObjectFetcher fetcherFor(byte[] bytes) {
        return key -> bytes;
    }

    @Test
    void csvParsesHeaderAndRowsAsVarcharColumns() {
        byte[] csv = "id,name\n1,alpha\n2,beta\n".getBytes(StandardCharsets.UTF_8);
        S3Table table = new S3Table(fetcherFor(csv), "widgets.csv", "csv", null, Set.of());

        RelDataType rowType = table.getRowType(new JavaTypeFactoryImpl());
        assertEquals(List.of("id", "name"), rowType.getFieldNames());
        rowType.getFieldList().forEach(f -> assertEquals(SqlTypeName.VARCHAR, f.getType().getSqlTypeName()));

        Enumerable<Object[]> rows = table.scan(null, new ArrayList<>(), null);
        List<Object[]> collected = new ArrayList<>();
        rows.forEach(collected::add);
        assertEquals(2, collected.size());
        assertEquals("1", collected.get(0)[0]);
        assertEquals("alpha", collected.get(0)[1]);
        assertEquals("2", collected.get(1)[0]);
        assertEquals("beta", collected.get(1)[1]);
    }

    @Test
    void xmlParsesDeclaredRecordElementIntoRows() {
        byte[] xml = ("<root><item><id>1</id><name>alpha</name></item>"
                + "<item><id>2</id><name>beta</name></item></root>").getBytes(StandardCharsets.UTF_8);
        S3Table table = new S3Table(fetcherFor(xml), "widgets.xml", "xml", "item", Set.of());

        RelDataType rowType = table.getRowType(new JavaTypeFactoryImpl());
        assertEquals(Set.of("id", "name"), Set.copyOf(rowType.getFieldNames()));

        Enumerable<Object[]> rows = table.scan(null, new ArrayList<>(), null);
        int[] count = {0};
        rows.forEach(r -> count[0]++);
        assertEquals(2, count[0]);
    }

    @Test
    void xmlWithoutRecordElementFailsClearlyRatherThanReturningEmpty() {
        byte[] xml = "<root><item><id>1</id></item></root>".getBytes(StandardCharsets.UTF_8);
        S3Table table = new S3Table(fetcherFor(xml), "widgets.xml", "xml", null, Set.of());
        RuntimeException e = assertThrows(RuntimeException.class, () -> table.getRowType(new JavaTypeFactoryImpl()));
        assertTrue(e.getMessage().contains("recordElement"), e.getMessage());
    }

    @Test
    void csvIsCachedAfterFirstParseNotReDownloadedOrReParsed() {
        int[] fetchCount = {0};
        byte[] csv = "id\n1\n".getBytes(StandardCharsets.UTF_8);
        ObjectFetcher countingFetcher = key -> {
            fetchCount[0]++;
            return csv;
        };
        S3Table table = new S3Table(countingFetcher, "k.csv", "csv", null, Set.of());
        table.scan(null, new ArrayList<>(), null).forEach(r -> { });
        table.scan(null, new ArrayList<>(), null).forEach(r -> { });
        assertEquals(1, fetchCount[0], "the object is fetched once, cached rows reused on the second scan");
    }
}
