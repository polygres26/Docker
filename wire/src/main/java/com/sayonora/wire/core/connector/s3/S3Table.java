package com.sayonora.wire.core.connector.s3;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ProjectableFilterableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * One S3 object exposed as a queryable SQL table -- ported from the sibling ThinkingSense project's
 * real, tested {@code com.omnigate.calcite.s3.S3Table} (same "download once per connection, let
 * Calcite's own query engine run real SQL over the rows" design). <b>Real, structural predicate
 * pushdown -- Parquet only</b> (this class implements {@link ProjectableFilterableTable}): a
 * {@code column = literal} predicate on a column named in {@link #pushdownColumns} is translated
 * into a real Parquet {@link FilterPredicate}, type-matched against that column's actual physical
 * Parquet type, and handed to {@code parquet-hadoop} itself via {@code ParquetReadOptions} -- real
 * row-group/dictionary-stats skip, not a Calcite-side filter. CSV/XML have no such structure: every
 * call parses the whole object (cached after the first parse).
 *
 * <p>Not a {@code DynamicallyFilterableTable} here -- that ThinkingSense-only optimization interface
 * has no Warp analog; this port keeps only the real, load-bearing {@link ProjectableFilterableTable}
 * contract.
 */
final class S3Table extends AbstractTable implements ProjectableFilterableTable {

    private final ObjectFetcher fetcher;
    private final String key;
    private final String format;
    private final String recordElement;
    private final Set<String> pushdownColumns;

    private volatile byte[] rawData;
    private volatile List<String> columns;
    /** Parquet only -- {@code null} for csv/xml. */
    private volatile MessageType parquetSchema;
    /** Caches the fully-decoded (unfiltered) Parquet row list once real pushdown doesn't apply --
     * see {@link #parseParquet}'s own javadoc for exactly when that is. {@code volatile}, not
     * {@code synchronized}: a small, accepted race (two concurrent first scans could both decode
     * once), never a correctness issue. */
    private volatile List<Map<String, Object>> fullyDecodedParquetRowsCache;
    private volatile List<Map<String, Object>> nonParquetRowsCache;

    /** @param format one of {@code "csv"}, {@code "xml"}, {@code "parquet"} (case-insensitive).
     * @param recordElement required (and only meaningful) for {@code "xml"}.
     * @param pushdownColumns column names eligible for {@code column = literal} predicate pushdown
     *     -- meaningful (and only implemented) for {@code "parquet"}. */
    S3Table(ObjectFetcher fetcher, String key, String format, String recordElement, Set<String> pushdownColumns) {
        this.fetcher = fetcher;
        this.key = key;
        this.format = format;
        this.recordElement = recordElement;
        this.pushdownColumns = pushdownColumns == null ? Set.of() : Set.copyOf(pushdownColumns);
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        ensureDownloaded();
        RelDataTypeFactory.Builder builder = typeFactory.builder();
        // Parquet objects carry a real physical+logical schema -- honor it instead of declaring
        // every column VARCHAR (which breaks DATE comparisons/EXTRACT). CSV/XML have no such
        // schema -- every value from those parsers is already a plain String, so VARCHAR is correct.
        boolean isParquet = "parquet".equalsIgnoreCase(format);
        for (String column : columns) {
            SqlTypeName sqlType = isParquet ? sqlTypeFor(parquetSchema.getType(column)) : SqlTypeName.VARCHAR;
            builder.add(column, typeFactory.createSqlType(sqlType));
        }
        return builder.build();
    }

    private static SqlTypeName sqlTypeFor(Type type) {
        if (!type.isPrimitive()) {
            return SqlTypeName.VARCHAR;
        }
        PrimitiveType primitiveType = type.asPrimitiveType();
        LogicalTypeAnnotation logicalType = primitiveType.getLogicalTypeAnnotation();
        if (logicalType instanceof LogicalTypeAnnotation.DateLogicalTypeAnnotation
                || primitiveType.getOriginalType() == OriginalType.DATE) {
            return SqlTypeName.DATE;
        }
        if (logicalType instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) {
            return SqlTypeName.TIMESTAMP;
        }
        return switch (primitiveType.getPrimitiveTypeName()) {
            case INT32 -> SqlTypeName.INTEGER;
            case INT64 -> SqlTypeName.BIGINT;
            case DOUBLE -> SqlTypeName.DOUBLE;
            case FLOAT -> SqlTypeName.FLOAT;
            case BOOLEAN -> SqlTypeName.BOOLEAN;
            default -> SqlTypeName.VARCHAR;
        };
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root, List<RexNode> filters, int[] projects) {
        ensureDownloaded();
        List<Map<String, Object>> rows = "parquet".equalsIgnoreCase(format)
                ? parseParquet(rawData, filters) // may mutate filters, removing what it pushed down
                : parseNonParquet(rawData);       // csv/xml: no pushdown capability, filters untouched

        int[] columnIndexes = projects != null ? projects : identityProjection();
        List<Object[]> result = new ArrayList<>(rows.size());
        for (Map<String, Object> row : rows) {
            Object[] values = new Object[columnIndexes.length];
            for (int i = 0; i < columnIndexes.length; i++) {
                values[i] = row.get(columns.get(columnIndexes[i]));
            }
            result.add(values);
        }
        return Linq4j.asEnumerable(result);
    }

    private int[] identityProjection() {
        int[] projection = new int[columns.size()];
        for (int i = 0; i < projection.length; i++) {
            projection[i] = i;
        }
        return projection;
    }

    private synchronized void ensureDownloaded() {
        if (rawData != null) {
            return;
        }
        rawData = fetcher.fetch(key);
        if ("parquet".equalsIgnoreCase(format)) {
            parquetSchema = readParquetSchema(rawData);
            columns = new ArrayList<>(parquetSchema.getFieldCount());
            for (int i = 0; i < parquetSchema.getFieldCount(); i++) {
                columns.add(parquetSchema.getFieldName(i));
            }
        } else {
            List<Map<String, Object>> rows = parseNonParquet(rawData);
            LinkedHashSet<String> discovered = new LinkedHashSet<>();
            for (Map<String, Object> row : rows) {
                discovered.addAll(row.keySet());
            }
            columns = new ArrayList<>(discovered);
        }
    }

    private List<Map<String, Object>> parseNonParquet(byte[] data) {
        List<Map<String, Object>> cached = nonParquetRowsCache;
        if (cached != null) {
            return cached;
        }
        List<Map<String, Object>> rows = "xml".equalsIgnoreCase(format) ? parseXml(data) : parseCsv(data);
        nonParquetRowsCache = rows;
        return rows;
    }

    private List<Map<String, Object>> parseCsv(byte[] data) {
        try (CSVParser parser = CSVParser.parse(
                new InputStreamReader(new ByteArrayInputStream(data), StandardCharsets.UTF_8),
                CSVFormat.DEFAULT.builder().setHeader().setSkipHeaderRecord(true).get())) {
            List<Map<String, Object>> rows = new ArrayList<>();
            for (CSVRecord record : parser) {
                Map<String, Object> row = new LinkedHashMap<>();
                record.toMap().forEach(row::put);
                rows.add(row);
            }
            return rows;
        } catch (IOException e) {
            throw new S3FetchException("Failed to parse CSV object " + key, e);
        }
    }

    private List<Map<String, Object>> parseXml(byte[] data) {
        if (recordElement == null || recordElement.isBlank()) {
            throw new S3FetchException("XML table for key " + key + " requires a configured 'recordElement'");
        }
        try {
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true); // XXE hardening
            DocumentBuilder builder = factory.newDocumentBuilder();
            org.w3c.dom.Document doc = builder.parse(new ByteArrayInputStream(data));
            NodeList records = doc.getElementsByTagName(recordElement);
            List<Map<String, Object>> rows = new ArrayList<>();
            for (int i = 0; i < records.getLength(); i++) {
                Element recordEl = (Element) records.item(i);
                Map<String, Object> row = new LinkedHashMap<>();
                NodeList children = recordEl.getChildNodes();
                for (int j = 0; j < children.getLength(); j++) {
                    Node child = children.item(j);
                    if (child.getNodeType() == Node.ELEMENT_NODE) {
                        row.put(child.getNodeName(), child.getTextContent());
                    }
                }
                rows.add(row);
            }
            return rows;
        } catch (Exception e) {
            throw new S3FetchException("Failed to parse XML object " + key, e);
        }
    }

    private static MessageType readParquetSchema(byte[] data) {
        ByteArrayInputFile inputFile = new ByteArrayInputFile(data);
        org.apache.parquet.conf.ParquetConfiguration configuration = new org.apache.parquet.conf.PlainParquetConfiguration();
        org.apache.parquet.ParquetReadOptions options = org.apache.parquet.ParquetReadOptions.builder(configuration).build();
        try (org.apache.parquet.hadoop.ParquetFileReader fileReader =
                org.apache.parquet.hadoop.ParquetFileReader.open(inputFile, options)) {
            return fileReader.getFileMetaData().getSchema();
        } catch (IOException e) {
            throw new S3FetchException("Failed to read Parquet schema", e);
        }
    }

    /** @param filters mutated per the {@link ProjectableFilterableTable} contract: any predicate
     * turned into a real Parquet {@link FilterPredicate} is removed. Caches the fully-decoded
     * unfiltered row list whenever no real predicate applies (see field javadoc) -- stays off when
     * a real predicate is pushed, since that path deliberately does selective I/O. */
    private List<Map<String, Object>> parseParquet(byte[] data, List<RexNode> filters) {
        FilterPredicate predicate = buildPushdownPredicate(filters);
        if (predicate == null) {
            List<Map<String, Object>> cached = fullyDecodedParquetRowsCache;
            if (cached != null) {
                return cached;
            }
        }
        ByteArrayInputFile inputFile = new ByteArrayInputFile(data);
        org.apache.parquet.conf.ParquetConfiguration configuration = new org.apache.parquet.conf.PlainParquetConfiguration();
        org.apache.parquet.ParquetReadOptions.Builder optionsBuilder = org.apache.parquet.ParquetReadOptions.builder(configuration);
        if (predicate != null) {
            optionsBuilder.withRecordFilter(FilterCompat.get(predicate)).useStatsFilter(true).useDictionaryFilter(true);
        }
        org.apache.parquet.ParquetReadOptions options = optionsBuilder.build();
        FilterCompat.Filter recordFilter = predicate != null ? FilterCompat.get(predicate) : FilterCompat.NOOP;
        try (org.apache.parquet.hadoop.ParquetFileReader fileReader =
                org.apache.parquet.hadoop.ParquetFileReader.open(inputFile, options)) {
            MessageType schema = fileReader.getFileMetaData().getSchema();
            GroupReadSupport readSupport = new GroupReadSupport();
            ReadSupport.ReadContext readContext = readSupport.init(configuration, Map.of(), schema);
            RecordMaterializer<Group> materializer =
                    readSupport.prepareForRead(configuration, Map.of(), schema, readContext);
            MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);

            List<Map<String, Object>> rows = new ArrayList<>();
            PageReadStore pages = fileReader.readNextRowGroup();
            while (pages != null) {
                long rowCount = pages.getRowCount();
                RecordReader<Group> recordReader = columnIO.getRecordReader(pages, materializer, recordFilter);
                for (long i = 0; i < rowCount; i++) {
                    Group group = recordReader.read();
                    if (!recordReader.shouldSkipCurrentRecord()) {
                        rows.add(flatten(group, schema));
                    }
                }
                pages = fileReader.readNextRowGroup();
            }
            if (predicate == null) {
                fullyDecodedParquetRowsCache = rows;
            }
            return rows;
        } catch (IOException e) {
            throw new S3FetchException("Failed to read Parquet object " + key, e);
        }
    }

    /** {@code null} if no filter in the list is a pushable equality predicate; otherwise the AND of
     * every one found, each removed from {@code filters} as it's consumed. */
    private FilterPredicate buildPushdownPredicate(List<RexNode> filters) {
        if (pushdownColumns.isEmpty() || filters.isEmpty()) {
            return null;
        }
        FilterPredicate combined = null;
        Iterator<RexNode> iterator = filters.iterator();
        while (iterator.hasNext()) {
            RexNode filter = iterator.next();
            FilterPredicate predicate = asPushablePredicate(filter);
            if (predicate == null) {
                continue;
            }
            combined = combined == null ? predicate : FilterApi.and(combined, predicate);
            iterator.remove();
        }
        return combined;
    }

    private FilterPredicate asPushablePredicate(RexNode filter) {
        if (!(filter instanceof RexCall call) || call.getKind() != SqlKind.EQUALS) {
            return null;
        }
        RexNode left = call.getOperands().get(0);
        RexNode right = call.getOperands().get(1);
        RexInputRef ref = left instanceof RexInputRef r ? r : (right instanceof RexInputRef r2 ? r2 : null);
        RexLiteral literal = left instanceof RexLiteral l ? l : (right instanceof RexLiteral l2 ? l2 : null);
        if (ref == null || literal == null) {
            return null;
        }
        String column = columns.get(ref.getIndex());
        if (!pushdownColumns.contains(column)) {
            return null;
        }
        Object value = literal.getValue2();
        if (value == null) {
            return null;
        }
        PrimitiveType primitiveType = parquetSchema.getType(column).asPrimitiveType();
        return switch (primitiveType.getPrimitiveTypeName()) {
            case INT32 -> FilterApi.eq(FilterApi.intColumn(column), Integer.valueOf(String.valueOf(value)));
            case INT64 -> FilterApi.eq(FilterApi.longColumn(column), Long.valueOf(String.valueOf(value)));
            case DOUBLE -> FilterApi.eq(FilterApi.doubleColumn(column), Double.valueOf(String.valueOf(value)));
            case FLOAT -> FilterApi.eq(FilterApi.floatColumn(column), Float.valueOf(String.valueOf(value)));
            case BOOLEAN -> FilterApi.eq(FilterApi.booleanColumn(column), Boolean.valueOf(String.valueOf(value)));
            case BINARY -> FilterApi.eq(FilterApi.binaryColumn(column), Binary.fromString(String.valueOf(value)));
            default -> null;
        };
    }

    private static Map<String, Object> flatten(Group group, MessageType schema) {
        Map<String, Object> row = new LinkedHashMap<>();
        for (int i = 0; i < schema.getFieldCount(); i++) {
            String fieldName = schema.getFieldName(i);
            int repetitionCount = group.getFieldRepetitionCount(i);
            row.put(fieldName, repetitionCount > 0 ? readValue(group, i, schema.getType(i)) : null);
        }
        return row;
    }

    private static Object readValue(Group group, int fieldIndex, Type fieldType) {
        if (!fieldType.isPrimitive()) {
            return group.getValueToString(fieldIndex, 0);
        }
        return switch (fieldType.asPrimitiveType().getPrimitiveTypeName()) {
            case INT32 -> group.getInteger(fieldIndex, 0);
            case INT64 -> group.getLong(fieldIndex, 0);
            case DOUBLE -> group.getDouble(fieldIndex, 0);
            case FLOAT -> group.getFloat(fieldIndex, 0);
            case BOOLEAN -> group.getBoolean(fieldIndex, 0);
            default -> stripTrailingPadding(group.getValueToString(fieldIndex, 0));
        };
    }

    private static String stripTrailingPadding(String value) {
        if (value == null) {
            return null;
        }
        int end = value.length();
        while (end > 0 && value.charAt(end - 1) == ' ') {
            end--;
        }
        return end == value.length() ? value : value.substring(0, end);
    }

    static final class S3FetchException extends RuntimeException {
        S3FetchException(String message) {
            super(message);
        }

        S3FetchException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
