package com.polygres.wire.boltwire;

import java.util.List;
import java.util.Map;

/** A property-graph node, ready to encode as Bolt's own real Node struct (PackStream tag
 * {@code 0x4E}/{@code 'N'}, 4 fields: id, labels, properties, elementId -- confirmed against a
 * genuine {@code CREATE (n:Person {...}) RETURN n} capture against a real Neo4j 5.26 server; see
 * {@link PackStream}'s own javadoc for the capture this whole package is grounded in).
 *
 * @param id the real, generated {@code polywire_graph_nodes.id} -- Bolt's own legacy integer id
 *     field, kept for wire-format completeness even though real Neo4j itself now considers it
 *     deprecated in favor of {@code elementId}
 * @param elementId a PolyWire-native element id, deliberately NOT shaped like a real Neo4j
 *     element id (Neo4j's own is an opaque, UUID-based, database-internal token) -- honest about
 *     its real origin ({@code "polywire-node:" + id}) rather than fabricating a fake Neo4j-looking
 *     one. A client never parses this string's format, only stores and echoes it back verbatim in
 *     later calls, so this doesn't break wire compatibility.
 */
record GraphNode(long id, List<String> labels, Map<String, Object> properties, String elementId) {

    static String elementId(long id) {
        return "polywire-node:" + id;
    }
}
