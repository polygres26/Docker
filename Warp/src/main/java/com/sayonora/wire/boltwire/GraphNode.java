package com.sayonora.wire.boltwire;

import java.util.List;
import java.util.Map;

/** A property-graph node, ready to encode as Bolt's own real Node struct (PackStream tag
 * {@code 0x4E}/{@code 'N'}). This server only ever negotiates Bolt 4.4 (see
 * {@code BoltWireSessionHandler#performHandshake}'s own javadoc), whose own Node struct has 3
 * fields -- id, labels, properties, no elementId -- see {@link PackStream.Writer#writeNode}'s own
 * javadoc for the real capture (against a fresh Neo4j 5.26 server, negotiating Bolt 5.8 via the
 * newer "manifest" handshake extension this server doesn't implement) that this was originally,
 * incorrectly grounded in, and the real Bolt-4.4-session capture that corrected it.
 *
 * @param id the real, generated {@code warp_graph_nodes.id} -- Bolt's own legacy integer id
 *     field, still every client's own way to reference this node (e.g. for a follow-up query)
 *     since Bolt 4.4 has no elementId concept at all
 * @param elementId a Warp-native element id ({@code "warp-node:" + id}), kept as an
 *     honest internal identifier and available for a possible future Bolt 5.x mode, but not
 *     currently written to the wire (see {@link PackStream.Writer#writeNode})
 */
record GraphNode(long id, List<String> labels, Map<String, Object> properties, String elementId) {

    static String elementId(long id) {
        return "warp-node:" + id;
    }
}
