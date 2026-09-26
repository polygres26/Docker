package com.sayonora.wire.azurewire;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Row models of the blob store: containers, blobs, segments and leases. */
final class BlobModel {

    private BlobModel() {
    }

    /** Lease columns shared by containers and blobs. */
    static final class Lease {
        String state = "available"; // available | leased | breaking | broken
        String id;
        int duration; // seconds, -1 infinite, 0 none
        Instant expiry; // end of a fixed-duration lease
        Instant breakAt; // end of the break period

        /** Effective state at {@code now}: a fixed lease past its expiry is "expired", a break past its period "broken". */
        String effective(Instant now) {
            switch (state) {
                case "leased":
                    if (duration > 0 && expiry != null && !now.isBefore(expiry)) {
                        return "expired";
                    }
                    return "leased";
                case "breaking":
                    if (breakAt != null && !now.isBefore(breakAt)) {
                        return "broken";
                    }
                    return "breaking";
                default:
                    return state;
            }
        }

        boolean locked(Instant now) {
            String e = effective(now);
            return e.equals("leased") || e.equals("breaking");
        }

        /** Normalises the stored state to its effective one (called before evaluating an action). */
        void settle(Instant now) {
            String e = effective(now);
            if (e.equals("expired")) {
                state = "expired";
                id = null == id ? null : id; // an expired lease id can still be re-acquired/renewed until re-leased
            } else if (e.equals("broken")) {
                state = "broken";
            }
        }
    }

    static final class Cont {
        String account;
        String name;
        Instant createdAt = Instant.now();
        Instant lastModified = Instant.now();
        String etag;
        Map<String, String> metadata = new LinkedHashMap<>();
        String publicAccess = "";
        List<AzureAuth.Policy> acl = new ArrayList<>();
        Lease lease = new Lease();
    }

    /** One stored segment: a chunked data blob (block / append block) or, for page blobs, a slice at a logical offset. */
    static final class Seg {
        String id; // block id (base64) for block blobs, null otherwise
        String d; // data id (uuid)
        long n; // length of the segment
        long o; // logical offset (page blobs only)
        long s; // start offset inside the data (page blobs only)
    }

    static final class Blob {
        String account;
        String container;
        String name;
        String snapshot = "";
        String type = "BlockBlob";
        long size;
        String etag;
        Instant createdAt = Instant.now();
        Instant lastModified = Instant.now();
        String contentType;
        String contentEncoding;
        String contentLanguage;
        String contentMd5;
        String cacheControl;
        String contentDisposition;
        Map<String, String> metadata = new LinkedHashMap<>();
        Map<String, String> tags = new LinkedHashMap<>();
        String tier;
        boolean tierInferred = true;
        Instant tierChanged;
        List<Seg> segments = new ArrayList<>();
        boolean sealed;
        long seq;
        Lease lease = new Lease();
        String copyId;
        String copySource;
        String copyStatus;
        Instant copyCompletion;

        Blob copyShallow() {
            Blob b = new Blob();
            b.account = account;
            b.container = container;
            b.name = name;
            b.snapshot = snapshot;
            b.type = type;
            b.size = size;
            b.etag = etag;
            b.createdAt = createdAt;
            b.lastModified = lastModified;
            b.contentType = contentType;
            b.contentEncoding = contentEncoding;
            b.contentLanguage = contentLanguage;
            b.contentMd5 = contentMd5;
            b.cacheControl = cacheControl;
            b.contentDisposition = contentDisposition;
            b.metadata = new LinkedHashMap<>(metadata);
            b.tags = new LinkedHashMap<>(tags);
            b.tier = tier;
            b.tierInferred = tierInferred;
            b.tierChanged = tierChanged;
            b.segments = new ArrayList<>(segments);
            b.sealed = sealed;
            b.seq = seq;
            return b;
        }
    }
}
