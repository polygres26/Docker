package com.sayonora.wire.core.connector.s3;

/**
 * The one seam every object-storage fetch goes through -- everything above this ({@link S3Table}'s
 * CSV/XML/Parquet parsing) only ever needs "give me the bytes for this key," never anything
 * provider-specific. Ported from the sibling ThinkingSense project's identically-named interface;
 * Warp's own port only ships {@link S3CompatibleObjectFetcher} (AWS S3 and every S3-compatible
 * surface -- MinIO, Wasabi, OCI/GCS interop), not ThinkingSense's Azure Blob/local-file variants,
 * matching this connector's approved scope.
 */
interface ObjectFetcher {
    byte[] fetch(String key);
}
