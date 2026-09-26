package com.sayonora.wire.pubsubwire;

import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.util.JsonFormat;
import com.google.pubsub.v1.Encoding;
import com.google.pubsub.v1.Schema;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;

/**
 * Schema validation. Avro definitions are parsed and messages decoded with the Avro library (JSON and BINARY encodings).
 * Protocol Buffer schemas are validated only when the schema carries {@code compiled_proto_schema} (a FileDescriptorSet and the
 * root message name): parsing {@code .proto} source text needs protoc, which is not available at runtime, so a Protocol Buffer
 * schema given only as text is stored and validated for non-emptiness, and message validation against it is UNIMPLEMENTED.
 */
final class PsSchemas {

    private PsSchemas() {
    }

    static void validateDefinition(Schema s) {
        if (s.getType() == Schema.Type.TYPE_UNSPECIFIED) {
            throw PsException.invalid("Schema type must be specified.");
        }
        boolean compiled = s.hasCompiledProtoSchema();
        if (s.getDefinition().isEmpty() && !compiled) {
            throw PsException.invalid("Schema definition must not be empty.");
        }
        if (s.getType() == Schema.Type.AVRO) {
            try {
                new org.apache.avro.Schema.Parser().parse(s.getDefinition());
            } catch (RuntimeException e) {
                throw PsException.invalid("Invalid schema definition: " + firstLine(e.getMessage()));
            }
        } else if (compiled) {
            descriptorOf(s);
        }
    }

    private static String firstLine(String m) {
        return m == null ? "" : m.split("\n")[0];
    }

    private static Descriptors.Descriptor descriptorOf(Schema s) {
        try {
            DescriptorProtos.FileDescriptorSet set = DescriptorProtos.FileDescriptorSet.parseFrom(s.getCompiledProtoSchema().getCompiledBytes());
            java.util.Map<String, Descriptors.FileDescriptor> built = new java.util.LinkedHashMap<>();
            for (DescriptorProtos.FileDescriptorProto f : set.getFileList()) {
                List<Descriptors.FileDescriptor> deps = new ArrayList<>();
                for (String d : f.getDependencyList()) {
                    Descriptors.FileDescriptor fd = built.get(d);
                    if (fd != null) {
                        deps.add(fd);
                    }
                }
                built.put(f.getName(), Descriptors.FileDescriptor.buildFrom(f, deps.toArray(new Descriptors.FileDescriptor[0])));
            }
            String root = s.getCompiledProtoSchema().getRootMessage();
            for (Descriptors.FileDescriptor fd : built.values()) {
                for (Descriptors.Descriptor d : fd.getMessageTypes()) {
                    if (d.getFullName().equals(root) || d.getName().equals(root)) {
                        return d;
                    }
                }
            }
            throw PsException.invalid("Root message " + root + " not found in the compiled schema.");
        } catch (IOException | Descriptors.DescriptorValidationException e) {
            throw PsException.invalid("Invalid compiled schema: " + firstLine(e.getMessage()));
        }
    }

    /** Throws INVALID_ARGUMENT when {@code data} does not conform. */
    static void validateMessage(Schema s, Encoding enc, ByteString data) {
        if (enc == Encoding.ENCODING_UNSPECIFIED) {
            throw PsException.invalid("Encoding must be specified for a topic with a schema.");
        }
        if (s.getType() == Schema.Type.AVRO) {
            org.apache.avro.Schema avro = new org.apache.avro.Schema.Parser().parse(s.getDefinition());
            try {
                GenericDatumReader<Object> r = new GenericDatumReader<>(avro);
                if (enc == Encoding.JSON) {
                    r.read(null, DecoderFactory.get().jsonDecoder(avro, new ByteArrayInputStream(data.toByteArray())));
                } else {
                    BinaryDecoder d = DecoderFactory.get().binaryDecoder(data.toByteArray(), null);
                    r.read(null, d);
                    if (!d.isEnd()) {
                        throw new IOException("trailing bytes after the Avro datum");
                    }
                }
            } catch (IOException | RuntimeException e) {
                throw PsException.invalid("Message failed schema validation: " + firstLine(e.getMessage()));
            }
            return;
        }
        if (!s.hasCompiledProtoSchema()) {
            throw PsException.unimplemented("Protocol Buffer schema validation needs compiled_proto_schema; "
                    + "parsing .proto source definitions is not supported by this server.");
        }
        Descriptors.Descriptor d = descriptorOf(s);
        try {
            if (enc == Encoding.JSON) {
                JsonFormat.parser().ignoringUnknownFields().merge(new String(data.toByteArray(), StandardCharsets.UTF_8),
                        DynamicMessage.newBuilder(d));
            } else {
                DynamicMessage.parseFrom(d, data);
            }
        } catch (IOException | RuntimeException e) {
            throw PsException.invalid("Message failed schema validation: " + firstLine(e.getMessage()));
        }
    }
}
