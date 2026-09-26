package com.sayonora.wire.mcp;

import static com.sayonora.wire.mcp.ToolSchemas.*;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.util.Base64;
import java.util.List;

/**
 * AWS Secrets Manager, Systems Manager Parameter Store, KMS and STS vocabulary (secrets_*, ssm_*, kms_*, sts_* tools; the store
 * kind {@code awsparams}). Each tool invokes the same {@code SecretsService}/{@code SsmService}/{@code KmsService}/
 * {@code StsService} operation awswire runs for an AWS SDK client (versions and staging labels, SecureString, envelope
 * encryption under KMS keys, ...).
 *
 * <p>Sensitive data: metadata tools are always available. Tools that return secret material -- {@code secrets_get_secret_value},
 * {@code ssm_get_parameter} / {@code ssm_get_parameters_by_path} with {@code withDecryption}, {@code kms_decrypt} -- refuse on a
 * read-only endpoint ({@code WARP_MCP_READ_ONLY=true}) unless the operator sets {@code WARP_MCP_ALLOW_SECRET_READS=true}. KMS key
 * material is never returned: there is no tool that exports a key, a private key or a plaintext data key, and
 * {@code kms_create_key} answers with key metadata only.
 */
final class AwsParamsToolProvider extends StoreToolProvider {

    AwsParamsToolProvider(StoreDescribeProvider describer, EmulatedStores stores) {
        super(BackendKind.AWSPARAMS, describer, stores);
    }

    @Override
    protected List<Tool> defineTools() {
        JsonObject secretId = str("Secret name or ARN");
        JsonObject page = num("Max results per page");
        JsonObject token = str("NextToken of the previous page");
        return List.of(
                new Tool("secrets_list_secrets", "List secrets (metadata only).", schema(List.of(), "maxResults", page, "nextToken", token), false),
                new Tool("secrets_describe_secret", "Secret metadata: description, KMS key, version stages, rotation, tags (no value).",
                        schema(List.of("secretId"), "secretId", secretId), false),
                new Tool("secrets_get_secret_value", "Read a secret's value (SENSITIVE: refused on read-only endpoints unless WARP_MCP_ALLOW_SECRET_READS=true).",
                        schema(List.of("secretId"), "secretId", secretId, "versionId", str("Version id"), "versionStage", str("Staging label (default AWSCURRENT)")), false),
                new Tool("secrets_create_secret", "Create a secret.", schema(List.of("name", "secretString"), "name", str("Secret name"),
                        "secretString", str("Secret value"), "description", str("Description"), "kmsKeyId", str("KMS key to encrypt under")), true),
                new Tool("secrets_put_secret_value", "Store a new version of a secret.", schema(List.of("secretId", "secretString"),
                        "secretId", secretId, "secretString", str("New secret value")), true),
                new Tool("secrets_delete_secret", "Delete a secret (recovery window 7 days unless forceDelete).",
                        schema(List.of("secretId"), "secretId", secretId, "recoveryWindowInDays", num("7-30 (default 7)"),
                                "forceDelete", bool("Delete immediately, without recovery")), true),
                new Tool("ssm_get_parameter", "Read one parameter (SecureString values need withDecryption, which is SENSITIVE like secret reads).",
                        schema(List.of("name"), "name", str("Parameter name"), "withDecryption", bool("Decrypt SecureString values")), false),
                new Tool("ssm_get_parameters_by_path", "Read the parameters under a path.", schema(List.of("path"), "path", str("Path, e.g. /app/prod"),
                        "recursive", bool("Include nested paths"), "withDecryption", bool("Decrypt SecureString values"), "maxResults", page, "nextToken", token), false),
                new Tool("ssm_describe_parameters", "List parameter metadata (no values).", schema(List.of(), "maxResults", page, "nextToken", token), false),
                new Tool("ssm_put_parameter", "Create or overwrite a parameter.", schema(List.of("name", "value"), "name", str("Parameter name"),
                        "value", str("Value"), "type", str("String (default), StringList or SecureString"), "overwrite", bool("Overwrite an existing parameter"),
                        "description", str("Description")), true),
                new Tool("ssm_delete_parameter", "Delete a parameter.", schema(List.of("name"), "name", str("Parameter name")), true),
                new Tool("kms_list_keys", "List KMS keys.", schema(List.of(), "limit", page, "marker", token), false),
                new Tool("kms_describe_key", "KMS key metadata (state, usage, spec, creation date; never key material).",
                        schema(List.of("keyId"), "keyId", str("Key id, ARN or alias")), false),
                new Tool("kms_list_aliases", "List key aliases.", schema(List.of(), "keyId", str("Only aliases of this key"), "limit", page, "marker", token), false),
                new Tool("kms_create_key", "Create a KMS key; returns metadata only.", schema(List.of(), "description", str("Description"),
                        "keyUsage", str("ENCRYPT_DECRYPT (default), SIGN_VERIFY, ..."), "keySpec", str("SYMMETRIC_DEFAULT (default), RSA_2048, ...")), true),
                new Tool("kms_encrypt", "Encrypt up to 4096 bytes under a KMS key; returns the ciphertext blob (base64).",
                        schema(List.of("keyId"), "keyId", str("Key id, ARN or alias"), "plaintext", str("UTF-8 text"), "plaintextBase64", str("Base64 bytes")), false),
                new Tool("kms_decrypt", "Decrypt a ciphertext blob (SENSITIVE: returns plaintext; refused on read-only endpoints unless "
                        + "WARP_MCP_ALLOW_SECRET_READS=true).", schema(List.of("ciphertextBase64"), "ciphertextBase64", str("Base64 ciphertext blob from kms_encrypt"),
                        "keyId", str("Key id (optional for symmetric keys)")), false),
                new Tool("sts_get_caller_identity", "The account and identity Warp's AWS endpoints report.", schema(List.of()), false));
    }

    private JsonObject call(String service, String op, JsonObject req) throws Exception {
        return AwsToolSupport.invoke(stores, service, op, req);
    }

    /** Copies selected string fields of an AWS answer. */
    private static JsonObject pick(JsonObject from, String... names) {
        JsonObject o = new JsonObject();
        for (String n : names) {
            if (from.has(n)) {
                o.add(n, from.get(n));
            }
        }
        return o;
    }

    @Override
    protected Outcome run(String tool, JsonObject a, Ctx ctx) throws Exception {
        JsonObject req = new JsonObject();
        switch (tool) {
            case "secrets_list_secrets":
                req.addProperty("MaxResults", limit(a, "maxResults", 100, 100));
                SnsToolProvider.put(req, "NextToken", optString(a, "nextToken"));
                return json(call("secretsmanager", "ListSecrets", req));
            case "secrets_describe_secret":
                req.addProperty("SecretId", requireString(a, "secretId"));
                return json(call("secretsmanager", "DescribeSecret", req));
            case "secrets_get_secret_value": {
                if (!AwsToolSupport.secretsReadable()) {
                    return AwsToolSupport.secretsRefused("a secret value");
                }
                req.addProperty("SecretId", requireString(a, "secretId"));
                SnsToolProvider.put(req, "VersionId", optString(a, "versionId"));
                SnsToolProvider.put(req, "VersionStage", optString(a, "versionStage"));
                return json(call("secretsmanager", "GetSecretValue", req));
            }
            case "secrets_create_secret":
                req.addProperty("Name", requireString(a, "name"));
                req.addProperty("SecretString", requireString(a, "secretString"));
                SnsToolProvider.put(req, "Description", optString(a, "description"));
                SnsToolProvider.put(req, "KmsKeyId", optString(a, "kmsKeyId"));
                return json(call("secretsmanager", "CreateSecret", req));
            case "secrets_put_secret_value":
                req.addProperty("SecretId", requireString(a, "secretId"));
                req.addProperty("SecretString", requireString(a, "secretString"));
                return json(call("secretsmanager", "PutSecretValue", req));
            case "secrets_delete_secret":
                req.addProperty("SecretId", requireString(a, "secretId"));
                if (optBool(a, "forceDelete", false)) {
                    req.addProperty("ForceDeleteWithoutRecovery", true);
                } else {
                    req.addProperty("RecoveryWindowInDays", optInt(a, "recoveryWindowInDays") == null ? 7 : optInt(a, "recoveryWindowInDays"));
                }
                return json(call("secretsmanager", "DeleteSecret", req));
            case "ssm_get_parameter": {
                boolean decrypt = optBool(a, "withDecryption", false);
                if (decrypt && !AwsToolSupport.secretsReadable()) {
                    return AwsToolSupport.secretsRefused("a decrypted parameter value");
                }
                req.addProperty("Name", requireString(a, "name"));
                req.addProperty("WithDecryption", decrypt);
                return json(call("ssm", "GetParameter", req));
            }
            case "ssm_get_parameters_by_path": {
                boolean decrypt = optBool(a, "withDecryption", false);
                if (decrypt && !AwsToolSupport.secretsReadable()) {
                    return AwsToolSupport.secretsRefused("decrypted parameter values");
                }
                req.addProperty("Path", requireString(a, "path"));
                req.addProperty("Recursive", optBool(a, "recursive", false));
                req.addProperty("WithDecryption", decrypt);
                req.addProperty("MaxResults", limit(a, "maxResults", 10, 10));
                SnsToolProvider.put(req, "NextToken", optString(a, "nextToken"));
                return json(call("ssm", "GetParametersByPath", req));
            }
            case "ssm_describe_parameters":
                req.addProperty("MaxResults", limit(a, "maxResults", 50, 50));
                SnsToolProvider.put(req, "NextToken", optString(a, "nextToken"));
                return json(call("ssm", "DescribeParameters", req));
            case "ssm_put_parameter":
                req.addProperty("Name", requireString(a, "name"));
                req.addProperty("Value", requireString(a, "value"));
                req.addProperty("Type", optString(a, "type") == null ? "String" : optString(a, "type"));
                req.addProperty("Overwrite", optBool(a, "overwrite", false));
                SnsToolProvider.put(req, "Description", optString(a, "description"));
                return json(call("ssm", "PutParameter", req));
            case "ssm_delete_parameter":
                req.addProperty("Name", requireString(a, "name"));
                return json(call("ssm", "DeleteParameter", req));
            case "kms_list_keys":
                req.addProperty("Limit", limit(a, "limit", 100, 1000));
                SnsToolProvider.put(req, "Marker", optString(a, "marker"));
                return json(call("kms", "ListKeys", req));
            case "kms_describe_key":
                req.addProperty("KeyId", requireString(a, "keyId"));
                return json(call("kms", "DescribeKey", req));
            case "kms_list_aliases":
                req.addProperty("Limit", limit(a, "limit", 100, 100));
                SnsToolProvider.put(req, "KeyId", optString(a, "keyId"));
                SnsToolProvider.put(req, "Marker", optString(a, "marker"));
                return json(call("kms", "ListAliases", req));
            case "kms_create_key": {
                SnsToolProvider.put(req, "Description", optString(a, "description"));
                SnsToolProvider.put(req, "KeyUsage", optString(a, "keyUsage"));
                SnsToolProvider.put(req, "KeySpec", optString(a, "keySpec"));
                JsonObject res = call("kms", "CreateKey", req);
                return json(res.has("KeyMetadata") ? pick(res, "KeyMetadata") : res);
            }
            case "kms_encrypt": {
                req.addProperty("KeyId", requireString(a, "keyId"));
                req.addProperty("Plaintext", Base64.getEncoder().encodeToString(bodyArg(a, "plaintext", "plaintextBase64")));
                JsonObject res = call("kms", "Encrypt", req);
                JsonObject out = pick(res, "KeyId", "EncryptionAlgorithm");
                out.add("ciphertextBase64", res.get("CiphertextBlob"));
                return json(out);
            }
            case "kms_decrypt": {
                if (!AwsToolSupport.secretsReadable()) {
                    return AwsToolSupport.secretsRefused("decrypted plaintext");
                }
                req.addProperty("CiphertextBlob", requireString(a, "ciphertextBase64"));
                SnsToolProvider.put(req, "KeyId", optString(a, "keyId"));
                JsonObject res = call("kms", "Decrypt", req);
                JsonObject out = pick(res, "KeyId", "EncryptionAlgorithm");
                byte[] plain = Base64.getDecoder().decode(res.get("Plaintext").getAsString());
                putBody(out, plain, plain.length);
                return json(out);
            }
            case "sts_get_caller_identity":
                return json(call("sts", "GetCallerIdentity", req));
            default:
                return Outcome.error("unknown awsparams tool: " + tool);
        }
    }
}
