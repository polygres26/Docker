package com.sayonora.dms.secrets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class SecretRefTest {

    @Test
    void plainPasswordParsesAsPlaintext() {
        SecretRef ref = SecretRef.parse("hunter2");
        assertEquals(new SecretRef.Plaintext("hunter2"), ref);
        assertTrue(!ref.isExternal());
    }

    @Test
    void nullPasswordParsesAsPlaintextNull() {
        SecretRef ref = SecretRef.parse(null);
        assertEquals(new SecretRef.Plaintext(null), ref);
    }

    @Test
    void awsSecretsManagerParsesSecretIdOnly() {
        SecretRef.AwsSecretsManager ref = (SecretRef.AwsSecretsManager) SecretRef.parse("awssm:prod/oracle-password");
        assertEquals("prod/oracle-password", ref.secretId());
        assertNull(ref.region());
        assertNull(ref.field());
        assertTrue(ref.isExternal());
    }

    @Test
    void awsSecretsManagerParsesRegionAndField() {
        SecretRef.AwsSecretsManager ref = (SecretRef.AwsSecretsManager)
                SecretRef.parse("awssm:prod/oracle-password?region=us-east-1&field=password");
        assertEquals("prod/oracle-password", ref.secretId());
        assertEquals("us-east-1", ref.region());
        assertEquals("password", ref.field());
    }

    @Test
    void azureKeyVaultParsesVaultAndSecretName() {
        SecretRef.AzureKeyVault ref = (SecretRef.AzureKeyVault) SecretRef.parse("azurekv:my-vault/oracle-password");
        assertEquals("my-vault", ref.vaultName());
        assertEquals("oracle-password", ref.secretName());
        assertNull(ref.version());
    }

    @Test
    void azureKeyVaultParsesVersion() {
        SecretRef.AzureKeyVault ref = (SecretRef.AzureKeyVault)
                SecretRef.parse("azurekv:my-vault/oracle-password?version=abc123");
        assertEquals("abc123", ref.version());
    }

    @Test
    void azureKeyVaultRejectsMissingSlash() {
        assertThrows(IllegalArgumentException.class, () -> SecretRef.parse("azurekv:no-slash-here"));
    }

    @Test
    void gcpSecretManagerParsesProjectAndSecretId() {
        SecretRef.GcpSecretManager ref = (SecretRef.GcpSecretManager) SecretRef.parse("gcpsm:my-project/oracle-password");
        assertEquals("my-project", ref.projectId());
        assertEquals("oracle-password", ref.secretId());
        assertNull(ref.version());
    }

    @Test
    void gcpSecretManagerParsesVersion() {
        SecretRef.GcpSecretManager ref = (SecretRef.GcpSecretManager)
                SecretRef.parse("gcpsm:my-project/oracle-password?version=5");
        assertEquals("5", ref.version());
    }

    @Test
    void gcpSecretManagerRejectsMissingSlash() {
        assertThrows(IllegalArgumentException.class, () -> SecretRef.parse("gcpsm:no-slash-here"));
    }
}
