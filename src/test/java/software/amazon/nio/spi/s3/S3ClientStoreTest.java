/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3AsyncClientBuilder;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.utils.Pair;
import software.amazon.nio.spi.s3.config.S3NioSpiConfiguration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@SuppressWarnings("unchecked")
class S3ClientStoreTest {

    @Mock
    S3Client mockClient;

    @Spy
    final S3ClientStore instance = S3ClientStore.getInstance();

    @BeforeEach
    public void initializeEach() throws Exception {
        lenient().doAnswer(i -> mock(S3Client.class)).when(instance).generateClient(any());
        lenient().doAnswer(i -> mock(S3AsyncClient.class)).when(instance).generateAsyncClient(any());
    }

    @AfterEach
    public void cleanUpEach() {
        instance.clearClientCache();
        instance.clearAsyncClientCache();
    }

    @Test
    void givenClientStore_whenANewUniqueClientIsRequested_thenCreateIt() {
        final var bucketName = "test";
        final var clientConfigs = new S3NioSpiConfiguration();

        var client = instance.getClientForConfigsAndBucketName(clientConfigs, bucketName);

        assertThat(client).isNotNull();
        verify(instance).generateClient(argThat(p -> p.equals(Pair.of(clientConfigs, bucketName))));
    }

    @Test
    void givenClientStore_whenANewUniqueAsyncClientIsRequested_thenCreateIt() {
        final var bucketName = "test";
        final var clientConfigs = new S3NioSpiConfiguration();

        var asyncClient = instance.getAsyncClientForConfigsAndBucketName(clientConfigs, bucketName);

        assertThat(asyncClient).isNotNull();
        verify(instance).generateAsyncClient(argThat(p -> p.equals(Pair.of(clientConfigs, bucketName))));
    }

    @Test
    void givenClientStore_whenAClientWithSameKeyIsRequested_thenReturnTheCachedOne() {
        final var bucketName = "test";
        final var clientConfigs = new S3NioSpiConfiguration();
        var cachedClient = instance.getClientForConfigsAndBucketName(clientConfigs, bucketName);

        var returnedClient = instance.getClientForConfigsAndBucketName(clientConfigs, bucketName);

        verify(instance).generateClient(argThat(p -> p.equals(Pair.of(clientConfigs, bucketName))));
        assertThat(returnedClient).isSameAs(cachedClient);
    }

    @Test
    void givenClientStore_whenAnAsyncClientWithSameKeyIsRequested_thenReturnTheCachedOne() {
        final var bucketName = "test";
        final var clientConfigs = new S3NioSpiConfiguration();
        var cachedClient = instance.getAsyncClientForConfigsAndBucketName(clientConfigs, bucketName);

        var returnedClient = instance.getAsyncClientForConfigsAndBucketName(clientConfigs, bucketName);

        verify(instance).generateAsyncClient(argThat(p -> p.equals(Pair.of(clientConfigs, bucketName))));
        assertThat(returnedClient).isSameAs(cachedClient);
    }

    @Test
    void givenConfigWithOnlyAccessKeySet_whenGenerateClient_thenThrow() {
        final var clientConfigs = new S3NioSpiConfiguration().withAccessKey("test");
        var mockedBuilder = mock(S3ClientBuilder.class);

        assertThrows(IllegalArgumentException.class,
                () -> instance.attachCredentialsIfProvided(mockedBuilder, clientConfigs));
    }

    @Test
    void givenConfigWithOnlySecretKeySet_whenGenerateClient_thenThrow() {
        final var clientConfigs = new S3NioSpiConfiguration().withSecretKey("test");
        var mockedBuilder = mock(S3ClientBuilder.class);

        assertThrows(IllegalArgumentException.class,
                () -> instance.attachCredentialsIfProvided(mockedBuilder, clientConfigs));
    }

    @Test
    void givenConfigWithOnlyAccessKeySet_whenGenerateAsyncClient_thenThrow() {
        final var clientConfigs = new S3NioSpiConfiguration().withAccessKey("test");
        var mockedBuilder = mock(S3AsyncClientBuilder.class);

        assertThrows(IllegalArgumentException.class,
                () -> instance.attachCredentialsIfProvided(mockedBuilder, clientConfigs));
    }

    @Test
    void givenConfigWithOnlySecretKeySet_whenGenerateAsyncClient_thenThrow() {
        final var clientConfigs = new S3NioSpiConfiguration().withSecretKey("test");
        var mockedBuilder = mock(S3AsyncClientBuilder.class);

        assertThrows(IllegalArgumentException.class,
                () -> instance.attachCredentialsIfProvided(mockedBuilder, clientConfigs));
    }

    @Test
    void givenConfigWithCredentialsSet_whenGenerateClient_thenUseThoseCredentials() {
        final var accessKey = "testKey";
        final var secretKey = "testSecret";
        final var clientConfigs = new S3NioSpiConfiguration().withAccessKey(accessKey).withSecretKey(secretKey);
        var builder = mock(S3ClientBuilder.class);

        instance.attachCredentialsIfProvided(builder, clientConfigs);

        verify(builder).credentialsProvider(argThat(a -> {
            var credentials = a.resolveCredentials();
            return credentials.accessKeyId().equals(accessKey) && credentials.secretAccessKey().equals(secretKey);
        }));
    }

    @Test
    void givenConfigWithCredentialsSet_whenGenerateAsyncClient_thenUseThoseCredentials() {
        final var accessKey = "testKey";
        final var secretKey = "testSecret";
        final var clientConfigs = new S3NioSpiConfiguration().withAccessKey(accessKey).withSecretKey(secretKey);
        var builder = mock(S3AsyncClientBuilder.class);

        instance.attachCredentialsIfProvided(builder, clientConfigs);

        verify(builder).credentialsProvider(argThat(a -> {
            var credentials = a.resolveCredentials();
            return credentials.accessKeyId().equals(accessKey) && credentials.secretAccessKey().equals(secretKey);
        }));
    }
}

