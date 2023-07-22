/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.awscore.AwsClient;
import software.amazon.awssdk.awscore.client.builder.AwsClientBuilder;
import software.amazon.awssdk.http.crt.AwsCrtAsyncHttpClient;
import software.amazon.awssdk.services.s3.*;
import software.amazon.awssdk.services.s3.endpoints.S3EndpointParams;
import software.amazon.awssdk.utils.Pair;
import software.amazon.awssdk.utils.builder.SdkBuilder;
import software.amazon.nio.spi.s3.config.S3NioSpiConfiguration;

import java.net.URI;
import java.time.Duration;
import java.util.*;

/**
 * A Singleton cache of clients for buckets configured for the region of those buckets
 */
public class S3ClientStore {

    private static final S3ClientStore instance = new S3ClientStore();

    // there is a new client for each unique pair of <S3NioSpiConfiguration, bucketName>
    private final Map<Pair<S3NioSpiConfiguration, String>, S3Client> configsAndBucketToClientMap = Collections.synchronizedMap(new HashMap<>());
    private final Map<Pair<S3NioSpiConfiguration, String>, S3AsyncClient> configsAndBucketToAsyncClientMap = Collections.synchronizedMap(new HashMap<>());

    final Logger logger = LoggerFactory.getLogger("S3ClientStore");

    private S3ClientStore(){}

    /**
     * Get the ClientStore instance
     * @return a singleton
     */
    public static S3ClientStore getInstance() { return instance; }

    /**
     * Get an existing client or generate a new client for the named bucket if one doesn't exist
     * @param bucketName the bucket name. If this value is null or empty a default client is returned
     * @return a client
     */
    public S3Client getClientForConfigsAndBucketName(S3NioSpiConfiguration configs, String bucketName) {
        var key = Pair.of(getConfigsAsKey(configs), getBucketNameAsKey(bucketName));
        logger.debug("obtaining client for configs ({}) and bucket ({})", key.left(), key.right());

        return configsAndBucketToClientMap.computeIfAbsent(key, this::generateClient);
    }

    /**
     * Get an existing async client or generate a new client for the named bucket if one doesn't exist
     * @param bucketName the bucket name. If this value is null or empty a default client is returned
     * @return a client
     */
    public S3AsyncClient getAsyncClientForConfigsAndBucketName(S3NioSpiConfiguration configs, String bucketName) {
        var key = Pair.of(getConfigsAsKey(configs), getBucketNameAsKey(bucketName));
        logger.debug("obtaining async client for configs ({}) and bucket ({})", key.left(), key.right());

        return configsAndBucketToAsyncClientMap.computeIfAbsent(key, this::generateAsyncClient);
    }

    /**
     * clean the bucket name to make it the key of the client map
     * @param bucketName the bucket name
     * @return the bucket name as a possible key
     */
    protected String getBucketNameAsKey(String bucketName) {
        return Optional.ofNullable(bucketName).orElse("").trim();
    }

    /**
     * clean the configs to make it the key of the client map
     * @param configs the s3 client configurations
     * @return the configs cleaned
     */
    protected S3NioSpiConfiguration getConfigsAsKey(S3NioSpiConfiguration configs) {
        return Optional.ofNullable(configs).orElse(new S3NioSpiConfiguration());
    }

    /**
     * Generate a client for the named bucket using a default client to determine the location of the named client
     * @param key the configs and the name of the bucket to make the client for
     * @return an S3 client
     */
    protected S3Client generateClient(Pair<S3NioSpiConfiguration, String> key) {
        var configs = key.left();
        var bucketName = key.right();
        var endpointParams = getEndpointParams(configs, bucketName);

        logger.debug("generating client for configs ({}) and bucket ({})", configs, bucketName);
        var clientBuilder = S3Client.builder();
        attachCredentialsIfProvided(clientBuilder, configs);
        return clientBuilder
                .region(configs.getRegion())
                .endpointOverride(URI.create(endpointParams.endpoint()))
                .build();
    }

    /**
     * Generate an asynchronous client for the named bucket using a default client to determine the location of the named client
     * @param key the configs and the name of the bucket to make the client for
     * @return an S3 client
     */
    protected S3AsyncClient generateAsyncClient(Pair<S3NioSpiConfiguration, String> key) {
        var configs = key.left();
        var bucketName = key.right();
        var endpointParams = getEndpointParams(configs, bucketName);

        logger.debug("generating async client for configs ({}) and bucket ({})", configs, bucketName);
        var asyncClientBuilder = S3AsyncClient.builder()
                .httpClientBuilder(AwsCrtAsyncHttpClient
                        .builder()
                        .connectionTimeout(Duration.ofSeconds(3))
                        .maxConcurrency(100));
        attachCredentialsIfProvided(asyncClientBuilder, configs);
        return asyncClientBuilder
                .region(configs.getRegion())
                .endpointOverride(URI.create(endpointParams.endpoint()))
                .build();
    }

    protected <C extends AwsClient, B extends S3BaseClientBuilder<B, C>> S3BaseClientBuilder<B, C> attachCredentialsIfProvided(
            S3BaseClientBuilder<B, C> builder, S3NioSpiConfiguration configs) {
        if (doUseCredentialsFromConfig(configs))
            builder.credentialsProvider(
                    StaticCredentialsProvider.create(
                            AwsBasicCredentials.create(configs.getAccessKey(), configs.getSecretKey())
                    )
            );

        return builder;
    }

    private boolean doUseCredentialsFromConfig(S3NioSpiConfiguration configs) {
        var accessKey = configs.getAccessKey();
        var secretKey = configs.getSecretKey();

        var accessKeyProvided = !Objects.isNull(accessKey) && !accessKey.isBlank();
        var secretKeyProvided = !Objects.isNull(secretKey) && !secretKey.isBlank();

        if (accessKeyProvided ^ secretKeyProvided)
            throw new IllegalArgumentException("Cannot configure only accessKey (%s), but not secretKey (%s), or vice-versa"
                    .formatted(accessKey, secretKey));

        return accessKeyProvided;
    }

    protected S3EndpointParams getEndpointParams(S3NioSpiConfiguration configs, String bucketName) {
        if (Objects.isNull(bucketName) || bucketName.isBlank())
            throw new IllegalArgumentException("The bucket name cannot be null or blank");

        return S3EndpointParams.builder()
                .endpoint(configs.getEndpoint())
                .bucket(bucketName)
                .region(configs.getRegion())
                .build();
    }

    /**
     * clear the client cache
     */
    public void clearClientCache() {
        this.configsAndBucketToClientMap.clear();
    }

    /**
     * clear the async client cache
     */
    public void clearAsyncClientCache() {
        this.configsAndBucketToAsyncClientMap.clear();
    }
}
