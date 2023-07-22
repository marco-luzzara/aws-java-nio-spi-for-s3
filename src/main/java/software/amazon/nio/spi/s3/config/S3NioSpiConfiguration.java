/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3.config;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.utils.Pair;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Object to hold configuration of the S3 NIO SPI
 */
public class S3NioSpiConfiguration extends AbstractMap<String, Object> {

    public enum ConfigProperties {
        /**
         * maximum fragment size
         */
        S3_SPI_READ_MAX_FRAGMENT_SIZE("s3.spi.read.max-fragment-size", 5242880),
        /**
         * maximum fragment number
         */
        S3_SPI_READ_MAX_FRAGMENT_NUMBER("s3.spi.read.max-fragment-number", 50),
        /**
         * the endpoint the S3 client connects to
         */
        S3_SPI_ENDPOINT("s3.spi.endpoint", "https://s3.us-east-1.amazonaws.com"),
        /**
         * the aws region
         */
        S3_SPI_REGION("s3.spi.region", Region.US_EAST_1),
        /**
         * access and secret key to use the s3 client
         */
        S3_SPI_ACCESS_KEY("s3.spi.access_key", ""),
        S3_SPI_SECRET_KEY("s3.spi.secret_key", "");


        private final String propertyName;
        private final Object propertyDefaultValue;

        ConfigProperties(String propertyName, Object propertyDefaultValue) {
            this.propertyName = propertyName;
            this.propertyDefaultValue = propertyDefaultValue;
        }

        public String getPropertyName() {
            return propertyName;
        }

        public Object getDefaultValue() {
            return propertyDefaultValue;
        }
    }

    /**
     * these are the properties initialized using the class methods
     */
    private final Map<String, Object> properties = new HashMap<>();

    /**
     * these properties include the system props and the environment variables,
     * they always have the priority over the `properties` and cannot be deleted
     * because injected from the outside
    */
    private final Map<String, Object> overwritingProperties = new HashMap<>();

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    /**
     * Create a new, empty configuration
     */
    public S3NioSpiConfiguration(){
        // add env var overrides if present
        this.overwritingProperties.putAll(Arrays.stream(ConfigProperties.values())
                .map(cp -> Pair.of(
                        cp.getPropertyName(),
                        Optional.ofNullable(System.getenv().get(this.convertPropertyNameToEnvVar(cp.getPropertyName())))))
                .filter(p -> p.right().isPresent())
                .map(pair -> Pair.of(pair.left(), pair.right().get()))
                .collect(Collectors.toMap(Pair::left, Pair::right)));

        // System props override env variables
        this.overwritingProperties.putAll(Arrays.stream(ConfigProperties.values())
                .map(cp -> Pair.of(
                        cp.getPropertyName(),
                        Optional.ofNullable(System.getProperty(cp.getPropertyName()))))
                .filter(p -> p.right().isPresent())
                .map(pair -> Pair.of(pair.left(), pair.right().get()))
                .collect(Collectors.toMap(Pair::left, Pair::right)));
    }

    public S3NioSpiConfiguration(Map<String, ?> configs) {
        configs.entrySet().forEach(c -> {
            ConfigProperties configProp = getConfigPropertyFromPropertyName(c.getKey());
            this.properties.put(configProp.getPropertyName(), c.getValue());
        });
    }

    /**
     * Get the value of the Maximum Fragment Size
     * @return the configured value or the default if not overridden
     */
    public int getMaxFragmentSize(){
        return Integer.parseInt(this.get(ConfigProperties.S3_SPI_READ_MAX_FRAGMENT_SIZE.getPropertyName()).toString());
    }

    /**
     * set the value of Maximum Fragment Size
     * @param maxFragmentSize
     * @return this
     */
    public S3NioSpiConfiguration withMaxFragmentSize(int maxFragmentSize){
        this.put(ConfigProperties.S3_SPI_READ_MAX_FRAGMENT_SIZE.getPropertyName(), maxFragmentSize);
        return this;
    }

    /**
     * Get the value of the Maximum Fragment Number
     * @return the configured value or the default if not overridden
     */
    public int getMaxFragmentNumber(){
        return Integer.parseInt(this.get(ConfigProperties.S3_SPI_READ_MAX_FRAGMENT_NUMBER.getPropertyName()).toString());
    }

    /**
     * set the value of Maximum Fragment Number
     * @param maxFragmentNumber
     * @return this
     */
    public S3NioSpiConfiguration withMaxFragmentNumber(int maxFragmentNumber){
        this.put(ConfigProperties.S3_SPI_READ_MAX_FRAGMENT_NUMBER.getPropertyName(), maxFragmentNumber);
        return this;
    }

    /**
     * Get the s3 endpoint
     * @return the configured value or the default if not overridden
     */
    public String getEndpoint(){
        return this.get(ConfigProperties.S3_SPI_ENDPOINT.getPropertyName()).toString();
    }

    /**
     * set the s3 endpoint
     * @param endpoint
     * @return this
     */
    public S3NioSpiConfiguration withEndpoint(String endpoint){
        this.put(ConfigProperties.S3_SPI_ENDPOINT.getPropertyName(), endpoint);
        return this;
    }

    /**
     * Get the s3 region
     * @return the configured value or the default if not overridden
     */
    public Region getRegion(){
        return Region.of(this.get(ConfigProperties.S3_SPI_REGION.getPropertyName()).toString());
    }

    /**
     * set the s3 region
     * @param region
     * @return this
     */
    public S3NioSpiConfiguration withRegion(Region region){
        this.put(ConfigProperties.S3_SPI_REGION.getPropertyName(), region);
        return this;
    }

    /**
     * Get the s3 client access key
     * @return the configured value or the default if not overridden
     */
    public String getAccessKey(){
        return this.get(ConfigProperties.S3_SPI_ACCESS_KEY.getPropertyName()).toString();
    }

    /**
     * set the s3 client access key
     * @param accessKey
     * @return this
     */
    public S3NioSpiConfiguration withAccessKey(String accessKey){
        this.put(ConfigProperties.S3_SPI_ACCESS_KEY.getPropertyName(), accessKey);
        return this;
    }

    /**
     * Get the s3 client secret key
     * @return the configured value or the default if not overridden
     */
    public String getSecretKey(){
        return this.get(ConfigProperties.S3_SPI_SECRET_KEY.getPropertyName()).toString();
    }

    /**
     * set the s3 client secret key
     * @param secretKey
     * @return this
     */
    public S3NioSpiConfiguration withSecretKey(String secretKey){
        this.put(ConfigProperties.S3_SPI_SECRET_KEY.getPropertyName(), secretKey);
        return this;
    }

    /**
     * Generates an environment variable name from a property name. E.g 'some.property' becomes 'SOME_PROPERTY'
     * @param propertyName the name to convert
     * @return the converted name
     */
    protected String convertPropertyNameToEnvVar(String propertyName){
        if(propertyName == null || propertyName.trim().isEmpty()) return "";

        return propertyName
                .trim()
                .replace('.', '_').replace('-', '_')
                .toUpperCase(Locale.ROOT);
    }

    private ConfigProperties getConfigPropertyFromPropertyName(String propertyName) {
        return Arrays.stream(ConfigProperties.values())
                .filter(v -> v.getPropertyName().equals(propertyName))
                .findFirst()
                .orElseThrow(() ->
                        new IllegalArgumentException("The key %s is not a valid property".formatted(propertyName)));
    }

    @Override
    public Object get(Object key) {
        final var strKey = key.toString();
        return this.overwritingProperties.getOrDefault(strKey,
                this.properties.getOrDefault(strKey,
                        // if both the properties and overwriting properties does not have the specified
                        // property configured, then take the default value of the corresponding ConfigProperty
                        // or throw if the key is not valid
                        getConfigPropertyFromPropertyName(strKey).getDefaultValue()));
    }

    @Override
    public Object put(String key, Object value) {
        return this.properties.put(key, value);
    }

    @Override
    public Object remove(Object key) {
        return this.properties.remove(key);
    }

    @Override
    public void clear() {
        this.properties.clear();
    }

    @Override
    public Set<Entry<String, Object>> entrySet() {
        var mergedProps = new HashMap<>(this.properties);
        mergedProps.putAll(this.overwritingProperties);

        return mergedProps.entrySet();
    }
}
