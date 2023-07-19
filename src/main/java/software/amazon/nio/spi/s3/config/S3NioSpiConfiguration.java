/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3.config;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.utils.Pair;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Object to hold configuration of the S3 NIO SPI
 */
public class S3NioSpiConfiguration implements Map<String, Object> {

    public enum ConfigProperties {
        /**
         * maximum fragment size
         */
        S3_SPI_READ_MAX_FRAGMENT_SIZE("s3.spi.read.max-fragment-size", 5242880),
        /**
         * maximum fragment number
         */
        S3_SPI_READ_MAX_FRAGMENT_NUMBER("s3.spi.read.max-fragment-number", 50);

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
    private final Properties properties = new Properties();

    /**
     * these properties include the system props and the environment variables,
     * they always have the priority over the `properties` and cannot be deleted
     * because injected from the outside
    */
    private final Properties overwritingProperties = new Properties();

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    /**
     * Create a new, empty configuration
     */
    public S3NioSpiConfiguration(){
        this.properties.put(ConfigProperties.S3_SPI_READ_MAX_FRAGMENT_NUMBER.getPropertyName(),
                ConfigProperties.S3_SPI_READ_MAX_FRAGMENT_NUMBER.getDefaultValue());
        this.properties.put(ConfigProperties.S3_SPI_READ_MAX_FRAGMENT_SIZE.getPropertyName(),
                ConfigProperties.S3_SPI_READ_MAX_FRAGMENT_SIZE.getDefaultValue());

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
        this.properties.put(ConfigProperties.S3_SPI_READ_MAX_FRAGMENT_SIZE.getPropertyName(), maxFragmentSize);
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
        this.properties.put(ConfigProperties.S3_SPI_READ_MAX_FRAGMENT_NUMBER.getPropertyName(), maxFragmentNumber);
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

    @Override
    public int size() {
        return this.keySet().size();
    }

    @Override
    public boolean isEmpty() {
        return this.properties.isEmpty() && this.overwritingProperties.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return this.properties.containsKey(key) || this.overwritingProperties.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return this.properties.containsValue(value) || this.overwritingProperties.containsValue(value);
    }

    @Override
    public Object get(Object key) {
        return this.overwritingProperties.getOrDefault(key, this.properties.get(key));
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
    public void putAll(Map<? extends String, ?> m) {
        this.properties.putAll(m);
    }

    @Override
    public void clear() {
        this.properties.clear();
    }

    @Override
    public Set<String> keySet() {
        var propsKeySet = this.properties.keySet();
        propsKeySet.addAll(this.overwritingProperties.keySet());

        return propsKeySet.stream().map(Object::toString).collect(Collectors.toSet());
    }

    @Override
    public Collection<Object> values() {
        var propsValues = this.properties.values();
        propsValues.addAll(this.overwritingProperties.values());

        return propsValues;
    }

    @Override
    public Set<Entry<String, Object>> entrySet() {
        var mergedProps = new HashMap<String, Object>((Map) this.properties);
        mergedProps.putAll((Map) this.overwritingProperties);

        return mergedProps.entrySet();
    }
}
