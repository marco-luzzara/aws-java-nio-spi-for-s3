/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3.config;


import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junitpioneer.jupiter.SetEnvironmentVariable;
import org.junitpioneer.jupiter.SetSystemProperty;
import software.amazon.awssdk.regions.Region;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static software.amazon.nio.spi.s3.config.S3NioSpiConfiguration.ConfigProperties;

class S3NioSpiConfigurationTest {
    static Stream<Arguments> configGetterAndDefaultValueSource() {
        return Stream.of(
                arguments((Function<S3NioSpiConfiguration, Object>) S3NioSpiConfiguration::getMaxFragmentSize, 5242880),
                arguments((Function<S3NioSpiConfiguration, Object>) S3NioSpiConfiguration::getMaxFragmentNumber, 50),
                arguments((Function<S3NioSpiConfiguration, Object>) S3NioSpiConfiguration::getEndpoint, "https://s3.us-east-1.amazonaws.com"),
                arguments((Function<S3NioSpiConfiguration, Object>) S3NioSpiConfiguration::getRegion, Region.US_EAST_1),
                arguments((Function<S3NioSpiConfiguration, Object>) S3NioSpiConfiguration::getAccessKey, ""),
                arguments((Function<S3NioSpiConfiguration, Object>) S3NioSpiConfiguration::getSecretKey, "")
        );
    }

    @ParameterizedTest
    @MethodSource("configGetterAndDefaultValueSource")
    void givenConfigWithoutMaxFragmentSize_whenReadIt_thenReturnDefault(
            Function<S3NioSpiConfiguration, Object> propertyGetter,
            Object defaultValue) {
        var configs = new S3NioSpiConfiguration();

        assertThat(propertyGetter.apply(configs)).isEqualTo(defaultValue);
    }

    static Stream<Arguments> configSetterAndGetterAndSetValueSource() {
        return Stream.of(
                arguments((Function<S3NioSpiConfiguration, Object>) S3NioSpiConfiguration::getMaxFragmentSize,
                        (Consumer<S3NioSpiConfiguration>) config -> config.withMaxFragmentSize(1), 1),
                arguments((Function<S3NioSpiConfiguration, Object>) S3NioSpiConfiguration::getMaxFragmentNumber,
                        (Consumer<S3NioSpiConfiguration>) config -> config.withMaxFragmentNumber(1), 1),
                arguments((Function<S3NioSpiConfiguration, Object>) S3NioSpiConfiguration::getEndpoint,
                        (Consumer<S3NioSpiConfiguration>) config -> config.withEndpoint("http://test.endpoint"), "http://test.endpoint"),
                arguments((Function<S3NioSpiConfiguration, Object>) S3NioSpiConfiguration::getRegion,
                        (Consumer<S3NioSpiConfiguration>) config -> config.withRegion(Region.US_EAST_1), Region.US_EAST_1),
                arguments((Function<S3NioSpiConfiguration, Object>) S3NioSpiConfiguration::getAccessKey,
                        (Consumer<S3NioSpiConfiguration>) config -> config.withAccessKey("access_key"), "access_key"),
                arguments((Function<S3NioSpiConfiguration, Object>) S3NioSpiConfiguration::getSecretKey,
                        (Consumer<S3NioSpiConfiguration>) config -> config.withSecretKey("secret_key"), "secret_key")
        );
    }

    @ParameterizedTest
    @MethodSource("configSetterAndGetterAndSetValueSource")
    void givenConfigWithMaxFragmentSize_whenReadIt_thenReturnSetValue(
            Function<S3NioSpiConfiguration, Object> propertyGetter,
            Consumer<S3NioSpiConfiguration> propertySetter,
            Object expectedValue) {
        var configs = new S3NioSpiConfiguration();
        propertySetter.accept(configs);

        assertThat(propertyGetter.apply(configs)).isEqualTo(expectedValue);
    }

    @Test
    @SetEnvironmentVariable(key = "S3_SPI_READ_MAX_FRAGMENT_SIZE", value = "2")
    void givenConfigFromEnvAndCode_whenReadIt_thenReturnEnvValue() {
        var configs = new S3NioSpiConfiguration().withMaxFragmentSize(1);

        assertThat(configs.getMaxFragmentSize()).isEqualTo(2);
    }

    @Test
    @SetSystemProperty(key = "s3.spi.read.max-fragment-size", value = "2")
    void givenConfigFromSystemPropAndCode_whenReadIt_thenReturnSystemPropValue() {
        var configs = new S3NioSpiConfiguration().withMaxFragmentSize(1);

        assertThat(configs.getMaxFragmentSize()).isEqualTo(2);
    }

    @Test
    @SetSystemProperty(key = "s3.spi.read.max-fragment-size", value = "2")
    @SetEnvironmentVariable(key = "S3_SPI_READ_MAX_FRAGMENT_SIZE", value = "3")
    void givenConfigFromSystemPropAndEnvAndCode_whenReadIt_thenReturnSystemPropValue() {
        var configs = new S3NioSpiConfiguration().withMaxFragmentSize(1);

        assertThat(configs.getMaxFragmentSize()).isEqualTo(2);
    }
}