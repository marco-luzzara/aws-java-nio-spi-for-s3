/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3.config;


import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.SetEnvironmentVariable;
import org.junitpioneer.jupiter.SetSystemProperty;

import static org.assertj.core.api.Assertions.assertThat;
import static software.amazon.nio.spi.s3.config.S3NioSpiConfiguration.ConfigProperties;

class S3NioSpiConfigurationTest {
    @Test
    void givenConfigWithoutMaxFragmentSize_whenReadIt_thenReturnDefault() {
        var configs = new S3NioSpiConfiguration();

        assertThat(configs.getMaxFragmentSize()).isEqualTo(ConfigProperties.S3_SPI_READ_MAX_FRAGMENT_SIZE.getDefaultValue());
    }

    @Test
    void givenConfigWithMaxFragmentSize_whenReadIt_thenReturnSetValue() {
        var configs = new S3NioSpiConfiguration().withMaxFragmentSize(1);

        assertThat(configs.getMaxFragmentSize()).isEqualTo(1);
    }

    @Test
    void givenConfigWithoutMaxFragmentNumber_whenReadIt_thenReturnDefault() {
        var configs = new S3NioSpiConfiguration();

        assertThat(configs.getMaxFragmentNumber()).isEqualTo(ConfigProperties.S3_SPI_READ_MAX_FRAGMENT_NUMBER.getDefaultValue());
    }

    @Test
    void givenConfigWithMaxFragmentNumber_whenReadIt_thenReturnSetValue() {
        var configs = new S3NioSpiConfiguration().withMaxFragmentNumber(1);

        assertThat(configs.getMaxFragmentNumber()).isEqualTo(1);
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