/*
 *  Copyright (c) 2025 Snowflake Computing Inc. All rights reserved.
 */

package org.apache.nifi.components.connector.examples.kafka;

import org.apache.nifi.components.DescribedValue;

import java.util.EnumSet;

public enum SaslMechanism implements DescribedValue {
    GSSAPI("GSSAPI", "GSSAPI", "General Security Services API for Kerberos authentication"),
    PLAIN("PLAIN", "PLAIN", "Plain username and password authentication"),
    SCRAM_SHA_256("SCRAM-SHA-256", "SCRAM-SHA-256", "Salted Challenge Response Authentication Mechanism using SHA-512 with username and password"),
    SCRAM_SHA_512("SCRAM-SHA-512", "SCRAM-SHA-512", "Salted Challenge Response Authentication Mechanism using SHA-256 with username and password"),
    AWS_MSK_IAM("AWS_MSK_IAM", "AWS_MSK_IAM", "Allows to use AWS IAM for authentication and authorization against Amazon MSK clusters that have AWS IAM enabled " +
                                              "as an authentication mechanism. The IAM credentials will be found using the AWS Default Credentials Provider Chain."),
    OAUTHBEARER("OAUTHBEARER", "OAUTHBEARER", "Token-based authentication using OAuth 2.0 access tokens.");

    private final String value;
    private final String displayName;
    private final String description;

    SaslMechanism(final String value, final String displayName, final String description) {
        this.value = value;
        this.displayName = displayName;
        this.description = description;
    }

    public static EnumSet<SaslMechanism> getAvailableSaslMechanisms() {
        return EnumSet.allOf(SaslMechanism.class);
    }

    @Override
    public String getValue() {
        return value;
    }

    @Override
    public String getDisplayName() {
        return displayName;
    }

    @Override
    public String getDescription() {
        return description;
    }
}
