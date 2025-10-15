/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nifi.components.connector;

import org.apache.nifi.components.ValidationResult;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestConnectorPropertyDescriptor {

    @Test
    void testValidateStringType() {
        final ConnectorPropertyDescriptor descriptor = new ConnectorPropertyDescriptor.Builder()
            .name("String Property")
            .type(PropertyType.STRING)
            .build();

        assertTrue(descriptor.validate("any string value").isValid());
        assertTrue(descriptor.validate("!@#$%^&*()_+-=[]{}|;:',.<>?/~`").isValid());
        assertTrue(descriptor.validate("").isValid());
    }

    @Test
    void testValidatePasswordType() {
        final ConnectorPropertyDescriptor descriptor = new ConnectorPropertyDescriptor.Builder()
            .name("Password Property")
            .type(PropertyType.PASSWORD)
            .build();

        assertTrue(descriptor.validate("secretPassword123!").isValid());
        assertTrue(descriptor.validate("").isValid());
    }

    @Test
    void testValidateStringListType() {
        final ConnectorPropertyDescriptor descriptor = new ConnectorPropertyDescriptor.Builder()
            .name("String List Property")
            .type(PropertyType.STRING_LIST)
            .build();

        assertTrue(descriptor.validate("item1,item2,item3").isValid());
        assertTrue(descriptor.validate("").isValid());
    }

    @Test
    void testValidateBooleanTypeWithValidValues() {
        final ConnectorPropertyDescriptor descriptor = new ConnectorPropertyDescriptor.Builder()
            .name("Boolean Property")
            .type(PropertyType.BOOLEAN)
            .build();

        assertTrue(descriptor.validate("true").isValid());
        assertTrue(descriptor.validate("false").isValid());
        assertTrue(descriptor.validate("TRUE").isValid());
        assertTrue(descriptor.validate("FALSE").isValid());
        assertTrue(descriptor.validate("TrUe").isValid());
        assertTrue(descriptor.validate("FaLsE").isValid());
    }

    @Test
    void testValidateBooleanTypeWithInvalidValues() {
        final ConnectorPropertyDescriptor descriptor = new ConnectorPropertyDescriptor.Builder()
            .name("Boolean Property")
            .type(PropertyType.BOOLEAN)
            .build();

        ValidationResult result = descriptor.validate("invalid");
        assertFalse(result.isValid());
        assertEquals("Boolean Property", result.getSubject());
        assertEquals("invalid", result.getInput());
        assertEquals("Value must be true or false", result.getExplanation());

        assertFalse(descriptor.validate("1").isValid());
        assertFalse(descriptor.validate("0").isValid());
        assertFalse(descriptor.validate("yes").isValid());
        assertFalse(descriptor.validate("no").isValid());
        assertFalse(descriptor.validate("").isValid());
    }

    @Test
    void testValidateIntegerTypeWithValidValues() {
        final ConnectorPropertyDescriptor descriptor = new ConnectorPropertyDescriptor.Builder()
            .name("Integer Property")
            .type(PropertyType.INTEGER)
            .build();

        assertTrue(descriptor.validate("12345").isValid());
        assertTrue(descriptor.validate("-12345").isValid());
        assertTrue(descriptor.validate("0").isValid());
        assertTrue(descriptor.validate("00123").isValid());
    }

    @Test
    void testValidateIntegerTypeWithInvalidValues() {
        final ConnectorPropertyDescriptor descriptor = new ConnectorPropertyDescriptor.Builder()
            .name("Integer Property")
            .type(PropertyType.INTEGER)
            .build();

        ValidationResult result = descriptor.validate("123.45");
        assertFalse(result.isValid());
        assertEquals("Integer Property", result.getSubject());
        assertEquals("123.45", result.getInput());
        assertEquals("Value must be an integer", result.getExplanation());

        assertFalse(descriptor.validate("not a number").isValid());
        assertFalse(descriptor.validate(" 123 ").isValid());
        assertFalse(descriptor.validate("+123").isValid());
        assertFalse(descriptor.validate("0x1A3F").isValid());
        assertFalse(descriptor.validate("").isValid());
    }

    @Test
    void testValidateDoubleTypeWithValidValues() {
        final ConnectorPropertyDescriptor descriptor = new ConnectorPropertyDescriptor.Builder()
            .name("Double Property")
            .type(PropertyType.DOUBLE)
            .build();

        assertTrue(descriptor.validate("123").isValid());
        assertTrue(descriptor.validate("123.456").isValid());
        assertTrue(descriptor.validate("-123.456").isValid());
        assertTrue(descriptor.validate("0.0").isValid());
        assertTrue(descriptor.validate("0").isValid());
    }

    @Test
    void testValidateDoubleTypeWithInvalidValues() {
        final ConnectorPropertyDescriptor descriptor = new ConnectorPropertyDescriptor.Builder()
            .name("Double Property")
            .type(PropertyType.DOUBLE)
            .build();

        ValidationResult result = descriptor.validate("not a number");
        assertFalse(result.isValid());
        assertEquals("Double Property", result.getSubject());
        assertEquals("not a number", result.getInput());
        assertEquals("Value must be a floating point number", result.getExplanation());

        assertFalse(descriptor.validate("123.456.789").isValid());
        assertFalse(descriptor.validate("1.23e10").isValid());
        assertFalse(descriptor.validate("123.").isValid());
        assertFalse(descriptor.validate(".123").isValid());
    }

    @Test
    void testValidateFloatTypeWithValidValues() {
        final ConnectorPropertyDescriptor descriptor = new ConnectorPropertyDescriptor.Builder()
            .name("Float Property")
            .type(PropertyType.FLOAT)
            .build();

        assertTrue(descriptor.validate("123").isValid());
        assertTrue(descriptor.validate("123.456").isValid());
        assertTrue(descriptor.validate("-123.456").isValid());
        assertTrue(descriptor.validate("0").isValid());
    }

    @Test
    void testValidateFloatTypeWithInvalidValues() {
        final ConnectorPropertyDescriptor descriptor = new ConnectorPropertyDescriptor.Builder()
            .name("Float Property")
            .type(PropertyType.FLOAT)
            .build();

        ValidationResult result = descriptor.validate("not a number");
        assertFalse(result.isValid());
        assertEquals("Float Property", result.getSubject());
        assertEquals("not a number", result.getInput());
        assertEquals("Value must be a floating point number", result.getExplanation());

        assertFalse(descriptor.validate("123.").isValid());
        assertFalse(descriptor.validate(".123").isValid());
    }
}

