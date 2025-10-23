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

import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class TestAbstractConnector {

    @Mock
    private ConnectorConfigurationContext mockContext;

    @Mock
    private ConnectorPropertyValue mockPropertyValue;

    private TestableAbstractConnector connector;

    @BeforeEach
    void setUp() {
        connector = new TestableAbstractConnector();
    }

    @Test
    void testValidateWithEmptyConfigurationSteps() {
        connector.setConfigurationSteps(Collections.emptyList());

        final List<ValidationResult> results = connector.validate(mockContext);

        assertTrue(results.isEmpty());
        assertTrue(connector.isCustomValidateCalled());
    }

    @Test
    void testValidateWithRequiredPropertyMissing() {
        final ConnectorPropertyDescriptor requiredProperty = new ConnectorPropertyDescriptor.Builder()
            .name("Required Property")
            .description("A required property")
            .required(true)
            .build();

        final ConnectorPropertyGroup propertyGroup = ConnectorPropertyGroup.builder()
            .name("Test Group")
            .addProperty(requiredProperty)
            .build();

        final ConfigurationStep configStep = new ConfigurationStep.Builder()
            .name("Test Step")
            .propertyGroups(List.of(propertyGroup))
            .build();

        connector.setConfigurationSteps(List.of(configStep));
        when(mockContext.getProperty("Test Step", "Required Property")).thenReturn(null);

        final List<ValidationResult> results = connector.validate(mockContext);

        assertEquals(1, results.size());
        final ValidationResult result = results.getFirst();
        assertFalse(result.isValid());
        assertEquals("Required Property", result.getSubject());
        assertNull(result.getInput());
        assertEquals("Required Property is required", result.getExplanation());
        assertFalse(connector.isCustomValidateCalled());
    }

    @Test
    void testValidateWithOptionalPropertyMissing() {
        final ConnectorPropertyDescriptor optionalProperty = new ConnectorPropertyDescriptor.Builder()
            .name("Optional Property")
            .description("An optional property")
            .required(false)
            .build();

        final ConnectorPropertyGroup propertyGroup = ConnectorPropertyGroup.builder()
            .name("Test Group")
            .addProperty(optionalProperty)
            .build();

        final ConfigurationStep configStep = new ConfigurationStep.Builder()
            .name("Test Step")
            .propertyGroups(List.of(propertyGroup))
            .build();

        connector.setConfigurationSteps(List.of(configStep));
        when(mockContext.getProperty("Test Step", "Optional Property")).thenReturn(null);

        final List<ValidationResult> results = connector.validate(mockContext);

        assertTrue(results.isEmpty());
        assertTrue(connector.isCustomValidateCalled());
    }

    @Test
    void testValidateWithInvalidPropertyValue() {
        final ConnectorPropertyDescriptor propertyWithValidator = new ConnectorPropertyDescriptor.Builder()
            .name("Validated Property")
            .description("A property with validation")
            .required(true)
            .addValidator(NON_EMPTY_VALIDATOR)
            .build();

        final ConnectorPropertyGroup propertyGroup = ConnectorPropertyGroup.builder()
            .name("Test Group")
            .addProperty(propertyWithValidator)
            .build();

        final ConfigurationStep configStep = new ConfigurationStep.Builder()
            .name("Test Step")
            .propertyGroups(List.of(propertyGroup))
            .build();

        connector.setConfigurationSteps(List.of(configStep));
        when(mockPropertyValue.getValue()).thenReturn("");
        when(mockContext.getProperty("Test Step", "Validated Property")).thenReturn(mockPropertyValue);

        final List<ValidationResult> results = connector.validate(mockContext);

        assertEquals(1, results.size());
        final ValidationResult result = results.getFirst();
        assertFalse(result.isValid());
        assertEquals("Validated Property", result.getSubject());
        assertFalse(connector.isCustomValidateCalled());
    }

    @Test
    void testValidateWithValidPropertyValue() {
        final ConnectorPropertyDescriptor validProperty = new ConnectorPropertyDescriptor.Builder()
            .name("Valid Property")
            .description("A valid property")
            .required(true)
            .addValidator(NON_EMPTY_VALIDATOR)
            .build();

        final ConnectorPropertyGroup propertyGroup = ConnectorPropertyGroup.builder()
            .name("Test Group")
            .addProperty(validProperty)
            .build();

        final ConfigurationStep configStep = new ConfigurationStep.Builder()
            .name("Test Step")
            .propertyGroups(List.of(propertyGroup))
            .build();

        connector.setConfigurationSteps(List.of(configStep));
        when(mockPropertyValue.getValue()).thenReturn("valid-value");
        when(mockContext.getProperty("Test Step", "Valid Property")).thenReturn(mockPropertyValue);

        final List<ValidationResult> results = connector.validate(mockContext);

        assertTrue(results.isEmpty());
        assertTrue(connector.isCustomValidateCalled());
    }

    @Test
    void testValidateWithPropertyDependencyNotSatisfied() {
        final ConnectorPropertyDescriptor dependencyProperty = new ConnectorPropertyDescriptor.Builder()
            .name("Dependency Property")
            .description("The dependency property")
            .required(false)
            .build();

        final ConnectorPropertyDescriptor dependentProperty = new ConnectorPropertyDescriptor.Builder()
            .name("Dependent Property")
            .description("Property that depends on another")
            .required(true)
            .dependsOn(dependencyProperty, "Required Value")
            .build();

        final ConnectorPropertyGroup propertyGroup = ConnectorPropertyGroup.builder()
            .name("Test Group")
            .addProperty(dependencyProperty)
            .addProperty(dependentProperty)
            .build();

        final ConfigurationStep configStep = new ConfigurationStep.Builder()
            .name("Test Step")
            .propertyGroups(List.of(propertyGroup))
            .build();

        connector.setConfigurationSteps(List.of(configStep));
        when(mockPropertyValue.getValue()).thenReturn("Wrong Value");
        when(mockContext.getProperty("Test Step", "Dependency Property")).thenReturn(mockPropertyValue);

        final List<ValidationResult> results = connector.validate(mockContext);

        assertTrue(results.isEmpty());
        assertTrue(connector.isCustomValidateCalled());
    }

    @Test
    void testValidateWithPropertyDependencySatisfiedButMissingRequiredValue() {
        final ConnectorPropertyDescriptor dependencyProperty = new ConnectorPropertyDescriptor.Builder()
            .name("Dependency Property")
            .description("The dependency property")
            .required(false)
            .build();

        final ConnectorPropertyDescriptor dependentProperty = new ConnectorPropertyDescriptor.Builder()
            .name("Dependent Property")
            .description("Property that depends on another")
            .required(true)
            .dependsOn(dependencyProperty, "Required Value")
            .build();

        final ConnectorPropertyGroup propertyGroup = ConnectorPropertyGroup.builder()
            .name("Test Group")
            .addProperty(dependencyProperty)
            .addProperty(dependentProperty)
            .build();

        final ConfigurationStep configStep = new ConfigurationStep.Builder()
            .name("Test Step")
            .propertyGroups(List.of(propertyGroup))
            .build();

        connector.setConfigurationSteps(List.of(configStep));
        final ConnectorPropertyValue dependencyValue = mock(ConnectorPropertyValue.class);
        when(dependencyValue.getValue()).thenReturn("Required Value");
        when(mockContext.getProperty("Test Step", "Dependency Property")).thenReturn(dependencyValue);
        when(mockContext.getProperty("Test Step", "Dependent Property")).thenReturn(null);

        final List<ValidationResult> results = connector.validate(mockContext);

        assertEquals(1, results.size());
        final ValidationResult result = results.getFirst();
        assertFalse(result.isValid());
        assertEquals("Dependent Property", result.getSubject());
        assertEquals("Dependent Property is required", result.getExplanation());
        assertFalse(connector.isCustomValidateCalled());
    }

    @Test
    void testValidateWithMultipleConfigurationSteps() {
        final ConnectorPropertyDescriptor prop1 = new ConnectorPropertyDescriptor.Builder()
            .name("Property One")
            .required(true)
            .addValidator(NON_EMPTY_VALIDATOR)
            .build();

        final ConnectorPropertyDescriptor prop2 = new ConnectorPropertyDescriptor.Builder()
            .name("Property Two")
            .required(true)
            .addValidator(NON_EMPTY_VALIDATOR)
            .build();

        final ConnectorPropertyGroup group1 = ConnectorPropertyGroup.builder()
            .name("Group One")
            .addProperty(prop1)
            .build();

        final ConnectorPropertyGroup group2 = ConnectorPropertyGroup.builder()
            .name("Group Two")
            .addProperty(prop2)
            .build();

        final ConfigurationStep step1 = new ConfigurationStep.Builder()
            .name("Step One")
            .propertyGroups(List.of(group1))
            .build();

        final ConfigurationStep step2 = new ConfigurationStep.Builder()
            .name("Step Two")
            .propertyGroups(List.of(group2))
            .build();

        connector.setConfigurationSteps(List.of(step1, step2));
        final ConnectorPropertyValue validValue = mock(ConnectorPropertyValue.class);
        when(validValue.getValue()).thenReturn("valid");
        final ConnectorPropertyValue invalidValue = mock(ConnectorPropertyValue.class);
        when(invalidValue.getValue()).thenReturn("");
        when(mockContext.getProperty("Step One", "Property One")).thenReturn(validValue);
        when(mockContext.getProperty("Step Two", "Property Two")).thenReturn(invalidValue);

        final List<ValidationResult> results = connector.validate(mockContext);

        assertEquals(1, results.size());
        final ValidationResult result = results.getFirst();
        assertFalse(result.isValid());
        assertEquals("Property Two", result.getSubject());
        assertFalse(connector.isCustomValidateCalled());
    }

    @Test
    void testValidateWithCustomValidationErrors() {
        connector.setConfigurationSteps(Collections.emptyList());
        connector.setCustomValidationResults(List.of(
            new ValidationResult.Builder()
                .valid(false)
                .subject("Custom Error")
                .explanation("Custom validation failed")
                .build()
        ));

        final List<ValidationResult> results = connector.validate(mockContext);

        assertEquals(1, results.size());
        final ValidationResult result = results.getFirst();
        assertFalse(result.isValid());
        assertEquals("Custom Error", result.getSubject());
        assertEquals("Custom validation failed", result.getExplanation());
        assertTrue(connector.isCustomValidateCalled());
    }

    @Test
    void testValidateWithCustomValidationReturningNull() {
        connector.setConfigurationSteps(Collections.emptyList());
        connector.setCustomValidationResults(null);

        final List<ValidationResult> results = connector.validate(mockContext);

        assertTrue(results.isEmpty());
        assertTrue(connector.isCustomValidateCalled());
    }

    @Test
    void testValidateWithCustomValidationReturningValidResults() {
        connector.setConfigurationSteps(Collections.emptyList());
        connector.setCustomValidationResults(List.of(
            new ValidationResult.Builder()
                .valid(true)
                .subject("Custom Check")
                .explanation("Custom validation passed")
                .build()
        ));

        final List<ValidationResult> results = connector.validate(mockContext);

        assertTrue(results.isEmpty());
        assertTrue(connector.isCustomValidateCalled());
    }

    @Test
    void testValidateWithCircularPropertyDependency() {
        final ConnectorPropertyDescriptor prop1 = new ConnectorPropertyDescriptor.Builder()
            .name("Property One")
            .required(false)
            .build();

        final ConnectorPropertyDescriptor prop2 = new ConnectorPropertyDescriptor.Builder()
            .name("Property Two")
            .required(false)
            .dependsOn(prop1, "Value One")
            .build();

        final ConnectorPropertyDescriptor circularProp1 = new ConnectorPropertyDescriptor.Builder()
            .name("Property One")
            .required(false)
            .dependsOn(prop2, "Value Two")
            .build();

        final ConnectorPropertyGroup propertyGroup = ConnectorPropertyGroup.builder()
            .name("Test Group")
            .addProperty(circularProp1)
            .addProperty(prop2)
            .build();

        final ConfigurationStep configStep = new ConfigurationStep.Builder()
            .name("Test Step")
            .propertyGroups(List.of(propertyGroup))
            .build();

        connector.setConfigurationSteps(List.of(configStep));
        final ConnectorPropertyValue value1 = mock(ConnectorPropertyValue.class);
        when(value1.getValue()).thenReturn("Value One");
        final ConnectorPropertyValue value2 = mock(ConnectorPropertyValue.class);
        when(value2.getValue()).thenReturn("Value Two");
        when(mockContext.getProperty("Test Step", "Property One")).thenReturn(value1);
        when(mockContext.getProperty("Test Step", "Property Two")).thenReturn(value2);

        final List<ValidationResult> results = connector.validate(mockContext);

        assertTrue(results.isEmpty());
        assertTrue(connector.isCustomValidateCalled());
    }

    private static class TestableAbstractConnector extends AbstractConnector {
        private List<ConfigurationStep> configurationSteps = Collections.emptyList();
        private Collection<ValidationResult> customValidationResults = Collections.emptyList();
        private boolean customValidateCalled = false;

        public void setConfigurationSteps(final List<ConfigurationStep> steps) {
            this.configurationSteps = steps;
        }

        public void setCustomValidationResults(final Collection<ValidationResult> results) {
            this.customValidationResults = results;
        }

        public boolean isCustomValidateCalled() {
            return customValidateCalled;
        }

        @Override
        public List<ConfigurationStep> getConfigurationSteps() {
            return configurationSteps;
        }

        @Override
        protected Collection<ValidationResult> customValidate(final ConnectorConfigurationContext context) {
            customValidateCalled = true;
            return customValidationResults;
        }

        @Override
        public void onStepConfigured(final String stepName) {
        }

        @Override
        public void prepareForUpdate() {
        }

        @Override
        public void abortUpdatePreparation(final Throwable cause) {
        }

        @Override
        public void finishUpdate() {
        }

        @Override
        public List<ConfigVerificationResult> verifyConfigurationStep(final String stepName, final Map<String, String> propertyValues) {
            return Collections.emptyList();
        }
    }

    private static final Validator NON_EMPTY_VALIDATOR = new Validator() {
        @Override
        public ValidationResult validate(final String subject, final String input, final ValidationContext context) {
            if (input == null || input.trim().isEmpty()) {
                return new ValidationResult.Builder()
                    .subject(subject)
                    .input(input)
                    .valid(false)
                    .explanation(subject + " cannot be empty")
                    .build();
            }

            return new ValidationResult.Builder()
                .subject(subject)
                .input(input)
                .valid(true)
                .build();
        }
    };

}
