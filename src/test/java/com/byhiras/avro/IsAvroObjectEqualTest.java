package com.byhiras.avro;

/**
 * Copyright 2015 Byhiras (Europe) Limited
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

import static com.byhiras.avro.IsAvroObjectEqual.avroObjectEqualTo;
import static com.byhiras.avro.IsAvroObjectEqual.isAvroObjectEqualTo;
import static com.google.common.base.Preconditions.checkArgument;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.hamcrest.Matcher;
import org.hamcrest.StringDescription;
import org.jmock.Expectations;
import org.jmock.auto.Mock;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.jmock.lib.concurrent.Synchroniser;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.byhiras.avro.AvroMatchers.Excluder;
import com.byhiras.avro.AvroMatchers.Options;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

@SuppressWarnings("ConstantConditions")
public class IsAvroObjectEqualTest {

    @Rule
    public final JUnitRuleMockery mockery = new JUnitRuleMockery() {
        {
            setImposteriser(ClassImposteriser.INSTANCE);
            setThreadingPolicy(new Synchroniser());
        }
    };

    @Mock
    private Matcher<?> customMatcher;

    private Person expected;
    private Person actual;

    @SuppressWarnings("unchecked")
    @Before
    public void before() throws IOException {
        expected = johnSmith().build();
        actual = johnSmith().build();
    }

    @Test
    public void testMatchedRecord() {
        assertThat(actual, not(sameInstance(expected)));
        assertThat(actual, avroObjectEqualTo(expected));
    }

    @Test
    public void testMismatchedField() {
        actual.setFirstName("James");

        Matcher<?> matcher = avroObjectEqualTo(expected);

        assertMismatchedAndDescriptionEqualTo(matcher, "firstName Expected: \"John\" but: was \"James\"");
    }

    @Test
    public void testMismatchedSubField() {
        actual.getAddress().setCounty("Somerset");

        Matcher<?> matcher = avroObjectEqualTo(expected);

        assertMismatchedAndDescriptionEqualTo(matcher, "address.county Expected: \"Buckinghamshire\" but: was \"Somerset\"");
    }

    @Test
    public void testNullSubRecord() {
        actual.setAddress(null);

        Matcher<?> matcher = avroObjectEqualTo(expected);

        assertMismatchedAndDescriptionEqualTo(matcher, "address was null");
    }

    @Test
    public void testNullSubMap() {
        actual.setFamilyMembers(null);

        Matcher<?> matcher = avroObjectEqualTo(expected);

        assertMismatchedAndDescriptionEqualTo(matcher, "familyMembers was null");
    }

    @Test
    public void testTwoMismatchedSubFields() {
        actual.getAddress().setFirstLine("Low and Under");
        actual.getAddress().setCounty("Somerset");

        assertMismatchedAndDescriptionEqualTo("address.firstLine Expected: \"High and Over\" but: was \"Low and Under\"\naddress.county Expected: \"Buckinghamshire\" but: was \"Somerset\"");
    }

    @Test
    public void testMismatchedMap_MissingKey() {
        actual.getFamilyMembers().remove("Sister");

        assertMismatchedAndDescriptionEqualTo("familyMembers.Sister Expected: \"Jane Smith\" but: was null");
    }

    @Test
    public void testMismatchedMap_AddedKey() {
        actual.getFamilyMembers().put("Brother", "James");

        assertMismatchedAndDescriptionEqualTo("familyMembers had additional keys: [\"Brother\"]");
    }

    @Test
    public void testMismatchedMap_DifferentValue() {
        actual.getFamilyMembers().put("Sister", "Joan Smith");

        assertMismatchedAndDescriptionEqualTo("familyMembers.Sister Expected: \"Jane Smith\" but: was \"Joan Smith\"");
    }

    @Test
    public void testExclusion() {
        actual.getAddress().setCounty("Somerset");

        Matcher<?> matcher = avroObjectEqualTo(expected, new Options()
                .setExcluder(new Excluder() {
                    @Override
                    public boolean isExcluded(Object record, List<String> path) {
                        return ImmutableList.of("address", "county").equals(path);
                    }
                }));
        assertThat(matcher.matches(actual), is(true));
    }

    /**
     * Test that exclusion ignores the entire subtree.
     */
    @Test
    public void testExclusion_Hierarchy() {
        actual.getAddress().setCounty("Somerset");

        Matcher<?> matcher = avroObjectEqualTo(expected, new Options()
                .setExcluder(new Excluder() {
                    @Override
                    public boolean isExcluded(Object record, List<String> path) {
                        return ImmutableList.of("address").equals(path);
                    }
                }));
        assertThat(matcher.matches(actual), is(true));
    }

    @Test
    public void testCustomMatcher() {
        actual.getAddress().setCountryId("FR");

        mockery.checking(new Expectations() {{
            oneOf(customMatcher).matches(actual.getAddress().getCountryId());
            will(returnValue(true));
        }});

        Matcher<?> matcher = avroObjectEqualTo(expected, new Options()
                .addCustomMatcher(ImmutableList.of("address", "countryId"), customMatcher));
        assertThat(matcher.matches(actual), is(true));
    }

    @Test
    public void testVerifyArrayOrder() {
        actual.setTelephoneNumbers(buildPhoneNumbers(
                PhoneNumberType.MOBILE, "07654",
                PhoneNumberType.HOME, "12345",
                PhoneNumberType.WORK, "23456"
        ));

        assertMismatchedAndDescriptionEqualTo("telephoneNumbers.0.type Expected: <HOME> but: was <MOBILE>\ntelephoneNumbers.0.digits Expected: \"12345\" but: was \"07654\"");
    }

    @Test
    public void testVerifyArrayOrder_Missing() {
        actual.setTelephoneNumbers(buildPhoneNumbers(
                PhoneNumberType.HOME, "12345",
                PhoneNumberType.WORK, "23456"
        ));

        assertMismatchedAndDescriptionEqualTo("telephoneNumbers.1.type Expected: <MOBILE> but: was <WORK>\ntelephoneNumbers.1.digits Expected: \"07654\" but: was \"23456\"");
    }

    @Test
    public void testVerifyArrayOrder_Additional() {
        actual.setTelephoneNumbers(buildPhoneNumbers(
                PhoneNumberType.HOME, "12345",
                PhoneNumberType.MOBILE, "07654",
                PhoneNumberType.WORK, "23456",
                PhoneNumberType.FAX, "67890"
        ));

        assertMismatchedAndDescriptionEqualTo("telephoneNumbers had additional indices: 3");
    }

    @Test
    public void testVerifyArrayOrder_NullArray() {
        actual.setTelephoneNumbers(null);

        assertMismatchedAndDescriptionEqualTo("telephoneNumbers was null");
    }

    @Test
    public void testIgnoreArrayOrder() {
        actual.setTelephoneNumbers(buildPhoneNumbers(
                PhoneNumberType.WORK, "23456",
                PhoneNumberType.HOME, "12345",
                PhoneNumberType.MOBILE, "07654"
        ));

        Matcher<?> matcher = avroObjectEqualTo(expected, new Options().setIgnoreArrayOrder(true));
        assertThat(matcher.matches(actual), is(true));
    }

    @Test
    public void testIgnoreArrayOrder_Additional() {
        actual.setTelephoneNumbers(buildPhoneNumbers(
                PhoneNumberType.WORK, "23456",
                PhoneNumberType.HOME, "12345",
                PhoneNumberType.FAX, "67890",
                PhoneNumberType.MOBILE, "07654"
        ));

        Matcher<?> matcher = avroObjectEqualTo(expected, new Options().setIgnoreArrayOrder(true));

        assertMismatchedAndDescriptionEqualTo(matcher, "telephoneNumbers Not matched: <{\"type\": \"FAX\", \"digits\": \"67890\"}>");
    }

    @Test
    public void testIgnoreArrayOrder_Missing() {
        actual.setTelephoneNumbers(buildPhoneNumbers(
                PhoneNumberType.WORK, "23456",
                PhoneNumberType.HOME, "12345"
        ));

        Matcher<?> matcher = avroObjectEqualTo(expected, new Options().setIgnoreArrayOrder(true));

        assertMismatchedAndDescriptionEqualTo(matcher,
                "telephoneNumbers No item matches: PhoneNumber: {\n  \"type\" : \"MOBILE\",\n  \"digits\" : \"07654\"\n} in [<{\"type\": \"WORK\", \"digits\": \"23456\"}>, <{\"type\": \"HOME\", \"digits\": \"12345\"}>]");
    }

    @Test
    public void testIgnoreArrayOrder_NullArray() {
        actual.setTelephoneNumbers(null);

        Matcher<?> matcher = avroObjectEqualTo(expected, new Options().setIgnoreArrayOrder(true));

        assertMismatchedAndDescriptionEqualTo(matcher, "telephoneNumbers was null");
    }

    @Test
    public void checkStringMismatchDetected() {
        actual.setLastName("Hancock");

        assertMismatchedAndDescriptionEqualTo("lastName Expected: \"Smith\" but: was \"Hancock\"");
    }

    @Test
    public void checkLongMismatchDetected() {
        actual.setAge(20L);

        assertMismatchedAndDescriptionEqualTo("age Expected: <21L> but: was <20L>");
    }

    @Test
    public void checkDoubleEpsilonApplied() {
        expected.setHeight(20D);
        actual.setHeight(20.000001D);
        assertMismatchedAndDescriptionEqualTo("height Expected: a numeric value within <2.0E-7> of <20.0> but: <20.000001> differed by <8.000000010279564E-7>");

        Person actual2 = johnSmith().setHeight(20.0000001D).build();
        assertThat(actual2, avroObjectEqualTo(expected));
    }

    @Test
    public void checkDoubleInfinityMatch() {
        expected.setHeight(Double.POSITIVE_INFINITY);
        actual.setHeight(Double.POSITIVE_INFINITY);

        assertThat(expected, avroObjectEqualTo(actual));
    }

    @Test
    public void checkDoubleNaNMatch() {
        expected.setHeight(Double.NaN);
        actual.setHeight(Double.NaN);

        assertThat(actual, avroObjectEqualTo(expected));
    }

    @Test
    public void checkListMemberMismatchDetected() {
        actual.getTelephoneNumbers().get(1).setDigits("01234 56789");

        assertMismatchedAndDescriptionEqualTo("telephoneNumbers.1.digits Expected: \"07654\" but: was \"01234 56789\"");
    }

    @Test
    public void checkEnforceNullMemberIsNull() {
        actual.setAddress(null);

        assertMismatchedAndDescriptionEqualTo("address was null");
    }

    @Test
    public void checkHandlingOfNullChildRecord() throws Exception {
        Person person = new Person();

        assertThat(person, avroObjectEqualTo(person));
    }

    @Test
    public void checkActualEmptyArray() {
        actual.setTelephoneNumbers(Collections.<PhoneNumber>emptyList());

        assertThat(actual, avroObjectEqualTo(actual));

        assertMismatchedAndDescriptionEqualTo("telephoneNumbers.0 was null");
    }

    @Test
    public void checkExpectedEmptyArray() {
        expected.setTelephoneNumbers(Collections.<PhoneNumber>emptyList());

        assertMismatchedAndDescriptionEqualTo("telephoneNumbers Expected: <[]> but: was <[{\"type\": \"HOME\", \"digits\": \"12345\"}, {\"type\": \"MOBILE\", \"digits\": \"07654\"}, {\"type\": \"WORK\", \"digits\": \"23456\"}]>");
    }

    @SuppressWarnings("deprecation")
    @Test
    public void checkExemptions() {
        actual.getAddress().setCountryId("DE");
        actual.setAge(50L);

        assertThat(actual, isAvroObjectEqualTo(expected, ImmutableSet.of("countryId", "age")));
    }

    private void assertMismatchedAndDescriptionEqualTo(Matcher<?> matcher, String description) {
        assertThat(matcher.matches(actual), is(false));

        StringDescription stringDescription = new StringDescription();
        matcher.describeMismatch(actual, stringDescription);
        assertThat(stringDescription.toString(), equalTo(description));
    }

    private void assertMismatchedAndDescriptionEqualTo(String description) {
        assertMismatchedAndDescriptionEqualTo(avroObjectEqualTo(expected), description);
    }

    /**
     * Curried test object
     */
    static Person.Builder johnSmith() {
        Map<String, String> familyMembers = Maps.newHashMap();
        familyMembers.put("Sister", "Jane Smith");

        return Person.newBuilder()
                .setFirstName("John")
                .setLastName("Smith")
                .setTitle("Mr")
                .setGender(Gender.MALE)
                .setAge(21L)
                .setEmail("john.smith@acme.com")
                .setAddress(Location.newBuilder()
                        .setFirstLine("High and Over")
                        .setSecondLine("Highover Park")
                        .setThirdLine("Amersham")
                        .setCounty("Buckinghamshire")
                        .setPostCode("HP7 0BP")
                        .build())
                .setTelephoneNumbers(buildPhoneNumbers(
                        PhoneNumberType.HOME, "12345",
                        PhoneNumberType.MOBILE, "07654",
                        PhoneNumberType.WORK, "23456"

                ))
                .setFamilyMembers(familyMembers);
    }

    static List<PhoneNumber> buildPhoneNumbers(Object... phoneNumberFields) {
        checkArgument(phoneNumberFields.length % 2 == 0, "Must have an even number of phoneNumberFields");
        List<PhoneNumber> phoneNumbers = Lists.newArrayList();
        for (int i = 0; i < phoneNumberFields.length; i += 2) {
            phoneNumbers.add(PhoneNumber.newBuilder()
                    .setType((PhoneNumberType) phoneNumberFields[i])
                    .setDigits((String) phoneNumberFields[i + 1])
                    .build());
        }
        return phoneNumbers;
    }
}
