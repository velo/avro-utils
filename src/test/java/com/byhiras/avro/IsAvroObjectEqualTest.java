package com.byhiras.avro;

/**
 * Copyright 2013 Byhiras (Europe) Limited
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

import static com.byhiras.avro.IsAvroObjectEqual.isAvroObjectEqualTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.core.StringContains.containsString;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.specific.SpecificRecord;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.StringDescription;
import org.junit.Test;

import com.google.common.collect.Lists;

public class IsAvroObjectEqualTest {
   
    /**
     * Demonstrate typical usage.
     */
    @Test
    public void checkEqualObjectsMatch() {
        Person johnSmith1 = johnSmith().build();
        Person johnSmith2 = johnSmith().build();
        
        assertThat( johnSmith1, isAvroObjectEqualTo( johnSmith2 ));
    }

    @Test
    public void checkStringMismatchDetected() {
        Person johnSmith = johnSmith().build();
        Person johnHancock = johnSmith().setLastName("Hancock").build();

        assertMismatchedAndDiagnosisContains(johnSmith, johnHancock, "property \"lastName\" was \"Hancock\"");
    }

    @Test
    public void checkLongMismatchDetected() {
        Person expected = johnSmith().build();
        Person actual   = johnSmith().setAge(20L).build();

        assertMismatchedAndDiagnosisContains(expected, actual, "property \"age\"");
    }

    @Test
    public void checkDoubleEpsilonApplied() {
        Person expected1 = johnSmith().setHeight( 20.000001D ).build();
        Person expected2 = johnSmith().setHeight( 20.0000001D ).build();
        Person actual = johnSmith().setHeight(20D).build();

        assertMismatchedAndDiagnosisContains(expected1, actual, "property \"height\"");
        assertThat( expected2, isAvroObjectEqualTo( actual ));
    }
    
    @Test
    public void checkDoubleInfinityAndNaNMatch() {
        Person expected1 = johnSmith().setHeight( Double.NaN ).build();
        Person actual1   = johnSmith().setHeight( Double.NaN ).build();

        assertThat( expected1, isAvroObjectEqualTo( actual1 ));

        Person expected2 = johnSmith().setHeight( Double.POSITIVE_INFINITY ).build();
        Person actual2   = johnSmith().setHeight( Double.POSITIVE_INFINITY ).build();

        assertThat( expected2, isAvroObjectEqualTo( actual2 ));
    }
    
    @Test
    public void checkListMemberMismatchDetected() {
        Person expected = johnSmith().setTelephoneNumbers(
                buildPhoneNumbers(PhoneNumberType.TELEPHONE, "01234 56789")).build();
        Person actual = johnSmith().setTelephoneNumbers(
                buildPhoneNumbers(PhoneNumberType.TELEPHONE, "01235 56789")).build();

        assertMismatchedAndDiagnosisContains(expected, actual, "property \"digits\" was \"01235 56789\"");
    }
    
    
    @Test
    public void checkListsMismatchDetected() {
        
        Person expected1 = johnSmith().build();
        Person expected2 = johnSmith().setFirstName( "Jim" ).build();
        List<Person> expected = Lists.newArrayList( expected1, expected2 );
        
        Person actual1 = johnSmith().build();
        Person actual2 = johnSmith().setFirstName( "Jason" ).build();
        List<Person> actual = Lists.newArrayList( actual1, actual2 );

        final Matcher<?> matcher = IsAvroIterableContaining.contains(expected);

        assertThat(matcher.matches(actual), equalTo(false));

        Description diagnosis = new StringDescription();
        matcher.describeMismatch(actual, diagnosis);
        
        //assertThat( actual, (Matcher<List<Person>>)matcher );
        assertThat(diagnosis.toString(), containsString( "hasProperty(\"firstName\", \"Jim\") property \"firstName\" was \"Jason\"" ));
    }

    @Test
    public void checkMapMemberMismatchDetected() {
        Person expected = johnSmith().build();
        Map<String, String> expectedFamily = new HashMap<String, String>();
        expectedFamily.put("mother", "Joan Smith");
        expected.setFamilyMembers(expectedFamily);

        Person actual = johnSmith().build();
        Map<String, String> actualFamily = new HashMap<String, String>();
        actualFamily.put("mother", "Jane Smith");
        actual.setFamilyMembers(actualFamily);

        assertMismatchedAndDiagnosisContains(expected, actual, "property \"familyMembers\" key \"mother\" was \"Jane Smith\"");
    }

    @SuppressWarnings("unchecked")
    @Test
    public void checkExtraMapMemberDetected() {
        Person expected = johnSmith().build();
        expected.setFamilyMembers(Collections.EMPTY_MAP);

        Person actual = johnSmith().build();
        Map<String, String> actualFamily = new HashMap<String, String>();
        actualFamily.put("mother", "Jane Smith");
        actual.setFamilyMembers(actualFamily);

        assertMismatchedAndDiagnosisContains(expected, actual, "property \"familyMembers\" got unexpected map entries: <[mother]>");
    }

    @Test
    public void checkEnforceNullMemberIsNullWhenStrict() {
        Person expected = johnSmith().build();
        Person actual = johnSmith().setTelephoneNumbers(buildPhoneNumbers(PhoneNumberType.TELEPHONE, "01234 56789")).build();

        assertMismatchedAndDiagnosisContains(expected, actual, "property \"telephoneNumbers\"");
    }

    @Test
    public void checkHandlingOfNullChildRecord() throws Exception {
        Person person = new Person();
        assertThat(person, isAvroObjectEqualTo(person));
    }

    private static <T extends SpecificRecord> void assertMismatchedAndDiagnosisContains(T expected, T actual, String phrase) {
        final IsAvroObjectEqual<T> matcher = new IsAvroObjectEqual<T>(expected);

        assertThat(matcher.matches(actual), equalTo(false));

        Description diagnosis = new StringDescription();
        matcher.describeMismatch(actual, diagnosis);
        assertThat(diagnosis.toString(), containsString(phrase));
    }

    /**
     * Curried test object
     */
    private static Person.Builder johnSmith() {
        return Person.newBuilder()
                .setFirstName("John")
                .setLastName("Smith")
                .setTitle("Mr")
                .setGender(Gender.MALE)
                .setEmail("john.smith@acme.com");
    }

    private static List<PhoneNumber> buildPhoneNumbers(PhoneNumberType type, String digits) {
        return Lists.newArrayList(PhoneNumber.newBuilder().setType(type).setDigits(digits).build());
    }
}
