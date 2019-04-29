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

import static com.byhiras.avro.AvroMatchers.avroContains;
import static com.byhiras.avro.AvroMatchers.avroContainsInAnyOrder;
import static com.byhiras.avro.IsAvroObjectEqualTest.johnSmith;

import java.util.List;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.StringDescription;
import org.junit.Test;

import com.google.common.base.Functions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

public class AvroMatcherIterableTest {
    @Test
    public void testContains_Match() {
        Person expected1 = johnSmith().build();
        Person expected2 = johnSmith().setFirstName("Jim").build();
        List<Person> expected = Lists.newArrayList(expected1, expected2);

        Person actual1 = johnSmith().build();
        Person actual2 = johnSmith().setFirstName("Jim").build();
        List<Person> actual = Lists.newArrayList(actual1, actual2);

        final Matcher<?> matcher = avroContains(expected);

        assertThat(matcher.matches(actual), is(true));
    }

    @Test
    public void testContains_MismatchedField() {
        Person expected1 = johnSmith().build();
        Person expected2 = johnSmith().setFirstName("Jim").build();
        List<Person> expected = Lists.newArrayList(expected1, expected2);

        Person actual1 = johnSmith().build();
        Person actual2 = johnSmith().setFirstName("Jason").build();
        List<Person> actual = Lists.newArrayList(actual1, actual2);

        final Matcher<?> matcher = avroContains(expected);

        assertThat(matcher.matches(actual), is(false));

        Description diagnosis = new StringDescription();
        matcher.describeMismatch(actual, diagnosis);
        assertThat(diagnosis.toString(), equalTo("1.firstName Expected: \"Jim\" but: was \"Jason\""));
    }

    @Test
    public void testContains_AdditionalEntry() {
        Person expected1 = johnSmith().build();
        Person expected2 = johnSmith().setFirstName("Jim").build();
        List<Person> expected = Lists.newArrayList(expected1, expected2);

        Person actual1 = johnSmith().build();
        Person actual2 = johnSmith().setFirstName("Jim").build();
        Person actual3 = johnSmith().setFirstName("Joan").build();
        List<Person> actual = Lists.newArrayList(actual1, actual2, actual3);

        final Matcher<?> matcher = avroContains(expected);

        assertThat(matcher.matches(actual), is(false));

        Description diagnosis = new StringDescription();
        matcher.describeMismatch(actual, diagnosis);
        assertThat(diagnosis.toString(), equalTo("had additional indices: 2"));
    }

    @Test
    public void testContains_Iterable() {
        Person expected1 = johnSmith().build();
        Person expected2 = johnSmith().setFirstName("Jim").build();
        List<Person> expected = Lists.newArrayList(expected1, expected2);

        Person actual1 = johnSmith().build();
        Person actual2 = johnSmith().setFirstName("Jim").build();
        Iterable<Person> actual = Iterables.transform(Lists.newArrayList(actual1, actual2), Functions.<Person> identity());

        final Matcher<?> matcher = avroContains(expected);

        assertThat(matcher.matches(actual), is(true));
    }

    @Test
    public void testContains_IterableMismatch() {
        Person expected1 = johnSmith().build();
        Person expected2 = johnSmith().setFirstName("Jim").build();
        List<Person> expected = Lists.newArrayList(expected1, expected2);

        Person actual1 = johnSmith().build();
        Person actual2 = johnSmith().setFirstName("James").build();
        Iterable<Person> actual = Iterables.transform(Lists.newArrayList(actual1, actual2), Functions.<Person> identity());

        final Matcher<?> matcher = avroContains(expected);

        assertThat(matcher.matches(actual), is(false));

        Description diagnosis = new StringDescription();
        matcher.describeMismatch(actual, diagnosis);
        assertThat(diagnosis.toString(), equalTo("1.firstName Expected: \"Jim\" but: was \"James\""));
    }

    @Test
    public void testContainsInAnyOrder_Match() {
        Person expected1 = johnSmith().build();
        Person expected2 = johnSmith().setFirstName("Jim").build();
        List<Person> expected = Lists.newArrayList(expected1, expected2);

        Person actual1 = johnSmith().build();
        Person actual2 = johnSmith().setFirstName("Jim").build();
        List<Person> actual = Lists.newArrayList(actual1, actual2);

        final Matcher<?> matcher = avroContainsInAnyOrder(expected);

        assertThat(matcher.matches(actual), is(true));
    }

    @Test
    public void testContainsInAnyOrder_Mismatch() {
        Person expected1 = johnSmith().build();
        Person expected2 = johnSmith().setFirstName("Jim").build();
        List<Person> expected = Lists.newArrayList(expected1, expected2);

        Person actual1 = johnSmith().build();
        Person actual2 = johnSmith().setFirstName("Jason").build();
        List<Person> actual = Lists.newArrayList(actual1, actual2);

        final Matcher<?> matcher = avroContainsInAnyOrder(expected);

        assertThat(matcher.matches(actual), is(false));

        Description diagnosis = new StringDescription();
        matcher.describeMismatch(actual, diagnosis);
        assertThat(diagnosis.toString(), equalTo(
                "Not matched: <{\"firstName\": \"Jason\", \"lastName\": \"Smith\", \"title\": \"Mr\", \"age\": 21, \"height\": null, \"gender\": \"MALE\", \"email\": \"john.smith@acme.com\", \"telephoneNumbers\": [{\"type\": \"HOME\", \"digits\": \"12345\"}, {\"type\": \"MOBILE\", \"digits\": \"07654\"}, {\"type\": \"WORK\", \"digits\": \"23456\"}], \"address\": {\"firstLine\": \"High and Over\", \"secondLine\": \"Highover Park\", \"thirdLine\": \"Amersham\", \"county\": \"Buckinghamshire\", \"postCode\": \"HP7 0BP\", \"countryId\": null}, \"familyMembers\": {\"Sister\": \"Jane Smith\"}}>"));
    }
}
