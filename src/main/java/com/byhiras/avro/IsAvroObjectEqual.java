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

import static org.hamcrest.core.AllOf.allOf;
import static org.hamcrest.core.IsInstanceOf.instanceOf;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificRecord;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;
import org.hamcrest.beans.HasPropertyWithValue;
import org.hamcrest.collection.IsIterableContainingInOrder;
import org.hamcrest.core.IsEqual;
import org.hamcrest.core.IsNull;
import org.hamcrest.number.IsCloseTo;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.primitives.Primitives;

/**
 * Generic matcher for all Avro objects, which attempts to identify specifically what is wrong.
 */
public class IsAvroObjectEqual<T extends SpecificRecord> extends TypeSafeDiagnosingMatcher<T> {
    private final T object;
    private final List<Matcher<T>> subMatchers = Lists.newArrayList();

    /**
     * Simple constructor, will produce a matcher with strict equality checking.
     */
    public IsAvroObjectEqual(T object) {
        this(object, new HashSet<String>());
    }

    /**
     * Constructor which allows strictness of the match to be controlled.
     *
     * @param object
     *            The object to compare to.
     * @param strict
     *            If not strict then null fields will not be considered in the
     *            match.
     */
    public IsAvroObjectEqual(T object, Set<String> exemptions) {
        super(SpecificRecord.class);

        this.object = object;
        for (Field field : object.getSchema().getFields()) {

            if ((exemptions == null) || (!exemptions.contains(field.name()))) {
                Object value = object.get(field.pos());
                subMatchers.add(new HasPropertyWithValue<T>(field.name(), createMatcher(field.schema(), value, exemptions)));
            }
        }
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private Matcher<?> createMatcher(Schema schema, Object value, Set<String> exemptions) {
        switch (schema.getType()) {
            case RECORD:
                return createRecordMatcher((SpecificRecord) value, exemptions);
            case UNION:
                return createUnionMatcher(schema, value, exemptions);
            case MAP:
                return new AvroMapMatcher(schema.getValueType(), (Map<String, ?>) value, exemptions);
            case ARRAY:
                return createListMatcher(schema.getElementType(), (List<?>) value, exemptions);
            case DOUBLE:
                return new IsCloseTo((Double) value, 1.0e-6);
            default:
                return new IsEqual(value);
        }
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private Matcher<?> createRecordMatcher(SpecificRecord value, Set<String> exemptions) {
        if (value == null) {
            return new IsNull();
        } else {
            return allOf(instanceOf(value.getClass()), new IsAvroObjectEqual(value, exemptions));
        }
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private Matcher<?> createListMatcher(Schema schema, List<?> values, Set<String> exemptions) {
        final List<Matcher<?>> elementMatchers = Lists.newArrayList();

        for (Object value : values) {
            elementMatchers.add(createMatcher(schema, value, exemptions));
        }

        // TODO: Write new order agnostic which is more adequately able to describe what is wrong
        return new IsIterableContainingInOrder(elementMatchers);
    }

    @SuppressWarnings("rawtypes")
    private Matcher<?> createUnionMatcher(Schema schema, Object value, Set<String> exemptions) {
        Matcher<?> result = null;

        for (Schema possibleSchema : schema.getTypes()) {
            if (possibleSchema.getType() == Schema.Type.NULL) {
                // If the union accepts nulls and the value passed in is null  then we either assert
                // the value to be matched is also null if we are being strict or ignore it if we
                // are not
                if (value == null) {
                    result = new IsNull();
                    break;
                }
            } else if (value != null) {
                Class<?> possibleClass = SpecificData.get().getClass(possibleSchema);

                // Avro will return the primitive wrapper which the value will not be compatible with
                if (possibleClass.isPrimitive()) {
                    possibleClass = Primitives.wrap(possibleClass);
                }
                if (possibleClass.isInstance(value)) {
                    result = createMatcher(possibleSchema, value, exemptions);
                }
            }
        }

        // Below can happen if you don't use builders to create objects.
        Preconditions.checkNotNull(result, "Could not create Matcher. Was a non-nullable field left null? Schema: " + schema + " Value: " + value);
        return result;
    }

    @Override
    public void describeTo(Description description) {
        description.appendText(object.getSchema().getName()).appendText(": ").appendValue(this.object);
    }

    @Override
    protected boolean matchesSafely(T other, Description mismatchDescription) {
        for (Matcher<T> matcher : subMatchers) {
            if (!matcher.matches(other)) {
                if (!(mismatchDescription instanceof Description.NullDescription)) {
                    mismatchDescription.appendDescriptionOf(matcher).appendText(" ");
                    matcher.describeMismatch(other, mismatchDescription);
                }
                return false;
            }
        }

        return true;
    }

    /**
     * Nested class which provides a more informative map matcher.
     */
    private final class AvroMapMatcher extends TypeSafeDiagnosingMatcher<Map<String, ?>> {
        final Map<String, Matcher<?>> subMatchers = Maps.newHashMap();

        public AvroMapMatcher(Schema schema, Map<String, ?> map, Set<String> exemptions) {
            super(Map.class);

            for (Map.Entry<String, ?> entry : map.entrySet()) {
                subMatchers.put(entry.getKey(), createMatcher(schema, entry.getValue(), exemptions));
            }
        }

        @Override
        public void describeTo(Description description) {
            description.appendText("map: ").appendValue(subMatchers);
        }

        @Override
        protected boolean matchesSafely(Map<String, ?> map, Description mismatchDescription) {
            Set<String> remainingKeys = Sets.newHashSet(map.keySet());

            for (Map.Entry<String, Matcher<?>> entry : subMatchers.entrySet()) {
                Object value = map.get(entry.getKey());
                Matcher<?> matcher = entry.getValue();
                if (!matcher.matches(value)) {
                    if (!(mismatchDescription instanceof Description.NullDescription)) {
                        mismatchDescription.appendText("key ")
                                .appendValue(entry.getKey()).appendText(" ");
                        matcher.describeMismatch(value, mismatchDescription);
                    }
                    return false;
                } else {
                    remainingKeys.remove(entry.getKey());
                }
            }

            if (remainingKeys.size() > 0) {
                if (!(mismatchDescription instanceof Description.NullDescription)) {
                    mismatchDescription.appendText("got unexpected map entries: ")
                            .appendValue(remainingKeys);
                }
                return false;
            }

            return true;
        }
    }

    /**
     * Convenience function which creates a matcher with a more readable syntax.
     */
    public static <T extends SpecificRecord> IsAvroObjectEqual<T> isAvroObjectEqualTo(T value) {
        return new IsAvroObjectEqual<T>(value);
    }

    public static <T extends SpecificRecord> IsAvroObjectEqual<T> isAvroObjectEqualTo(T value, Set<String> exemptions) {
        return new IsAvroObjectEqual<T>(value, exemptions);
    }
}
