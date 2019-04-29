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

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

import static com.byhiras.avro.AvroMatchers.excludeFields;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificRecord;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Description.NullDescription;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.hamcrest.StringDescription;
import org.hamcrest.collection.IsIterableContainingInAnyOrder;

import com.byhiras.avro.AvroMatchers.Options;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.primitives.Primitives;

/**
 * Generic matcher for all Avro objects, which attempts to identify specifically what is wrong.
 */
public class IsAvroObjectEqual {
    static <T extends IndexedRecord> Matcher<T> avroObjectEqualTo(T obj) {
        return new AvroObjectMatcher<T>(obj);
    }

    static <T extends IndexedRecord> Matcher<T> avroObjectEqualTo(T obj, Options options) {
        return new AvroObjectMatcher<T>(obj, options);
    }

    /**
     * @deprecated use {@link AvroMatchers#avroEqualTo(IndexedRecord, Options)} instead
     */
    @Deprecated
    public static <T extends SpecificRecord> Matcher<T> isAvroObjectEqualTo(T value) {
        return avroObjectEqualTo(value);
    }

    /**
     * @deprecated use {@link AvroMatchers#avroEqualTo(IndexedRecord, Options)} instead
     */
    @Deprecated
    public static <T extends SpecificRecord> Matcher<T> isAvroObjectEqualTo(T value, final Set<String> exemptions) {
        Options options = new Options();
        options.setExcluder(excludeFields(exemptions));
        return avroObjectEqualTo(value, options);
    }

    @SuppressWarnings("unchecked")
    static <E extends IndexedRecord> Matcher<Iterable<? extends E>> contains(Collection<E> values, Options options) {
        return contains(values, options, false);
    }

    @SuppressWarnings("unchecked")
    static <E extends IndexedRecord> Matcher<Iterable<? extends E>> containsInAnyOrder(Collection<E> values, Options options) {
        return contains(values, options, true);
    }

    @SuppressWarnings("unchecked")
    private static <E extends IndexedRecord> Matcher<Iterable<? extends E>> contains(Collection<E> values, Options options,
            boolean rootIgnoreOrder) {
        if (values.isEmpty()) {
            return Matchers.<Iterable<? extends E>> equalTo(values);
        }

        final List<Matcher<? super E>> elementMatchers = (List) createElementMatchers(null, values,
                ImmutableList.<String> of(), options, RECORD_MATCHER_FACTORY);
        // ugly raw cast to get ListMatcher to match
        return rootIgnoreOrder ? new CollectionMatcher<E>(elementMatchers, ImmutableList.<String> of()) : (Matcher) new ExternalListMatcher<E>(elementMatchers);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private static Matcher<?> createMatcher(Schema schema, Object value, List<String> fieldPath, Options options) {
        return DEFAULT_MATCHER_FACTORY.createMatcher(schema, value, fieldPath, options);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private static Matcher createRecordMatcher(IndexedRecord value, List<String> fieldPath, Options options) {
        if (value == null) {
            return nullValue();
        }
        return new AvroObjectMatcher(value, fieldPath, options);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private static Matcher<?> createListMatcher(Schema elementSchema, List<?> values, List<String> fieldPath, Options options) {
        if (values.isEmpty()) {
            return equalTo(values);
        }

        final List<Matcher<?>> elementMatchers = createElementMatchers(elementSchema, values, fieldPath, options,
                DEFAULT_MATCHER_FACTORY);

        if (options.isIgnoreArrayOrder()) {
            return new CollectionMatcher(elementMatchers, fieldPath);
        }
        // TODO might not be very pretty output.  Make a more intelligent list matcher that lists missing values, differing indices, etc?
        return new ListMatcher(elementMatchers, fieldPath);
    }

    private static List<Matcher<?>> createElementMatchers(Schema elementSchema, Collection<?> values, List<String> fieldPath, Options options,
            MatcherFactory matcherFactory) {
        final List<Matcher<?>> elementMatchers = Lists.newArrayListWithCapacity(values.size());
        int i = 0;
        for (Object value : values) {
            List<String> elementPath = ImmutableList.copyOf(Iterables.concat(fieldPath, ImmutableList.of(Integer.toString(i))));
            elementMatchers.add(matcherFactory.createMatcher(elementSchema, value, elementPath, options));
            i++;
        }
        return elementMatchers;
    }

    @SuppressWarnings("rawtypes")
    private static Matcher<?> createUnionMatcher(Schema schema, Object value, List<String> fieldPath, Options options) {
        Matcher<?> result = null;

        for (Schema possibleSchema : schema.getTypes()) {
            if (possibleSchema.getType() == Schema.Type.NULL) {
                if (value == null) {
                    result = nullValue();
                    break;
                }
            } else if (value != null) {
                Class<?> possibleClass = SpecificData.get().getClass(possibleSchema);

                // Avro will return the primitive wrapper which the value will not be compatible with
                if (possibleClass.isPrimitive()) {
                    possibleClass = Primitives.wrap(possibleClass);
                }
                if (possibleClass.isInstance(value)) {
                    result = createMatcher(possibleSchema, value, fieldPath, options);
                }
            }
        }

        // Below can happen if you don't use builders to create objects.
        Preconditions.checkNotNull(result, "Could not create Matcher. Was a non-nullable field left null? Schema: " + schema + " Value: " + value);
        return result;
    }

    private static Matcher<?> createDoubleMatcher(Double value) {
        if (value.isNaN() || value.isInfinite()) {
            return equalTo(value);
        }
        if (value == 0.0) {
            return closeTo(value, 1e-6);
        }
        return closeTo(value, Math.abs(value) * 1e-8);
    }

    /**
     * Adaptation of {@link org.hamcrest.TypeSafeDiagnosingMatcher}.
     * @param <T>
     */
    private static abstract class AvroDiagnosingMatcher<T> extends BaseMatcher<T> implements InternalMatcher {
        protected final List<String> objectPath;
        protected final Class<?> expectedType;

        public AvroDiagnosingMatcher(Class<?> expectedType, List<String> objectPath) {
            this.expectedType = expectedType;
            this.objectPath = objectPath;
        }

        @Override
        @SuppressWarnings("unchecked")
        public final boolean matches(Object item) {
            return item != null
                    && expectedType.isInstance(item)
                    && matchesSafely((T) item, new Description.NullDescription());
        }

        @SuppressWarnings("unchecked")
        @Override
        public final void describeMismatch(Object item, Description mismatchDescription) {
            if (item == null || !expectedType.isInstance(item)) {
                if (mismatchDescription instanceof MismatchList) {
                    MismatchList mismatchList = MismatchList.checkArgumentIsMismatchList(mismatchDescription);
                    StringDescription stringDescription = new StringDescription();
                    super.describeMismatch(item, stringDescription);
                    mismatchList.addMismatch(objectPath, stringDescription.toString());
                } else {
                    super.describeMismatch(item, mismatchDescription);
                }
            } else {
                matchesSafely((T) item, mismatchDescription);
            }
        }

        @Override
        public void describeTo(Description description) {
        }

        protected abstract boolean matchesSafely(T item, Description mismatchDescription);

        @Override
        public void describeMismatch2(Object item, Description mismatchDescription) {
            describeMismatch(item, mismatchDescription);
        }
    }

    private static class AvroObjectMatcher<T extends IndexedRecord> extends AvroDiagnosingMatcher<T> {
        protected final T object;
        private final List<Matcher<T>> subMatchers;
        private final Options options;

        /**
         * Simple constructor, will produce a matcher with strict equality checking.
         */
        public AvroObjectMatcher(T object) {
            this(object, new Options());
        }

        /**
         * Constructor.
         *
         * @param object  The object to compare to.
         * @param options options
         */
        public AvroObjectMatcher(T object, Options options) {
            this(object, Lists.<String> newArrayList(), options);
        }

        /**
         * Constructor which allows strictness of the match to be controlled.
         *
         * @param object  The object to compare to.
         * @param options options
         */
        private AvroObjectMatcher(T object, List<String> objectPath, @Nonnull Options options) {
            super(IndexedRecord.class, objectPath);

            this.object = object;
            this.options = options;

            ImmutableList.Builder<Matcher<T>> subMatcherBuilder = ImmutableList.builder();
            for (Field field : object.getSchema().getFields()) {
                List<String> fieldPath = ImmutableList.copyOf(Iterables.concat(objectPath, ImmutableList.of(field.name())));
                if (!options.getExcluder().isExcluded(object, fieldPath)) {
                    Object value = object.get(field.pos());
                    subMatcherBuilder.add(new FieldMatcher<T>(fieldPath, field.pos(), createMatcher(field.schema(), value, fieldPath, options)));
                }
            }
            subMatchers = subMatcherBuilder.build();
        }

        @SuppressWarnings({ "unchecked", "ConstantConditions" })
        @Override
        public void describeTo(Description description) {
            // pretty up the output
            String objectJson;
            try {
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                JsonEncoder jsonEncoder = EncoderFactory.get().jsonEncoder(object.getSchema(), bos, true);
                options.getDatumWriterFactory().apply(object.getSchema()).write(object, jsonEncoder);
                jsonEncoder.flush();
                objectJson = new String(bos.toByteArray(), "UTF-8");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            description.appendText(object.getSchema().getName()).appendText(": ").appendText(objectJson);
        }

        @Override
        protected boolean matchesSafely(T other, final Description mismatchDescription) {
            if (!object.getClass().isInstance(other)) {
                if (mismatchDescription instanceof MismatchList) {
                    ((MismatchList) mismatchDescription).addMismatch(objectPath, "is not instance of " + object.getClass().getName());
                }
                return false;
            }

            boolean matches = true;
            MismatchList mismatchList = null;
            for (Matcher<T> matcher : subMatchers) {
                if (!matcher.matches(other)) {
                    if (mismatchDescription instanceof Description.NullDescription) {
                        // shortcut and return false
                        return false;
                    }

                    // otherwise continue and catalogue mismatches
                    if (mismatchList == null) {
                        if (mismatchDescription instanceof MismatchList) {
                            mismatchList = (MismatchList) mismatchDescription;
                        } else {
                            assert objectPath.isEmpty() : "mismatchDescription should be a MismatchList unless this is the root element";
                            mismatchList = new MismatchList();
                        }
                    }

                    matcher.describeMismatch(other, mismatchList);
                    matches = false;
                }
            }

            if (!matches && objectPath.isEmpty()) { // mismatch and we are the top-level element
                mismatchDescription.appendDescriptionOf(mismatchList);
            }

            return matches;
        }
    }

    private static abstract class AbstractFieldMatcher<T> extends AvroDiagnosingMatcher<T> {
        private final Matcher<?> valueMatcher;

        public AbstractFieldMatcher(Class<?> expectedType, List<String> fieldPath, Matcher<?> valueMatcher) {
            super(expectedType, fieldPath);
            this.valueMatcher = valueMatcher;
        }

        @Override
        protected boolean matchesSafely(T item, Description mismatchDescription) {
            Object value = getValue(item);
            if (valueMatcher.matches(value)) {
                return true;
            }

            if (!(mismatchDescription instanceof Description.NullDescription)) {
                MismatchList mismatchList = MismatchList.checkArgumentIsMismatchList(mismatchDescription);
                if (valueMatcher instanceof InternalMatcher) {
                    ((InternalMatcher) valueMatcher).describeMismatch2(value, mismatchList);
                } else {
                    // we have reached a 'leaf' mismatch, add it to the stack
                    StringDescription mismatchError = new StringDescription();
                    mismatchError.appendText("Expected: ");
                    valueMatcher.describeTo(mismatchError);
                    mismatchError.appendText(" but: ");
                    valueMatcher.describeMismatch(value, mismatchError);
                    mismatchList.addMismatch(objectPath, mismatchError.toString());
                }
            }
            return false;
        }

        protected abstract Object getValue(T item);
    }

    private static class FieldMatcher<T extends IndexedRecord> extends AbstractFieldMatcher<T> {
        private final int fieldPos;

        public FieldMatcher(List<String> fieldPath, int fieldPos, Matcher<?> valueMatcher) {
            super(IndexedRecord.class, fieldPath, valueMatcher);
            this.fieldPos = fieldPos;
        }

        @Override
        protected Object getValue(T item) {
            return item.get(fieldPos);
        }
    }

    private static class MapEntryMatcher extends AbstractFieldMatcher<Map<String, ?>> {
        private final String key;

        public MapEntryMatcher(List<String> fieldPath, String key, Matcher<?> valueMatcher) {
            super(Map.class, fieldPath, valueMatcher);
            this.key = key;
        }

        @Override
        protected Object getValue(Map<String, ?> item) {
            return item.get(key);
        }

        public boolean hasValue(Map<String, ?> item) {
            return item.containsKey(key);
        }
    }

    private static class ListEntryMatcher extends AbstractFieldMatcher<List<?>> {
        private final int index;

        public ListEntryMatcher(List<String> fieldPath, int index, Matcher<?> valueMatcher) {
            super(List.class, fieldPath, valueMatcher);
            this.index = index;
        }

        @Override
        protected Object getValue(List<?> item) {
            return hasValue(item) ? item.get(index) : null;
        }

        private boolean hasValue(List<?> item) {
            return index >= 0 && index < item.size();
        }
    }

    /**
     * Nested class which provides a more informative map matcher.
     *
     * TODO pretty up description
     */
    private static class AvroMapMatcher extends AvroDiagnosingMatcher<Map<String, ?>> {
        private final Map<String, MapEntryMatcher> subMatchers = Maps.newHashMap();

        public AvroMapMatcher(Schema schema, Map<String, ?> map, List<String> objectPath, Options options) {
            super(Map.class, objectPath);

            for (Map.Entry<String, ?> entry : map.entrySet()) {
                List<String> entryPath = ImmutableList.copyOf(Iterables.concat(objectPath, ImmutableList.of(entry.getKey())));
                subMatchers.put(entry.getKey(), new MapEntryMatcher(entryPath, entry.getKey(), createMatcher(schema, entry.getValue(), entryPath, options)));
            }
        }

        @Override
        protected boolean matchesSafely(Map<String, ?> map, Description mismatchDescription) {
            Set<String> remainingKeys = Sets.newHashSet(map.keySet());

            boolean matches = true;
            for (Map.Entry<String, MapEntryMatcher> entry : subMatchers.entrySet()) {
                MapEntryMatcher matcher = entry.getValue();
                if (!matcher.matches(map)) {
                    if (mismatchDescription instanceof Description.NullDescription) {
                        // shortcut  and return false;
                        return false;
                    }

                    // otherwise continue and catalogue mismatches
                    if (matcher.hasValue(map)) {
                        remainingKeys.remove(entry.getKey());
                    }
                    matcher.describeMismatch(map, mismatchDescription);
                    matches = false;
                } else {
                    remainingKeys.remove(entry.getKey());
                }
            }

            if (remainingKeys.size() > 0) {
                if (!(mismatchDescription instanceof Description.NullDescription)) {
                    MismatchList mismatchList = MismatchList.checkArgumentIsMismatchList(mismatchDescription);
                    StringDescription mismatchError = new StringDescription();
                    mismatchError.appendText("had additional keys: ").appendValueList("[", ",", "]", remainingKeys);
                    mismatchList.addMismatch(objectPath, mismatchError.toString());
                }
                matches = false;
            }

            return matches;
        }
    }

    private static class ListMatcher<E> extends AvroDiagnosingMatcher<Iterable<? extends E>> {
        private final List<ListEntryMatcher> matchers;

        public ListMatcher(List<Matcher<? super E>> matchers, List<String> objectPath) {
            super(Iterable.class, objectPath);
            this.matchers = Lists.newArrayListWithCapacity(matchers.size());
            int i = 0;
            for (Matcher<? super E> matcher : matchers) {
                List<String> elementPath = ImmutableList.copyOf(Iterables.concat(objectPath, ImmutableList.of(Integer.toString(i))));
                this.matchers.add(new ListEntryMatcher(elementPath, i, matcher));
                i++;
            }
        }

        @Override
        protected boolean matchesSafely(Iterable<? extends E> item, Description mismatchDescription) {
            // not very nice, but we need it to be a list because that's what ListEntryMatcher expects.  We
            // use ListEntryMatcher because it is an AbstractFieldMatcher and so handles printing the fieldPath
            // etc nicely
            List<?> itemList = item instanceof List ? (List) item : Lists.newArrayList(item);

            for (ListEntryMatcher matcher : matchers) {
                if (!matcher.matches(itemList)) {
                    if (!(mismatchDescription instanceof NullDescription)) {
                        matcher.describeMismatch(itemList, mismatchDescription);
                    }
                    // shortcut
                    return false;
                }
            }
            if (itemList.size() > matchers.size()) {
                if (!(mismatchDescription instanceof Description.NullDescription)) {
                    MismatchList mismatchList = MismatchList.checkArgumentIsMismatchList(mismatchDescription);
                    StringDescription mismatchError = new StringDescription();
                    mismatchError.appendText("had additional indices: ");
                    List<Integer> indexes = Lists.newArrayListWithCapacity(itemList.size() - matchers.size());
                    for (int i = matchers.size(); i < itemList.size(); i++) {
                        indexes.add(i);
                    }
                    mismatchError.appendText(Joiner.on(", ").join(indexes));
                    mismatchList.addMismatch(objectPath, mismatchError.toString());
                }
                return false;
            }
            return true;
        }
    }

    /**
     * Used for {@link #contains(Collection, Options)} matching.
     *
     * @param <E>
     */
    private static class ExternalListMatcher<E> extends ListMatcher<E> {
        public ExternalListMatcher(List<Matcher<? super E>> matchers) {
            super(matchers, ImmutableList.<String> of());
        }

        @Override
        protected boolean matchesSafely(Iterable<? extends E> item, Description mismatchDescription) {
            if (mismatchDescription instanceof NullDescription) {
                return super.matchesSafely(item, mismatchDescription);
            }

            MismatchList mismatchList = new MismatchList();
            boolean matches = super.matchesSafely(item, mismatchList);
            mismatchList.describeTo(mismatchDescription);
            return matches;
        }
    }

    private static class CollectionMatcher<E> extends IsIterableContainingInAnyOrder<E> implements InternalMatcher {
        private final List<String> objectPath;

        public CollectionMatcher(List<Matcher<? super E>> matchers, List<String> objectPath) {
            super(matchers);
            this.objectPath = objectPath;
        }

        @Override
        public void describeTo(Description description) {
            super.describeTo(description);
        }

        @Override
        protected boolean matchesSafely(Iterable<? extends E> items, Description mismatchDescription) {
            if (mismatchDescription instanceof MismatchList) {
                MismatchList list = (MismatchList) mismatchDescription;
                StringDescription desc = new StringDescription();
                boolean matches = super.matchesSafely(items, desc);
                list.addMismatch(objectPath, desc.toString());
                return matches;
            }

            return super.matchesSafely(items, mismatchDescription);
        }

        @SuppressWarnings("unchecked")
        @Override
        public void describeMismatch2(Object item, Description mismatchDescription) {
            if (item == null || !(item instanceof Iterable)) {
                MismatchList mismatchList = MismatchList.checkArgumentIsMismatchList(mismatchDescription);
                StringDescription stringDescription = new StringDescription();
                super.describeMismatch(item, stringDescription);
                mismatchList.addMismatch(objectPath, stringDescription.toString());
            } else {
                matchesSafely((Iterable<? extends E>) item, mismatchDescription);
            }
        }
    }

    /**
     * Marker interface.
     */
    private interface InternalMatcher {
        /**
         * Parallels {@link org.hamcrest.Matcher#describeMismatch(Object, Description)} because CollectionMatcher's
         * super-class make this final.
         *
         * @param item item
         * @param mismatchDescription mismatch description
         */
        void describeMismatch2(Object item, Description mismatchDescription);
    }

    private interface MatcherFactory {
        Matcher<?> createMatcher(Schema schema, Object value, List<String> fieldPath, Options options);
    }

    private static final MatcherFactory DEFAULT_MATCHER_FACTORY = new MatcherFactory() {
        @SuppressWarnings("unchecked")
        @Override
        public Matcher<?> createMatcher(Schema schema, Object value, List<String> fieldPath, Options options) {
            Matcher<?> custom = options.getMatcher(fieldPath);
            if (custom != null) {
                return custom;
            }
            switch (schema.getType()) {
            case RECORD:
                return createRecordMatcher((IndexedRecord) value, fieldPath, options);
            case UNION:
                return createUnionMatcher(schema, value, fieldPath, options);
            case MAP:
                return new AvroMapMatcher(schema.getValueType(), (Map<String, ?>) value, fieldPath, options);
            case ARRAY:
                return createListMatcher(schema.getElementType(), (List<? extends IndexedRecord>) value, fieldPath, options);
            case DOUBLE:
                return createDoubleMatcher((Double) value);
            default:
                return equalTo(value);
            }
        }
    };

    /**
     * Only produces record matchers.  Used by 'public' collection matchers.
     */
    private static final MatcherFactory RECORD_MATCHER_FACTORY = new MatcherFactory() {
        @Override
        public Matcher<?> createMatcher(Schema schema, Object value, List<String> fieldPath, Options options) {
            assert value instanceof IndexedRecord;
            return createRecordMatcher((IndexedRecord) value, fieldPath, options);
        }
    };
}
