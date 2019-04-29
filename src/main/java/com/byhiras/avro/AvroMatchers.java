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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificData;
import org.hamcrest.Matcher;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

public class AvroMatchers {

    public static <T extends IndexedRecord> Matcher<T> avroEqualTo(T obj) {
        return IsAvroObjectEqual.avroObjectEqualTo(obj);
    }

    public static <T extends IndexedRecord> Matcher<T> avroEqualTo(T obj, Options options) {
        return IsAvroObjectEqual.avroObjectEqualTo(obj, options);
    }

    public static <E extends IndexedRecord> Matcher<Iterable<? extends E>> avroContains(E... elements) {
        return avroContains(new Options(), elements);
    }

    public static <E extends IndexedRecord> Matcher<Iterable<? extends E>> avroContains(Options options, E... elements) {
        return IsAvroObjectEqual.contains(ImmutableList.copyOf(elements), options);
    }

    public static <E extends IndexedRecord> Matcher<Iterable<? extends E>> avroContains(Collection<E> elements) {
        return avroContains(elements, new Options());
    }

    public static <E extends IndexedRecord> Matcher<Iterable<? extends E>> avroContains(Collection<E> elements, Options options) {
        return IsAvroObjectEqual.contains(ImmutableList.copyOf(elements), options);
    }

    public static <E extends IndexedRecord> Matcher<Iterable<? extends E>> avroContainsInAnyOrder(E... elements) {
        return avroContainsInAnyOrder(new Options(), elements);
    }

    public static <E extends IndexedRecord> Matcher<Iterable<? extends E>> avroContainsInAnyOrder(Options options, E... elements) {
        return IsAvroObjectEqual.containsInAnyOrder(ImmutableList.copyOf(elements), options);
    }

    public static <E extends IndexedRecord> Matcher<Iterable<? extends E>> avroContainsInAnyOrder(Collection<E> elements) {
        return avroContainsInAnyOrder(elements, new Options());
    }

    public static <E extends IndexedRecord> Matcher<Iterable<? extends E>> avroContainsInAnyOrder(Collection<E> elements, Options options) {
        return IsAvroObjectEqual.containsInAnyOrder(ImmutableList.copyOf(elements), options);
    }

    public static Excluder excludeFields(String... recordFields) {
        if (recordFields == null || recordFields.length == 0) {
            return ALWAYS_FALSE;
        }
        return new RecordFieldExcluder(ImmutableSet.copyOf(recordFields));
    }

    public static Excluder excludeFields(Collection<String> recordFields) {
        if (recordFields == null || recordFields.isEmpty()) {
            return ALWAYS_FALSE;
        }
        return new RecordFieldExcluder(ImmutableSet.copyOf(recordFields));
    }

    private AvroMatchers() {
    }

    private static final Excluder ALWAYS_FALSE = new Excluder() {
        @Override
        public boolean isExcluded(Object record, List<String> fieldPath) {
            return false;
        }
    };
    private static final Function<Schema, DatumWriter> SPECIFIC_DATA_WRITER_FACTORY = new Function<Schema, DatumWriter>() {
        @Override
        public DatumWriter apply(Schema schema) {
            return SpecificData.get().createDatumWriter(schema);
        }
    };

    public interface Excluder {
        /**
         * Should field identified by the given field path be excluded from comparison?
         *
         * @param record the record on which the field exists
         * @param fieldPath the path to the field from the root object
         * @return {@code true} if the field should be excluded, {@code false} otherwise
         */
        boolean isExcluded(Object record, List<String> fieldPath);
    }

    public static class Options {
        private final Map<List<String>, Matcher<?>> customMatchers = Maps.newHashMap();
        private boolean ignoreArrayOrder;
        private Excluder excluder = ALWAYS_FALSE;
        private Function<Schema, DatumWriter> datumWriterFactory = SPECIFIC_DATA_WRITER_FACTORY;

        @Nonnull
        public Excluder getExcluder() {
            return excluder;
        }

        public Options setExcluder(@Nonnull Excluder excluder) {
            checkNotNull(excluder, "excluder is null");
            this.excluder = excluder;
            return this;
        }

        public Options addCustomMatcher(@Nonnull List<String> path, @Nonnull Matcher<?> matcher) {
            checkNotNull(path, "path is null");
            checkArgument(!path.isEmpty(), "path is empty");
            checkNotNull(matcher, "matcher is null");
            customMatchers.put(path, matcher);
            return this;
        }

        public Matcher<?> getMatcher(List<String> path) {
            // TODO allow wildcards for eg array matching, map matching
            return customMatchers.get(path);
        }

        public Options setIgnoreArrayOrder(boolean ignoreArrayOrder) {
            this.ignoreArrayOrder = ignoreArrayOrder;
            return this;
        }

        public boolean isIgnoreArrayOrder() {
            return ignoreArrayOrder;
        }

        @Nonnull
        public Function<Schema, DatumWriter> getDatumWriterFactory() {
            return datumWriterFactory;
        }

        public Options setDatumWriterFactory(@Nonnull Function<Schema, DatumWriter> datumWriterFactory) {
            checkNotNull(datumWriterFactory, "datumWriterFactory is null");
            this.datumWriterFactory = datumWriterFactory;
            return this;
        }
    }
}
