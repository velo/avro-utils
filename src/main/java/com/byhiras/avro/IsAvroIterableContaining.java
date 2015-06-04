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

import java.util.Set;

import org.apache.avro.specific.SpecificRecord;
import org.hamcrest.Factory;
import org.hamcrest.Matcher;

import com.byhiras.avro.AvroMatchers.Options;
import com.google.common.collect.ImmutableList;

/**
 * Improved version of IsIterableContainingInAnyOrder which should make it much clearer to see what is wrong
 * in a long list of items.
 *
 * @author Jacob Metcalf
 * @deprecated Use {@link AvroMatchers} instead.
 */
public class IsAvroIterableContaining {
    @Factory
    public static <E extends SpecificRecord> Matcher<Iterable<? extends E>> contains(Iterable<E> items) {
        return AvroMatchers.avroContainsInAnyOrder(ImmutableList.copyOf(items));
    }

    @Factory
    public static <E extends SpecificRecord> Matcher<Iterable<? extends E>> contains(Iterable<E> items,
            Set<String> exemptions) {
        return AvroMatchers.avroContainsInAnyOrder(ImmutableList.copyOf(items),
                new Options().setExcluder(AvroMatchers.excludeFields(exemptions)));
    }

    @Factory
    public static <E extends SpecificRecord> Matcher<Iterable<? extends E>> contains(Set<String> exemptions,
            E... items) {
        return AvroMatchers.avroContainsInAnyOrder(new Options().setExcluder(AvroMatchers.excludeFields(exemptions)), items);
    }
}
