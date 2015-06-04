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

import java.util.ArrayList;
import java.util.List;

import org.hamcrest.Description;
import org.hamcrest.SelfDescribing;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;

class MismatchList implements Description, SelfDescribing {
    @SuppressWarnings("ConstantConditions")
    public static MismatchList checkArgumentIsMismatchList(Description arg) {
        checkArgument(arg instanceof MismatchList, "Expected %s but was %s", MismatchList.class.getName(), arg.getClass().getName());
        return (MismatchList) arg;
    }

    private final List<Mismatch> mismatches = new ArrayList<Mismatch>();

    public Description addMismatch(Iterable<String> fieldPath, String mismatch) {
        mismatches.add(new Mismatch(fieldPath, mismatch));
        return this;
    }

    @Override
    public void describeTo(Description description) {
        description.appendList("", "\n", "", mismatches);
    }

    @Override
    public Description appendText(String text) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Description appendDescriptionOf(SelfDescribing value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Description appendValue(Object value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Description appendValueList(String start, String separator, String end, T... values) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Description appendValueList(String start, String separator, String end, Iterable<T> values) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Description appendList(String start, String separator, String end,
            Iterable<? extends SelfDescribing> values) {
        throw new UnsupportedOperationException();
    }

    private static final Joiner FIELD_PATH_JOINER = Joiner.on('.');

    static class Mismatch implements SelfDescribing {
        final List<String> fieldPath;
        final String mismatchDescription;

        public Mismatch(Iterable<String> fieldPath, String mismatchDescription) {
            this.fieldPath = ImmutableList.copyOf(fieldPath);
            this.mismatchDescription = mismatchDescription;
        }

        @Override
        public void describeTo(Description description) {
            if (!fieldPath.isEmpty()) {
                description.appendText(FIELD_PATH_JOINER.join(fieldPath));
                description.appendText(" ");
            }
            description.appendText(mismatchDescription);
        }
    }
}
