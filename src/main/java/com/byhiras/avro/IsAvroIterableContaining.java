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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.avro.specific.SpecificRecord;
import org.hamcrest.Description;
import org.hamcrest.Factory;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;

/**
 * Improved version of IsIterableContainingInAnyOrder which should make it much clearer to see what is wrong
 * in a long list of items.
 * 
 * @author Jacob Metcalf
 */
public class IsAvroIterableContaining<E extends SpecificRecord> extends TypeSafeDiagnosingMatcher<Iterable<? extends E>> {
    
    private final List<IsAvroObjectEqual<? super E>> matchers;

    public IsAvroIterableContaining(List<IsAvroObjectEqual<? super E>> matchers) {
        this.matchers = matchers;
    }

    @Override
    protected boolean matchesSafely(Iterable<? extends E> iterable, Description mismatchDescription) {
        MatchSeries<E> matchSeries = new MatchSeries<E>(matchers, mismatchDescription);
        for (E item : iterable) {
            if (!matchSeries.matches(item)) {
                return false;
            }
        }

        return matchSeries.isFinished();
    }

    public void describeTo(Description description) {
        description.appendText("iterable of Avro Objects");
    }

    private static class MatchSeries<F extends SpecificRecord> {
        public final List<IsAvroObjectEqual<? super F>> matchers;
        private final Description mismatchDescription;
        public int nextMatchIx = 0;

        public MatchSeries(List<IsAvroObjectEqual<? super F>> matchers, Description mismatchDescription) {
            this.mismatchDescription = mismatchDescription;
            if (matchers.isEmpty()) {
                throw new IllegalArgumentException("Should specify at least one expected element");
            }
            this.matchers = matchers;
        }

        public boolean matches(F item) {
            return isNotSurplus(item) && isMatched(item);
        }

        public boolean isFinished() {
            if (nextMatchIx < matchers.size()) {
                mismatchDescription.appendText("No item matched: ").appendDescriptionOf(matchers.get(nextMatchIx));
                return false;
            }
            return true;
        }

        private boolean isMatched(F item) {
            IsAvroObjectEqual<? super F> matcher = matchers.get(nextMatchIx);
            if (!matcher.matches(item)) {
                describeMismatch(matcher, item);
                return false;
            }
            nextMatchIx++;
            return true;
        }

        private boolean isNotSurplus(F item) {
            if (matchers.size() <= nextMatchIx) {
                mismatchDescription.appendText("Not matched: ").appendValue(item);
                return false;
            }
            return true;
        }

        private void describeMismatch(IsAvroObjectEqual<? super F> matcher, F item) {
            mismatchDescription .appendText("\n            expected item " + nextMatchIx + ": ")
                .appendDescriptionOf( matcher )
                .appendText("\n            actual item " + nextMatchIx + ": ")
                .appendText( item.getSchema().getName())
                .appendText(": ")
                .appendValue( item )
                .appendText("\n            difference: ");
            
            matcher.describeMismatch(item, mismatchDescription);
        }
    }

    @Factory
    public static <E extends SpecificRecord> Matcher<Iterable<? extends E>> contains(Iterable<E> items) {
        List<IsAvroObjectEqual<? super E>> matchers = new ArrayList<IsAvroObjectEqual<? super E>>();
        for (E item : items) {
            matchers.add( IsAvroObjectEqual.isAvroObjectEqualTo( item ));
        }
        return new IsAvroIterableContaining<E>(matchers);
    }
    
    @Factory
    public static <E extends SpecificRecord> Matcher<Iterable<? extends E>> contains(Iterable<E> items,Set<String> exemptions) {
        List<IsAvroObjectEqual<? super E>> matchers = new ArrayList<IsAvroObjectEqual<? super E>>();
        for (E item : items) {
            matchers.add( IsAvroObjectEqual.isAvroObjectEqualTo( item, exemptions ));
        }
        return new IsAvroIterableContaining<E>(matchers);
    }
}
