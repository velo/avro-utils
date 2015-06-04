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

import java.util.List;
import java.util.Set;

import org.apache.avro.generic.IndexedRecord;

import com.byhiras.avro.AvroMatchers.Excluder;
import com.google.common.collect.ImmutableSet;

class RecordFieldExcluder implements Excluder {
    private final Set<String> exclusions;

    public RecordFieldExcluder(Iterable<String> exclusions) {
        this.exclusions = ImmutableSet.copyOf(exclusions);
    }

    @Override
    public boolean isExcluded(Object record, List<String> fieldPath) {
        if (!(record instanceof IndexedRecord) || fieldPath.isEmpty()) {
            return false;
        }

        return exclusions.contains(fieldPath.get(fieldPath.size() - 1));
    }
}
