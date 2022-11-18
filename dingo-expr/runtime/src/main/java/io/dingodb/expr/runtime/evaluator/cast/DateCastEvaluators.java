/*
 * Copyright 2021 DataCanvas
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.dingodb.expr.runtime.evaluator.cast;

import io.dingodb.expr.annotations.Evaluators;
import io.dingodb.expr.runtime.utils.DateTimeUtils;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.sql.Date;

@Evaluators(
    induceSequence = {
        long.class,
        int.class,
    }
)
final class DateCastEvaluators {
    private DateCastEvaluators() {
    }

    static @NonNull Date dateCast(long value) {
        return new Date(value);
    }

    static @Nullable Date dateCast(String value) {
        return DateTimeUtils.parseDate(value);
    }

    static @NonNull Date dateCast(@NonNull Date value) {
        return value;
    }
}
