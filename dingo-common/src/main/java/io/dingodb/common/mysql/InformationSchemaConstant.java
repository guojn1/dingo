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

package io.dingodb.common.mysql;

import io.dingodb.common.ddl.DdlUtil;

public final class InformationSchemaConstant {
    public static final String GLOBAL_VAR_PREFIX_BEGIN = String.format("%s:%s", DdlUtil.tenantPrefix, "global_variables|0|");
    public static final String GLOBAL_VAR_PREFIX_END = String.format("%s:%s", DdlUtil.tenantPrefix, "global_variables|1|");

    private InformationSchemaConstant() {
    }
}
