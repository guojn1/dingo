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

package io.dingodb.calcite.schema;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.dingodb.calcite.DingoParserContext;
import io.dingodb.calcite.DingoTable;
import io.dingodb.common.CommonId;
import io.dingodb.common.infoschema.InfoCache;
import io.dingodb.common.infoschema.InfoSchema;
import io.dingodb.common.util.Optional;
import io.dingodb.meta.MetaService;
import lombok.Getter;
import org.apache.calcite.schema.SchemaVersion;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class DingoRootSchema extends AbstractSchema {
    public static final String ROOT_SCHEMA_NAME = MetaService.ROOT_NAME;
    public static final String DEFAULT_SCHEMA_NAME = MetaService.DINGO_NAME;

    private static final MetaService ROOT_META_SERVICE = MetaService.root();

    private final Map<String, MetaService> metaServiceCache = new HashMap<>();
    @Getter
    private Map<String, DingoSchema> cache = new ConcurrentHashMap<>();

    private Map<CommonId, InfoSchema> isMap;
    private Map<Long, Long> relateTableMap;

    public DingoRootSchema(DingoParserContext context) {
        super(ROOT_META_SERVICE, context, ImmutableList.of(ROOT_SCHEMA_NAME));
        isMap = new ConcurrentHashMap<>();
        relateTableMap = new ConcurrentHashMap<>();
    }

    @Override
    public Set<String> getTableNames() {
        return ImmutableSet.of();
    }

    @Override
    public DingoTable getTable(String name) {
        return null;
    }

    @Override
    public CommonId getTableId(String tableName) {
        return null;
    }

    public void createSubSchema(String schemaName) {
        metaService.createSubMetaService(schemaName);
    }

    public void dropSubSchema(String schemaName) {
        metaService.dropSubMetaService(schemaName);
    }

    @Override
    public DingoSchema getSubSchema(String name) {
        return Optional.mapOrNull(
            metaService.getSubMetaService(name),
            __ -> new DingoSchema(__, context, ImmutableList.of(ROOT_SCHEMA_NAME, __.name()))
        );
    }

    @Override
    public synchronized Set<String> getSubSchemaNames() {
        return getSubSchemas().keySet();
    }

    public synchronized Map<String, DingoSchema> getSubSchemas() {
        if (metaServiceCache != metaService.getSubMetaServices()) {
            Map<String, DingoSchema> schemas = new HashMap<>();
            metaService.getSubMetaServices().forEach(
                (k, v) -> schemas.put(k, new DingoSchema(v, context, ImmutableList.of(ROOT_SCHEMA_NAME, v.name())))
            );
            cache = schemas;
        }
        return cache;
    }

    @Override
    public DingoSchema snapshot(SchemaVersion version) {
        return new DingoSchema(ROOT_META_SERVICE, context, ImmutableList.of(ROOT_SCHEMA_NAME));
    }

    public void initInfoSchemaByTxn(CommonId txnId) {
        InfoSchema is = InfoCache.infoCache.getLatest();
        isMap.put(txnId, is);
    }

    public void clearInfoSchemaByTxn(CommonId txnId) {
        isMap.remove(txnId);
    }

}
