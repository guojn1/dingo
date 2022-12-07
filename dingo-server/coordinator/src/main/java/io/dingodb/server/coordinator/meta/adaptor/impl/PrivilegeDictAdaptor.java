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

package io.dingodb.server.coordinator.meta.adaptor.impl;

import com.google.auto.service.AutoService;
import io.dingodb.common.CommonId;
import io.dingodb.server.coordinator.meta.adaptor.MetaAdaptorRegistry;
import io.dingodb.server.coordinator.store.MetaStore;
import io.dingodb.server.protocol.meta.PrivilegeDict;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;

import static io.dingodb.server.protocol.CommonIdConstant.ID_TYPE;
import static io.dingodb.server.protocol.CommonIdConstant.ROOT_DOMAIN;
import static io.dingodb.server.protocol.CommonIdConstant.TABLE_IDENTIFIER;

@Slf4j
public class PrivilegeDictAdaptor extends BaseAdaptor<PrivilegeDict> {

    public static final CommonId META_ID = CommonId.prefix(ID_TYPE.table, TABLE_IDENTIFIER.table);
    public static final byte[] SEQ_KEY = META_ID.encode();

    private static final Map<String, CommonId> PRIVILEGE_DICT = new HashMap<>();

    static {
        PRIVILEGE_DICT.put("Select", new CommonId(
            META_ID.type(), META_ID.identifier(), ROOT_DOMAIN, 1, 1
        ));
        PRIVILEGE_DICT.put("Insert", new CommonId(
            META_ID.type(), META_ID.identifier(), ROOT_DOMAIN, 2, 1
        ));
        PRIVILEGE_DICT.put("Update", new CommonId(
            META_ID.type(), META_ID.identifier(), ROOT_DOMAIN, 3, 1
        ));
        PRIVILEGE_DICT.put("Delete", new CommonId(
            META_ID.type(), META_ID.identifier(), ROOT_DOMAIN, 4, 1
        ));
        PRIVILEGE_DICT.put("Index", new CommonId(
            META_ID.type(), META_ID.identifier(), ROOT_DOMAIN, 5, 1
        ));
        PRIVILEGE_DICT.put("Alter", new CommonId(
            META_ID.type(), META_ID.identifier(), ROOT_DOMAIN, 6, 1
        ));
        PRIVILEGE_DICT.put("Create", new CommonId(
            META_ID.type(), META_ID.identifier(), ROOT_DOMAIN, 7, 1
        ));
        PRIVILEGE_DICT.put("Drop", new CommonId(
            META_ID.type(), META_ID.identifier(), ROOT_DOMAIN, 8, 1
        ));
        PRIVILEGE_DICT.put("Grant", new CommonId(
            META_ID.type(), META_ID.identifier(), ROOT_DOMAIN, 9, 1
        ));
        PRIVILEGE_DICT.put("Create_view", new CommonId(
            META_ID.type(), META_ID.identifier(), ROOT_DOMAIN, 10, 1
        ));
        PRIVILEGE_DICT.put("Show_view", new CommonId(
            META_ID.type(), META_ID.identifier(), ROOT_DOMAIN, 11, 1
        ));
        PRIVILEGE_DICT.put("Create_routine", new CommonId(
            META_ID.type(), META_ID.identifier(), ROOT_DOMAIN, 12, 1
        ));
        PRIVILEGE_DICT.put("Alter_routine", new CommonId(
            META_ID.type(), META_ID.identifier(), ROOT_DOMAIN, 13, 1
        ));
        PRIVILEGE_DICT.put("Execute", new CommonId(
            META_ID.type(), META_ID.identifier(), ROOT_DOMAIN, 14, 1
        ));
        PRIVILEGE_DICT.put("Trigger", new CommonId(
            META_ID.type(), META_ID.identifier(), ROOT_DOMAIN, 15, 1
        ));
        PRIVILEGE_DICT.put("Event", new CommonId(
            META_ID.type(), META_ID.identifier(), ROOT_DOMAIN, 16, 1
        ));
        PRIVILEGE_DICT.put("Create_tmp_table", new CommonId(
            META_ID.type(), META_ID.identifier(), ROOT_DOMAIN, 17, 1
        ));
        PRIVILEGE_DICT.put("Lock_tables", new CommonId(
            META_ID.type(), META_ID.identifier(), ROOT_DOMAIN, 18, 1
        ));
        PRIVILEGE_DICT.put("References", new CommonId(
            META_ID.type(), META_ID.identifier(), ROOT_DOMAIN, 19, 1
        ));
        PRIVILEGE_DICT.put("Reload", new CommonId(
            META_ID.type(), META_ID.identifier(), ROOT_DOMAIN, 20, 1
        ));
        PRIVILEGE_DICT.put("Shutdown", new CommonId(
            META_ID.type(), META_ID.identifier(), ROOT_DOMAIN, 21, 1
        ));
        PRIVILEGE_DICT.put("Process", new CommonId(
            META_ID.type(), META_ID.identifier(), ROOT_DOMAIN, 22, 1
        ));
        PRIVILEGE_DICT.put("File", new CommonId(
            META_ID.type(), META_ID.identifier(), ROOT_DOMAIN, 23, 1
        ));
        PRIVILEGE_DICT.put("Show_db", new CommonId(
            META_ID.type(), META_ID.identifier(), ROOT_DOMAIN, 24, 1
        ));
        PRIVILEGE_DICT.put("Super", new CommonId(
            META_ID.type(), META_ID.identifier(), ROOT_DOMAIN, 25, 1
        ));
        PRIVILEGE_DICT.put("Repl_slave", new CommonId(
            META_ID.type(), META_ID.identifier(), ROOT_DOMAIN, 26, 1
        ));
        PRIVILEGE_DICT.put("Repl_client", new CommonId(
            META_ID.type(), META_ID.identifier(), ROOT_DOMAIN, 27, 1
        ));
        PRIVILEGE_DICT.put("Create_user", new CommonId(
            META_ID.type(), META_ID.identifier(), ROOT_DOMAIN, 28, 1
        ));
        PRIVILEGE_DICT.put("Create_tablespace", new CommonId(
            META_ID.type(), META_ID.identifier(), ROOT_DOMAIN, 29, 1
        ));
        PRIVILEGE_DICT.put("extend1", new CommonId(
            META_ID.type(), META_ID.identifier(), ROOT_DOMAIN, 30, 1
        ));
        PRIVILEGE_DICT.put("extend2", new CommonId(
            META_ID.type(), META_ID.identifier(), ROOT_DOMAIN, 31, 1
        ));
        PRIVILEGE_DICT.put("extend3", new CommonId(
            META_ID.type(), META_ID.identifier(), ROOT_DOMAIN, 32, 1
        ));
        PRIVILEGE_DICT.put("extend4", new CommonId(
            META_ID.type(), META_ID.identifier(), ROOT_DOMAIN, 33, 1
        ));
        PRIVILEGE_DICT.put("extend5", new CommonId(
            META_ID.type(), META_ID.identifier(), ROOT_DOMAIN, 34, 1
        ));
    }

    public PrivilegeDictAdaptor(MetaStore metaStore) {
        super(metaStore);
        MetaAdaptorRegistry.register(PrivilegeDict.class, this);
        if (this.metaMap.isEmpty()) {
            for (Map.Entry<String, CommonId> entry : PRIVILEGE_DICT.entrySet()) {
                PrivilegeDict privilegeDict = PrivilegeDict.builder()
                    .privilege(entry.getKey())
                    .id(entry.getValue())
                    .index(entry.getValue().seq())
                    .build();
                save(privilegeDict);
            }
        }
    }

    @Override
    protected CommonId newId(PrivilegeDict meta) {
        CommonId id = new CommonId(
            META_ID.type(), META_ID.identifier(), ROOT_DOMAIN, metaStore.generateSeq(SEQ_KEY), 1
        );
        return id;
    }

    @Override
    public CommonId metaId() {
        return META_ID;
    }

    @AutoService(BaseAdaptor.Creator.class)
    public static class Creator implements BaseAdaptor.Creator<PrivilegeDict, PrivilegeDictAdaptor> {
        @Override
        public PrivilegeDictAdaptor create(MetaStore metaStore) {
            return new PrivilegeDictAdaptor(metaStore);
        }
    }

    public Map<String, CommonId> getPrivilegeDict() {
        return PRIVILEGE_DICT;
    }

}
