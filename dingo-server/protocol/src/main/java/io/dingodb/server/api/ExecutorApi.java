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

package io.dingodb.server.api;

import io.dingodb.common.CommonId;
import io.dingodb.common.annotation.ApiDeclaration;
import io.dingodb.common.codec.annotation.TransferArgsCodecAnnotation;
import io.dingodb.common.store.KeyValue;
import io.dingodb.net.Channel;

import java.util.List;
import java.util.concurrent.Future;

public interface ExecutorApi {

    @ApiDeclaration
    default boolean exist(CommonId tableId, byte[] primaryKey) {
        throw new UnsupportedOperationException();
    }

    default boolean upsertKeyValue(CommonId tableId, KeyValue row) {
        return upsertKeyValue(null, null, tableId, row);
    }

    @ApiDeclaration
    @TransferArgsCodecAnnotation(name = "UpsertKeyValueCodeCUsingKeyValue")
    default boolean upsertKeyValue(Channel channel, CommonId schema, CommonId tableId, KeyValue row) {
        throw new UnsupportedOperationException();
    }

    default boolean upsertKeyValue(CommonId tableId, List<KeyValue> rows) {
        return upsertKeyValue(null, null, tableId, rows);
    }

    @ApiDeclaration
    @TransferArgsCodecAnnotation(name = "UpsertKeyValueCodeCUsingListKeyValue")
    default boolean upsertKeyValue(Channel channel, CommonId schema, CommonId tableId, List<KeyValue> rows) {
        throw new UnsupportedOperationException();
    }

    default boolean upsertKeyValue(CommonId tableId, byte[] primaryKey, byte[] row) {
        return upsertKeyValue(null, null, tableId, primaryKey, row);
    }

    @ApiDeclaration
    @TransferArgsCodecAnnotation(name = "UpsertKeyValueCodeCUsingByteArray")
    default boolean upsertKeyValue(Channel channel, CommonId schema, CommonId tableId, byte[] primaryKey, byte[] row) {
        throw new UnsupportedOperationException();
    }

    default byte[] getValueByPrimaryKey(CommonId tableId, byte[] primaryKey) {
        return getValueByPrimaryKey(null, null, tableId, primaryKey);
    }

    @ApiDeclaration
    default byte[] getValueByPrimaryKey(Channel channel, CommonId schema, CommonId tableId, byte[] primaryKey) {
        throw new UnsupportedOperationException();
    }

    default List<KeyValue> getKeyValueByPrimaryKeys(CommonId tableId, List<byte[]> primaryKeys) {
        return getKeyValueByPrimaryKeys(null, null, tableId, primaryKeys);
    }

    @ApiDeclaration
    default List<KeyValue> getKeyValueByPrimaryKeys(Channel channel, CommonId schema, CommonId tableId,
                                            List<byte[]> primaryKeys) {
        throw new UnsupportedOperationException();
    }

    default boolean delete(CommonId tableId, byte[] primaryKey) {
        return delete(null, null, tableId, primaryKey);
    }

    @ApiDeclaration
    default boolean delete(Channel channel, CommonId schema, CommonId tableId, byte[] primaryKey) {
        throw new UnsupportedOperationException();
    }

    default boolean delete(CommonId tableId, List<byte[]> primaryKeys) {
        return delete(null, null, tableId, primaryKeys);
    }

    @ApiDeclaration
    default boolean delete(Channel channel, CommonId schema, CommonId tableId, List<byte[]> primaryKeys) {
        throw new UnsupportedOperationException();
    }

    default boolean deleteRange(CommonId tableId, byte[] startPrimaryKey, byte[] endPrimaryKey) {
        return deleteRange(null, null, tableId, startPrimaryKey, endPrimaryKey);
    }

    @ApiDeclaration
    default boolean deleteRange(Channel channel, CommonId schema, CommonId tableId,
                        byte[] startPrimaryKey, byte[] endPrimaryKey) {
        throw new UnsupportedOperationException();
    }

    default List<KeyValue> getKeyValueByRange(CommonId tableId, byte[] startPrimaryKey, byte[] endPrimaryKey) {
        return getKeyValueByRange(null, null, tableId, startPrimaryKey, endPrimaryKey);
    }

    @ApiDeclaration
    default List<KeyValue> getKeyValueByRange(Channel channel, CommonId schema, CommonId tableId,
                                      byte[] startPrimaryKey, byte[] endPrimaryKey) {
        throw new UnsupportedOperationException();
    }

    @ApiDeclaration
    default Future<Object> operator(CommonId tableId, List<byte[]> startPrimaryKey, List<byte[]> endPrimaryKey,
                                    byte[] op) {
        throw new UnsupportedOperationException();
    }

    @ApiDeclaration
    default KeyValue udfGet(CommonId tableId, byte[] primaryKey, String udfName, String functionName) {
        return udfGet(tableId, primaryKey, udfName, functionName, 0);
    }

    @ApiDeclaration
    default  KeyValue udfGet(CommonId tableId, byte[] primaryKey, String udfName, String functionName, int version) {
        throw new UnsupportedOperationException();
    }

    @ApiDeclaration
    default boolean udfUpdate(CommonId tableId, byte[] primaryKey, String udfName, String functionName) {
        return udfUpdate(tableId, primaryKey, udfName, functionName, 0);
    }

    @ApiDeclaration
    default boolean udfUpdate(CommonId tableId, byte[] primaryKey, String udfName, String functionName, int version) {
        throw new UnsupportedOperationException();
    }
}
