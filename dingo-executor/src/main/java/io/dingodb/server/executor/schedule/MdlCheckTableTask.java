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

package io.dingodb.server.executor.schedule;

import io.dingodb.common.environment.ExecutionEnvironment;

import java.util.Map;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class MdlCheckTableTask extends TimerTask {
    long saveMaxSchemaVersion;
    boolean jobNeedToSync = false;
    Map<Long, Long> jobCache;

    public MdlCheckTableTask() {
        jobCache = new ConcurrentHashMap<>();
    }

    @Override
    public void run() {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.getMdlCheckTableInfo().getLock().writeLock().lock();
        long maxVer = env.getMdlCheckTableInfo().getNewestVer();
        if (maxVer > saveMaxSchemaVersion) {
            saveMaxSchemaVersion = maxVer;
        } else if (!jobNeedToSync) {
            env.getMdlCheckTableInfo().getLock().writeLock().unlock();
            return;
        }

        int jobNeedToCheckCnt = env.getMdlCheckTableInfo().getJobsVerMap().size();
        if (jobNeedToCheckCnt == 0) {
            jobNeedToSync = false;
            env.getMdlCheckTableInfo().getLock().writeLock().unlock();
            return;
        }

        Map<Long, Long> jobsVerMap = env.getMdlCheckTableInfo()
            .getJobsVerMap()
            .entrySet()
            .stream()
            .collect(Collectors.toConcurrentMap(Map.Entry::getKey, Map.Entry::getValue));

        Map<Long, String> jobsIdsMap = env.getMdlCheckTableInfo()
            .getJobsIdsMap()
            .entrySet()
            .stream()
            .collect(Collectors.toConcurrentMap(Map.Entry::getKey, Map.Entry::getValue));
        env.getMdlCheckTableInfo().getLock().writeLock().unlock();

        jobNeedToSync = true;

    }
}
