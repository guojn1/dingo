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

package io.dingodb.dingokv.util.concurrent.disruptor;

import com.lmax.disruptor.ExceptionHandler;
import io.dingodb.dingokv.util.StackTraceUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public class LoggingExceptionHandler implements ExceptionHandler<Object> {
    private static final Logger LOG = LoggerFactory.getLogger(LoggingExceptionHandler.class);

    @Override
    public void handleEventException(final Throwable ex, final long sequence, final Object event) {
        if (LOG.isWarnEnabled()) {
            LOG.warn("Exception processing: {} {}, {}.", sequence, event, StackTraceUtil.stackTrace(ex));
        }
    }

    @Override
    public void handleOnStartException(final Throwable ex) {
        if (LOG.isWarnEnabled()) {
            LOG.warn("Exception during onStart(), {}.", StackTraceUtil.stackTrace(ex));
        }
    }

    @Override
    public void handleOnShutdownException(final Throwable ex) {
        if (LOG.isWarnEnabled()) {
            LOG.warn("Exception during onShutdown(), {}.", StackTraceUtil.stackTrace(ex));
        }
    }
}
