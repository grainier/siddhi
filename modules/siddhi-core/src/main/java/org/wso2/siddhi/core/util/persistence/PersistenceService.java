/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.wso2.siddhi.core.util.persistence;

import org.apache.log4j.Logger;
import org.wso2.siddhi.core.SiddhiEventOffsetHolder;
import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.exception.NoPersistenceStoreException;
import org.wso2.siddhi.core.util.ThreadBarrier;
import org.wso2.siddhi.core.util.snapshot.SnapshotService;

public class PersistenceService {

    static final Logger log = Logger.getLogger(PersistenceService.class);
    private String executionPlanName;
    private PersistenceStore persistenceStore;
    private SnapshotService snapshotService;
    private ThreadBarrier threadBarrier;

    public PersistenceService(ExecutionPlanContext executionPlanContext) {
        this.snapshotService = executionPlanContext.getSnapshotService();
        this.persistenceStore = executionPlanContext.getSiddhiContext().getPersistenceStore();
        this.executionPlanName = executionPlanContext.getName();
        this.threadBarrier = executionPlanContext.getThreadBarrier();
    }


    public String persist() {

        if (persistenceStore != null) {
            if (log.isDebugEnabled()) {
                log.debug("Persisting...");
            }
            byte[] snapshot = snapshotService.snapshot();
            byte[] snapshotOffset = snapshotService.snapshotOffsetHolder();
            String revision = System.currentTimeMillis() + "_" + executionPlanName;
            persistenceStore.save(executionPlanName, revision, snapshot);
            persistenceStore.save(executionPlanName+"offset", revision, snapshotOffset);
            for (PersistenceListener persistenceListener : SiddhiEventOffsetHolder.getPersistenceListeners()) {
                persistenceListener.onSave(executionPlanName, revision);
            }
            if (log.isDebugEnabled()) {
                log.debug("Persisted.");
            }
            return revision;
        } else {
            throw new NoPersistenceStoreException("No persistence store assigned for execution plan " + executionPlanName);
        }

    }

    public void restoreRevision(String revision) {

        if (persistenceStore != null) {
            if (log.isDebugEnabled()) {
                log.debug("Restoring revision: " + revision + " ...");
            }
            byte[] snapshot = persistenceStore.load(executionPlanName, revision);
            snapshotService.restore(snapshot);
            if (log.isDebugEnabled()) {
                log.debug("Restored revision: " + revision);
            }
        } else {
            throw new NoPersistenceStoreException("No persistence store assigned for execution plan " + executionPlanName);
        }

    }

    public void restoreRevisionOffset(String revision) {

        if (persistenceStore != null) {
            if (log.isDebugEnabled()) {
                log.debug("Restoring revision Offset: " + revision + " ...");
            }
            byte[] snapshot = persistenceStore.load(executionPlanName+"offset", revision);
            snapshotService.restoreOffsetHolder(snapshot);
            if (log.isDebugEnabled()) {
                log.debug("Restored revision Offset: " + revision);
            }
        } else {
            throw new NoPersistenceStoreException("No persistence store assigned for execution plan " + executionPlanName);
        }

    }

    public void restoreLastRevision() {
        try {
            this.threadBarrier.lock();
            if (persistenceStore != null) {
                String revision = persistenceStore.getLastRevision(executionPlanName);
                String revisionOffset = persistenceStore.getLastRevision(executionPlanName+"offset");
                if (revision != null) {
                    restoreRevision(revision);
                    restoreRevisionOffset(revisionOffset);
                }
            } else {
                throw new NoPersistenceStoreException("No persistence store assigned for execution plan " + executionPlanName);
            }
        } finally {
            threadBarrier.unlock();
        }
    }
}
