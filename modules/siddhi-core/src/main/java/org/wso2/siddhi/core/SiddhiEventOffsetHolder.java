/*
 * Copyright (c)  2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package org.wso2.siddhi.core;

import org.apache.log4j.Logger;
import org.wso2.siddhi.core.util.persistence.PersistenceListener;
import org.wso2.siddhi.core.util.snapshot.Snapshotable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class SiddhiEventOffsetHolder implements Snapshotable {
    private static final Logger log = Logger.getLogger(SiddhiEventOffsetHolder.class);
    private static Map<String, Map<String, Object>> receiverSpecificOffsetMap =
            new HashMap<String, Map<String, Object>>();
    private static List<PersistenceListener> persistenceListeners =
            new ArrayList<PersistenceListener>();

    public static void putEventOffset(String receiverName, Map<String, Object> offsetData) {
        receiverSpecificOffsetMap.put(receiverName, offsetData);
    }

    public static Map<String, Object> getLastEventOffset(String receiverName) {
        return receiverSpecificOffsetMap.get(receiverName);
    }

    public static void registerPersistenceListener(PersistenceListener listener) {
        persistenceListeners.add(listener);
    }

    public static List<PersistenceListener> getPersistenceListeners() {
        return persistenceListeners;
    }


    @Override
    public Object[] currentState() {
        return new Object[]{receiverSpecificOffsetMap};
    }

    @Override
    public void restoreState(Object[] state) {
        receiverSpecificOffsetMap = (HashMap<String, Map<String, Object>>) state[0];
        log.info("Offset Map" + receiverSpecificOffsetMap.toString());
    }

    @Override
    public String getElementId() {
        return "SiddhiEventOffsetHolder";
    }
}
