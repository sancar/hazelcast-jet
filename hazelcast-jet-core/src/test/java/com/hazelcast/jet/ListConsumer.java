/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;

class ListConsumer extends AbstractProcessor {

    private final List<Object> list;
    private boolean isComplete;
    private int yieldIndex = -1;

    ListConsumer() {
        list = new ArrayList<>();
    }

    @Override
    protected boolean tryProcess(int ordinal, @Nonnull Object item) throws Exception {
        if (list.size() == yieldIndex) {
            yieldIndex = -1;
            return false;
        }
        list.add(item);
        return true;
    }

    @Override
    public boolean complete() {
        isComplete = true;
        return true;
    }

    public void yieldOn(int index) {
        yieldIndex = index;
    }

    public boolean isComplete() {
        return isComplete;
    }

    public List<Object> getList() {
        return list;
    }
}
