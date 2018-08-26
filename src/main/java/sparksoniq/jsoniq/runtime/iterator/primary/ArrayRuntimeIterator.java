/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Author: Stefan Irimescu
 *
 */
 package sparksoniq.jsoniq.runtime.iterator.primary;

import sparksoniq.jsoniq.item.ArrayItem;
import sparksoniq.jsoniq.item.Item;
import sparksoniq.jsoniq.item.metadata.ItemMetadata;
import sparksoniq.jsoniq.runtime.iterator.LocalRuntimeIterator;
import sparksoniq.exceptions.IteratorFlowException;
import sparksoniq.jsoniq.runtime.iterator.RuntimeIterator;
import sparksoniq.jsoniq.runtime.metadata.IteratorMetadata;
import sparksoniq.semantics.DynamicContext;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;

public class ArrayRuntimeIterator extends LocalRuntimeIterator {

    public ArrayRuntimeIterator(RuntimeIterator arrayItems, IteratorMetadata iteratorMetadata) {
        super(new ArrayList<>(), iteratorMetadata);
        if(arrayItems!=null)
            this._children.add(arrayItems);
    }

    @Override
    public ArrayItem next() {
        if (this.hasNext()) {
            this._hasNext = false;
            return result;
        }
        else throw new IteratorFlowException("Invalid next() call on array iterator", getMetadata());
    }

    @Override
    public void open(DynamicContext context) {
        if (this._isOpen)
            throw new IteratorFlowException("Runtime iterator cannot be opened twice", getMetadata());
        this._isOpen = true;
        this._currentDynamicContext = context;

        List<Item> result = this.runChildrenIterators(this._currentDynamicContext);
        this.result = new ArrayItem(result, ItemMetadata.fromIteratorMetadata(getMetadata()));
        this._hasNext = true;
    }

    private ArrayItem result;
}
