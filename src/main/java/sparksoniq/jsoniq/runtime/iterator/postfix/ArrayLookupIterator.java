/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Authors: Stefan Irimescu, Can Berker Cikis
 *
 */

package sparksoniq.jsoniq.runtime.iterator.postfix;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.rumbledb.api.Item;
import org.rumbledb.exceptions.ExceptionMetadata;
import org.rumbledb.exceptions.InvalidSelectorException;
import org.rumbledb.exceptions.IteratorFlowException;
import org.rumbledb.exceptions.UnexpectedTypeException;
import sparksoniq.jsoniq.ExecutionMode;
import sparksoniq.jsoniq.item.ArrayItem;
import sparksoniq.jsoniq.runtime.iterator.HybridRuntimeIterator;
import sparksoniq.jsoniq.runtime.iterator.RuntimeIterator;
import sparksoniq.semantics.DynamicContext;

import java.util.Arrays;

public class ArrayLookupIterator extends HybridRuntimeIterator {


    private static final long serialVersionUID = 1L;
    private RuntimeIterator iterator;
    private Integer lookup;
    private Item nextResult;

    public ArrayLookupIterator(
            RuntimeIterator array,
            RuntimeIterator iterator,
            ExecutionMode executionMode,
            ExceptionMetadata iteratorMetadata
    ) {
        super(Arrays.asList(array, iterator), executionMode, iteratorMetadata);
        this.iterator = array;
    }

    @Override
    public Item nextLocal() {
        if (this.hasNext) {
            Item result = this.nextResult; // save the result to be returned
            setNextResult(); // calculate and store the next result
            return result;
        }
        throw new IteratorFlowException("Invalid next call in Array Lookup", getMetadata());
    }


    @Override
    protected boolean hasNextLocal() {
        return this.hasNext;
    }

    @Override
    protected void resetLocal(DynamicContext context) {
        this.iterator.reset(this.currentDynamicContextForLocalExecution);
        setNextResult();
    }

    @Override
    protected void closeLocal() {
        this.iterator.close();
    }

    private void initLookupPosition() {
        RuntimeIterator lookupIterator = this.children.get(1);

        lookupIterator.open(this.currentDynamicContextForLocalExecution);
        Item lookupExpression = null;
        if (lookupIterator.hasNext()) {
            lookupExpression = lookupIterator.next();
        }
        if (lookupIterator.hasNext())
            throw new InvalidSelectorException(
                    "\"Invalid Lookup Key; Array lookup can't be performed with multiple keys: "
                        + lookupExpression.serialize(),
                    getMetadata()
            );
        if (!lookupExpression.isNumeric()) {
            throw new UnexpectedTypeException(
                    "Type error; Non numeric array lookup for : "
                        + lookupExpression.serialize(),
                    getMetadata()
            );
        }
        lookupIterator.close();
        try {
            this.lookup = lookupExpression.castToIntegerValue();
        } catch (IteratorFlowException e) {
            throw new IteratorFlowException(e.getJSONiqErrorMessage(), getMetadata());
        }
    }

    @Override
    public void openLocal() {
        initLookupPosition();
        this.iterator.open(this.currentDynamicContextForLocalExecution);
        setNextResult();
    }

    public void setNextResult() {
        this.nextResult = null;

        while (this.iterator.hasNext()) {
            Item item = this.iterator.next();
            if (item instanceof ArrayItem) {
                ArrayItem arrItem = (ArrayItem) item;
                if (this.lookup > 0 && this.lookup <= arrItem.getSize()) {
                    // -1 for Jsoniq convention, arrays start from 1
                    Item result = arrItem.getItemAt(this.lookup - 1);
                    this.nextResult = result;
                    break;
                }
            }
        }

        if (this.nextResult == null) {
            this.hasNext = false;
            this.iterator.close();
        } else {
            this.hasNext = true;
        }
    }

    @Override
    public JavaRDD<Item> getRDDAux(DynamicContext dynamicContext) {
        JavaRDD<Item> childRDD = this.children.get(0).getRDD(dynamicContext);
        initLookupPosition();
        FlatMapFunction<Item, Item> transformation = new ArrayLookupClosure(this.lookup);

        JavaRDD<Item> resultRDD = childRDD.flatMap(transformation);
        return resultRDD;
    }
}
