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

package sparksoniq.jsoniq.runtime.iterator.functions.sequences.general;

import org.rumbledb.api.Item;
import org.rumbledb.exceptions.ExceptionMetadata;
import org.rumbledb.exceptions.IteratorFlowException;
import sparksoniq.jsoniq.ExecutionMode;
import sparksoniq.jsoniq.runtime.iterator.RuntimeIterator;
import sparksoniq.jsoniq.runtime.iterator.functions.base.LocalFunctionCallIterator;
import sparksoniq.semantics.DynamicContext;

import java.util.List;

public class TailFunctionIterator extends LocalFunctionCallIterator {


    private static final long serialVersionUID = 1L;
    private RuntimeIterator iterator;
    private Item nextResult;

    public TailFunctionIterator(
            List<RuntimeIterator> parameters,
            ExecutionMode executionMode,
            ExceptionMetadata iteratorMetadata
    ) {
        super(parameters, executionMode, iteratorMetadata);
    }

    @Override
    public Item next() {
        if (this.hasNext()) {
            Item result = this.nextResult; // save the result to be returned
            setNextResult(); // calculate and store the next result
            return result;
        }
        throw new IteratorFlowException(FLOW_EXCEPTION_MESSAGE + "tail function", getMetadata());
    }

    @Override
    public void open(DynamicContext context) {
        super.open(context);

        this.iterator = this.children.get(0);
        this.iterator.open(context);

        if (!this.iterator.hasNext()) {
            this.hasNext = false;
        } else {
            this.iterator.next(); // skip the first item
            setNextResult();
        }
    }

    public void setNextResult() {
        this.nextResult = null;

        if (this.iterator.hasNext()) {
            this.nextResult = this.iterator.next();
        }

        if (this.nextResult == null) {
            this.hasNext = false;
            this.iterator.close();
        } else {
            this.hasNext = true;
        }
    }

}
