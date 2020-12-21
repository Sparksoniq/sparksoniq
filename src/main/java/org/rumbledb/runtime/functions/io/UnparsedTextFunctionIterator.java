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
 * Authors: Ghislain Fourny
 *
 */


package org.rumbledb.runtime.functions.io;

import org.rumbledb.api.Item;
import org.rumbledb.context.DynamicContext;
import org.rumbledb.exceptions.ExceptionMetadata;
import org.rumbledb.exceptions.IteratorFlowException;
import org.rumbledb.expressions.ExecutionMode;
import org.rumbledb.items.ItemFactory;
import org.rumbledb.runtime.RuntimeIterator;
import org.rumbledb.runtime.functions.base.LocalFunctionCallIterator;
import org.rumbledb.runtime.functions.input.FileSystemUtil;

import java.net.URI;
import java.util.List;

public class UnparsedTextFunctionIterator extends LocalFunctionCallIterator {

    private static final long serialVersionUID = 1L;
    private RuntimeIterator iterator;

    private transient Item path;

    public UnparsedTextFunctionIterator(
            List<RuntimeIterator> arguments,
            ExecutionMode executionMode,
            ExceptionMetadata iteratorMetadata
    ) {
        super(arguments, executionMode, iteratorMetadata);
    }

    @Override
    public void open(DynamicContext context) {
        super.open(context);
        this.iterator = this.children.get(0);
        this.path = this.iterator.materializeFirstItemOrNull(context);
        this.hasNext = this.path != null;
    }

    @Override
    public void reset(DynamicContext context) {
        super.reset(context);
        this.iterator = this.children.get(0);
        this.path = this.iterator.materializeFirstItemOrNull(context);
        this.hasNext = this.path != null;
    }

    @Override
    public void close() {
        super.close();
        this.path = null;
    }

    @Override
    public Item next() {
        if (this.hasNext) {
            URI uri = FileSystemUtil.resolveURI(
                this.staticURI,
                this.path.getStringValue(),
                getMetadata()
            );
            String result = FileSystemUtil.readContent(
                uri,
                this.currentDynamicContextForLocalExecution.getRumbleRuntimeConfiguration(),
                getMetadata()
            );
            this.hasNext = false;
            return ItemFactory.getInstance().createStringItem(result);
        }
        throw new IteratorFlowException(
                RuntimeIterator.FLOW_EXCEPTION_MESSAGE + " unparsed-text function",
                getMetadata()
        );
    }


}
