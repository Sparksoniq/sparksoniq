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
import org.rumbledb.exceptions.RumbleException;
import org.rumbledb.expressions.ExecutionMode;
import org.rumbledb.items.parsing.ItemParser;

import com.dslplatform.json.DslJson;
import com.dslplatform.json.JsonReader;

import org.rumbledb.runtime.RuntimeIterator;
import org.rumbledb.runtime.functions.base.LocalFunctionCallIterator;
import org.rumbledb.runtime.functions.input.FileSystemUtil;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.List;

public class JsonDocFunctionIterator extends LocalFunctionCallIterator {

    private static final long serialVersionUID = 1L;
    private RuntimeIterator iterator;

    public JsonDocFunctionIterator(
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
        this.iterator.open(this.currentDynamicContextForLocalExecution);
        this.hasNext = this.iterator.hasNext();
        this.iterator.close();
    }

    @Override
    public Item next() {
        if (this.hasNext) {
            this.hasNext = false;
            Item path = this.iterator.materializeFirstItemOrNull(this.currentDynamicContextForLocalExecution);
            try {
                URI uri = FileSystemUtil.resolveURI(
                    this.staticURI,
                    path.getStringValue(),
                    getMetadata()
                );
                InputStream is = FileSystemUtil.getDataInputStream(
                    uri,
                    this.currentDynamicContextForLocalExecution.getRumbleRuntimeConfiguration(),
                    getMetadata()
                );
                DslJson<Object> dslJson = new DslJson<Object>();
                JsonReader<Object> object = dslJson.newReader();
                object.process(is);
                return ItemParser.getItemFromObject(object, getMetadata(), true);
            } catch (IteratorFlowException e) {
                RumbleException ex = new IteratorFlowException(e.getJSONiqErrorMessage(), getMetadata());
                ex.initCause(e);
                throw ex;
            } catch (IOException e) {
                RumbleException ex = new IteratorFlowException(
                        "I/O Exception while binding the input stream",
                        getMetadata()
                );
                ex.initCause(e);
                throw ex;
            }
        }
        throw new IteratorFlowException(RuntimeIterator.FLOW_EXCEPTION_MESSAGE + " json-doc function", getMetadata());
    }


}
