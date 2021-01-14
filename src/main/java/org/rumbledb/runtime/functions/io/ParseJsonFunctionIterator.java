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

import com.dslplatform.json.DslJson;
import com.dslplatform.json.JsonReader;
import org.rumbledb.api.Item;
import org.rumbledb.context.DynamicContext;
import org.rumbledb.exceptions.ExceptionMetadata;
import org.rumbledb.exceptions.IteratorFlowException;
import org.rumbledb.exceptions.RumbleException;
import org.rumbledb.expressions.ExecutionMode;
import org.rumbledb.items.parsing.ItemParser;
import org.rumbledb.runtime.RuntimeIterator;
import org.rumbledb.runtime.functions.base.LocalFunctionCallIterator;

import java.nio.charset.Charset;
import java.util.List;

public class ParseJsonFunctionIterator extends LocalFunctionCallIterator {

    private static final long serialVersionUID = 1L;

    private transient Item string;

    public ParseJsonFunctionIterator(
            List<RuntimeIterator> arguments,
            ExecutionMode executionMode,
            ExceptionMetadata iteratorMetadata
    ) {
        super(arguments, executionMode, iteratorMetadata);
    }

    @Override
    public void open(DynamicContext context) {
        super.open(context);
        this.string = this.children.get(0).materializeFirstItemOrNull(context);
        this.hasNext = this.string != null;
    }

    @Override
    public void reset(DynamicContext context) {
        super.reset(context);
        this.string = this.children.get(0).materializeFirstItemOrNull(context);
        this.hasNext = this.string != null;
    }

    @Override
    public void close() {
        super.close();
    }

    @Override
    public Item next() {
        if (this.hasNext) {
            this.hasNext = false;
            try {
                DslJson<Object> dslJson = new DslJson<Object>();
                JsonReader<Object> object = dslJson.newReader(
                    this.string.getStringValue().getBytes(Charset.forName("UTF-8"))
                );
                return ItemParser.getItemFromObject(object, getMetadata(), true);
            } catch (IteratorFlowException e) {
                RumbleException ex = new IteratorFlowException(e.getJSONiqErrorMessage(), getMetadata());
                ex.initCause(e);
                throw ex;
            }
        }
        throw new IteratorFlowException(RuntimeIterator.FLOW_EXCEPTION_MESSAGE + " json-parse function", getMetadata());
    }


}
