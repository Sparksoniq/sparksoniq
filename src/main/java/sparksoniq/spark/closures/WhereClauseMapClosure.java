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
 package sparksoniq.spark.closures;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;

import sparksoniq.jsoniq.item.Item;
import sparksoniq.jsoniq.runtime.iterator.RuntimeIterator;
import sparksoniq.semantics.DynamicContext;
import sparksoniq.spark.DataFrameUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class WhereClauseMapClosure implements FilterFunction<Row> {
    private final RuntimeIterator _expression;
    StructType _inputSchema;

    private List<List<Item>> _rowColumns;
    private DynamicContext _context;
    
    private transient Kryo _kryo;
    private transient Input _input;

    public WhereClauseMapClosure(RuntimeIterator expression, StructType inputSchema) {
        this._expression = expression;
        this._inputSchema = inputSchema;

        _rowColumns = new ArrayList<>();
        _context = new DynamicContext();
        
        _kryo = new Kryo();
        _kryo.setReferences(false);
        DataFrameUtils.registerKryoClassesKryo(_kryo);
        _input = new Input();
    }

    @Override
    public boolean call(Row row) throws Exception {
        _rowColumns.clear();
        _context.removeAllVariables();

        String[] columnNames = _inputSchema.fieldNames();

        // Deserialize row
        _rowColumns = DataFrameUtils.deserializeEntireRow(row, _kryo, _input);


        // Create dynamic context with deserialized data
        for (int columnIndex = 0; columnIndex < columnNames.length; columnIndex++) {
            _context.addVariableValue(columnNames[columnIndex], _rowColumns.get(columnIndex));
        }

        _expression.open(_context);
        Item result = _expression.next();
        _expression.close();
        return result.getBooleanValue();
    }
    
    private void readObject(java.io.ObjectInputStream in)
            throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        
        _kryo = new Kryo();
        _kryo.setReferences(false);
        DataFrameUtils.registerKryoClassesKryo(_kryo);
        _input = new Input();
    }
}
