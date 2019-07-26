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

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import sparksoniq.jsoniq.item.Item;
import sparksoniq.jsoniq.runtime.iterator.RuntimeIterator;
import sparksoniq.semantics.DynamicContext;
import sparksoniq.spark.DataFrameUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class ForClauseFlatMapClosure implements FlatMapFunction<Row, Row> {
    private RuntimeIterator _expression;
    private StructType _inputSchema;
    private int _duplicateColumnIndex;

    private List<List<Item>> _rowColumns;
    private DynamicContext _context;
    private List<Row> _results;
    private List<Item> _newColumn;


    private transient Kryo _kryo;
    private transient Output _output;
    private transient Input _input;

    public ForClauseFlatMapClosure(
            RuntimeIterator expression,
            StructType inputSchema,
            int duplicateVariableIndex) {
        _expression = expression;
        _inputSchema = inputSchema;
        _duplicateColumnIndex = duplicateVariableIndex;

        _rowColumns = new ArrayList<>();
        _context = new DynamicContext();
        _results = new ArrayList<>();
        _newColumn = new ArrayList<>();

        _kryo = new Kryo();
        _kryo.setReferences(false);
        DataFrameUtils.registerKryoClassesKryo(_kryo);
        _output = new Output(128, -1);
        _input = new Input();
    }

    @Override
    public Iterator<Row> call(Row row) {
        _rowColumns.clear();
        _context.removeAllVariables();
        _results.clear();

        String[] columnNames = _inputSchema.fieldNames();

        // Deserialize row
        _rowColumns = DataFrameUtils.deserializeEntireRow(row, _kryo, _input);

        // prepare dynamic context
        for (int columnIndex = 0; columnIndex < columnNames.length; columnIndex++) {
            _context.addVariableValue(columnNames[columnIndex], _rowColumns.get(columnIndex));
        }

        // apply expression in the dynamic context
        _expression.open(_context);
        while (_expression.hasNext()) {
            _newColumn.clear();
            Item nextItem = _expression.next();
            _newColumn.add(nextItem);

            Row newRow = DataFrameUtils.reserializeRowWithNewData(row, _newColumn, _duplicateColumnIndex, _kryo, _output);

            _results.add(newRow);
        }
        _expression.close();

        return _results.iterator();
    }
    
    private void readObject(java.io.ObjectInputStream in)
            throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        
        _kryo = new Kryo();
        _kryo.setReferences(false);
        DataFrameUtils.registerKryoClassesKryo(_kryo);
        _output = new Output(128, -1);
        _input = new Input();
    }
}
