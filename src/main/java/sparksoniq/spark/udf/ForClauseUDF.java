/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package sparksoniq.spark.udf;

import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.StructType;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import scala.collection.Seq;
import scala.collection.mutable.WrappedArray;
import sparksoniq.jsoniq.item.Item;
import sparksoniq.jsoniq.runtime.iterator.RuntimeIterator;
import sparksoniq.semantics.DynamicContext;
import sparksoniq.spark.DataFrameUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ForClauseUDF implements UDF1<WrappedArray, List<Seq<byte[]>>> {
    private RuntimeIterator _expression;
    private StructType _inputSchema;
    List<String> _columnNames;

    private List<List<Item>> _deserializedParams;
    private DynamicContext _context;
    private List<Item> _nextResult;
    private List<Seq<byte[]>> _results;
    
    private transient Kryo _kryo;
    private transient Output _output;
    private transient Input _input;

    public ForClauseUDF(
            RuntimeIterator expression,
            StructType inputSchema,
            List<String> columnNames) {
        _expression = expression;
        _inputSchema = inputSchema;
        _columnNames = columnNames;

        _deserializedParams = new ArrayList<>();
        _context = new DynamicContext();
        _nextResult = new ArrayList<>();
        _results = new ArrayList<>();
        
        _kryo = new Kryo();
        _kryo.setReferences(false);
        DataFrameUtils.registerKryoClassesKryo(_kryo);
        _output = new Output(128, -1);
        _input = new Input();
    }


    @Override
    public List<Seq<byte[]>> call(WrappedArray wrappedParameters) {
        _deserializedParams.clear();
        _context.removeAllVariables();
        _results.clear();

        DataFrameUtils.deserializeWrappedParameters(wrappedParameters, _deserializedParams, _kryo, _input);

        DataFrameUtils.prepareDynamicContext(_context, _columnNames, _deserializedParams);

        // apply expression in the dynamic context
        _expression.open(_context);
        while (_expression.hasNext()) {
            _nextResult.clear();
            Item nextItem = _expression.next();
            _nextResult.add(nextItem);
            _results.add(DataFrameUtils.serializeItemList(_nextResult, _kryo, _output));
        }
        _expression.close();

        return _results;
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
