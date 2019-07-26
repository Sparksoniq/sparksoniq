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

import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;

import scala.collection.Seq;
import sparksoniq.jsoniq.item.Item;
import sparksoniq.jsoniq.tuple.FlworTuple;
import sparksoniq.spark.DataFrameUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ForClauseLocalToRowClosure implements Function<Item, Row> {
    private final FlworTuple _inputTuple;

    private transient Kryo _kryo;
    private transient Output _output;

    public ForClauseLocalToRowClosure(FlworTuple inputTuple) {
        this._inputTuple = inputTuple;
        _kryo = new Kryo();
        _kryo.setReferences(false);
        DataFrameUtils.registerKryoClassesKryo(_kryo);
        _output = new Output(128, -1);
    }

    @Override
    public Row call(Item item) throws Exception {
        List<List<Item>> rowColumns = new ArrayList<>();
        _inputTuple.getKeys().forEach(key -> rowColumns.add(_inputTuple.getValue(key)));

        List<Item> itemList = new ArrayList<>();
        itemList.add(item);
        rowColumns.add(itemList);

        List<Seq<byte[]>> serializedRowColumns = new ArrayList<>();
        for (List<Item> column : rowColumns) {
            serializedRowColumns.add(DataFrameUtils.serializeItemList(column, _kryo, _output));
        }

        return RowFactory.create(serializedRowColumns.toArray());
    }
    
    private void readObject(java.io.ObjectInputStream in)
            throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        
        _kryo = new Kryo();
        _kryo.setReferences(false);
        DataFrameUtils.registerKryoClassesKryo(_kryo);
        _output = new Output(128, -1);
    }
}
