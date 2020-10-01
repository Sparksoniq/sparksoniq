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

package org.rumbledb.runtime.functions.input;

import org.apache.spark.api.java.JavaRDD;
import org.rumbledb.api.Item;
import org.rumbledb.context.DynamicContext;
import org.rumbledb.exceptions.ExceptionMetadata;
import org.rumbledb.expressions.ExecutionMode;
import org.rumbledb.items.parsing.BSONDocumentToItemMapper;
import org.rumbledb.runtime.RDDRuntimeIterator;
import org.rumbledb.runtime.RuntimeIterator;

import sparksoniq.spark.SparkSessionManager;

import org.apache.spark.api.java.JavaSparkContext;
import org.bson.Document;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MongoCollectionFunctionIterator extends RDDRuntimeIterator {

    private static final long serialVersionUID = 1L;

    public MongoCollectionFunctionIterator(
            List<RuntimeIterator> arguments,
            ExecutionMode executionMode,
            ExceptionMetadata iteratorMetadata
    ) {
        super(arguments, executionMode, iteratorMetadata);
    }

    @Override
    public JavaRDD<Item> getRDDAux(DynamicContext context) {
        String collection = this.children.get(0).materializeFirstItemOrNull(context).getStringValue();
        //String collection = null;
        //if(this.children.size() > 1)
        //{
        //    collection = this.children.get(1).materializeFirstItemOrNull(context).getStringValue();
        //}
        
        JavaSparkContext jsc = SparkSessionManager.getInstance().getJavaSparkContext();

        Map<String, String> readOverrides = new HashMap<String, String>();
        //readOverrides.put("uri", connectionString);
        //if(collection != null)
        //{
            readOverrides.put("collection", collection);
        //}
        ReadConfig readConfig = ReadConfig.create(jsc).withOptions(readOverrides);

        JavaMongoRDD<Document> documents = MongoSpark.load(jsc, readConfig);
        return documents.mapPartitions(new BSONDocumentToItemMapper(getMetadata()));
    }
}
