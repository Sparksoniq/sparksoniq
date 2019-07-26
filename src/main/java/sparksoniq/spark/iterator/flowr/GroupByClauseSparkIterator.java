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

package sparksoniq.spark.iterator.flowr;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import sparksoniq.exceptions.InvalidGroupVariableException;
import sparksoniq.exceptions.IteratorFlowException;
import sparksoniq.exceptions.NonAtomicKeyException;
import sparksoniq.exceptions.SparksoniqRuntimeException;
import sparksoniq.jsoniq.item.Item;
import sparksoniq.jsoniq.runtime.iterator.RuntimeIterator;
import sparksoniq.jsoniq.runtime.iterator.primary.VariableReferenceIterator;
import sparksoniq.jsoniq.runtime.metadata.IteratorMetadata;
import sparksoniq.jsoniq.runtime.tupleiterator.RuntimeTupleIterator;
import sparksoniq.jsoniq.runtime.tupleiterator.SparkRuntimeTupleIterator;
import sparksoniq.jsoniq.tuple.FlworKey;
import sparksoniq.jsoniq.tuple.FlworTuple;
import sparksoniq.semantics.DynamicContext;
import sparksoniq.spark.DataFrameUtils;
import sparksoniq.spark.closures.GroupByLinearizeTupleClosure;
import sparksoniq.spark.closures.GroupByToPairMapClosure;
import sparksoniq.spark.iterator.flowr.expression.GroupByClauseSparkIteratorExpression;
import sparksoniq.spark.udf.GroupClauseCreateColumnsUDF;
import sparksoniq.spark.udf.GroupClauseSerializeAggregateResultsUDF;
import sparksoniq.spark.udf.LetClauseUDF;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class GroupByClauseSparkIterator extends SparkRuntimeTupleIterator {
    private final List<GroupByClauseSparkIteratorExpression> _expressions;
    private List<FlworTuple> _localTupleResults;
    private int _resultIndex;
    Set<String> _dependencies;

    public GroupByClauseSparkIterator(RuntimeTupleIterator child, List<GroupByClauseSparkIteratorExpression> variables,
                                      IteratorMetadata iteratorMetadata) {
        super(child, iteratorMetadata);
        this._expressions = variables;
        _dependencies = new HashSet<String>();
        for(GroupByClauseSparkIteratorExpression e : _expressions)
        {
            if(e.getExpression() != null)
            {
                _dependencies.addAll(e.getExpression().getVariableDependencies());
            } else {
                _dependencies.add(e.getVariableReference().getVariableName());
            }
        }
    }

    @Override
    public boolean isRDD() {
        return _child.isRDD();
    }

    @Override
    public boolean isDataFrame() {
        return _child.isDataFrame();
    }

    @Override
    public void open(DynamicContext context) {
        super.open(context);

        // isRDD checks omitted, as open is used for non-RDD(local) operations

        if (this._child != null) {
            _child.open(_currentDynamicContext);

            if (_child.hasNext()) {
                this._hasNext = true;
            } else {
                this._hasNext = false;
            }

        } else {
            throw new SparksoniqRuntimeException("Invalid groupby clause.");
        }
    }

    @Override
    public FlworTuple next() {
        if (_hasNext) {

            if (_localTupleResults == null) {
                _localTupleResults = new ArrayList<>();
                _resultIndex = 0;
                setAllLocalResults();
            }

            FlworTuple result = _localTupleResults.get(_resultIndex++);
            if (_resultIndex == _localTupleResults.size()) {
                this._hasNext = false;
            }
            return result;
        }
        throw new IteratorFlowException("Invalid next() call in let flwor clause", getMetadata());
    }

    /**
     * All local results need to be calculated for grouping to be performed.
     */
    private void setAllLocalResults() {
        Map<FlworKey, List<FlworTuple>> keyTuplePairs = mapTuplesToPairs();
        keyTuplePairs.forEach((key, tupleList) -> linearizeTuples(tupleList));

        _child.close();
        if (_localTupleResults.size() == 0) {
            this._hasNext = false;
        } else {
            this._hasNext = true;
        }
    }


    private HashMap<FlworKey, List<FlworTuple>> mapTuplesToPairs() {
        HashMap<FlworKey, List<FlworTuple>> keyValuePairs = new HashMap<>();

        // assign current context as parent. re-use the same context object for efficiency
        DynamicContext tupleContext = new DynamicContext(_currentDynamicContext);
        while (_child.hasNext()) {
            FlworTuple inputTuple = _child.next();

            List<Item> results = new ArrayList<>();
            for (GroupByClauseSparkIteratorExpression expression : _expressions) {
                tupleContext.removeAllVariables();                     // clear the previous variables
                tupleContext.setBindingsFromTuple(inputTuple);        // assign new variables from new tuple

                // if grouping on an expression
                RuntimeIterator groupVariableExpression = expression.getExpression();
                if (groupVariableExpression != null) {
                    if (inputTuple.contains(expression.getVariableReference().getVariableName())) {
                        throw new InvalidGroupVariableException("Group by variable redeclaration is illegal", expression.getIteratorMetadata());
                    }

                    List<Item> newVariableResults = new ArrayList<>();
                    groupVariableExpression.open(tupleContext);
                    while (groupVariableExpression.hasNext()) {
                        Item resultItem = groupVariableExpression.next();
                        if (!resultItem.isAtomic()) {
                            throw new NonAtomicKeyException("Group by keys must be atomics", expression.getIteratorMetadata().getExpressionMetadata());
                        }
                        newVariableResults.add(resultItem);
                    }
                    groupVariableExpression.close();

                    //if a new variable is declared inside the group by clause, insert value in tuple
                    inputTuple.putValue(expression.getVariableReference().getVariableName(), newVariableResults, false);
                    results.addAll(newVariableResults);

                } else { // if grouping on a variable reference
                    VariableReferenceIterator groupVariableReference = expression.getVariableReference();
                    if (!inputTuple.contains(groupVariableReference.getVariableName())) {
                        throw new InvalidGroupVariableException("Variable " + groupVariableReference.getVariableName() + " cannot be used in group clause", expression.getIteratorMetadata());
                    }

                    groupVariableReference.open(tupleContext);
                    while (groupVariableReference.hasNext()) {
                        results.add(groupVariableReference.next());
                    }
                    groupVariableReference.close();
                }
            }
            FlworKey key = new FlworKey(results);
            List<FlworTuple> values = keyValuePairs.get(key);   // all values for a single matching key are held in a list
            if (values == null) {
                values = new ArrayList<>();
                keyValuePairs.put(key, values);
            }
            values.add(inputTuple);
        }
        return keyValuePairs;
    }

    private void linearizeTuples(List<FlworTuple> keyTuplePairs) {
        Iterator<FlworTuple> iterator = keyTuplePairs.iterator();
        FlworTuple oldFirstTuple = iterator.next();
        FlworTuple newTuple = new FlworTuple(oldFirstTuple.getKeys().size());
        for (String tupleVariable : oldFirstTuple.getKeys()) {
            iterator = keyTuplePairs.iterator();
            if (_expressions.stream().anyMatch(v -> v.getVariableReference().getVariableName().equals(tupleVariable)))
                newTuple.putValue(tupleVariable, oldFirstTuple.getValue(tupleVariable), false);
            else {
                List<Item> allValues = new ArrayList<>();
                while (iterator.hasNext())
                    allValues.addAll(iterator.next().getValue(tupleVariable));
                newTuple.putValue(tupleVariable, allValues, false);
            }
        }
        _localTupleResults.add(newTuple);
    }


    @Override
    public JavaRDD<FlworTuple> getRDD(DynamicContext context) {
        _rdd = this._child.getRDD(context);
        //map to pairs - ArrayItem [sort keys] , tuples
        JavaPairRDD<FlworKey, FlworTuple> keyTuplePair = this._rdd
                .mapToPair(new GroupByToPairMapClosure(_expressions));
        //group by key
        JavaPairRDD<FlworKey, Iterable<FlworTuple>> groupedPair =
                keyTuplePair.groupByKey();
        //linearize iterable tuples into arrays
        this._rdd = groupedPair.map(new GroupByLinearizeTupleClosure(_expressions));
        return _rdd;
    }

    @Override
    public Dataset<Row> getDataFrame(DynamicContext context) {
        if (this._child == null) {
            throw new SparksoniqRuntimeException("Invalid groupby clause.");
        }
        Dataset<Row> df = _child.getDataFrame(context);
        StructType inputSchema;
        String[] columnNamesArray;
        List<String> columnNames;

        List<VariableReferenceIterator> variableAccessExpressions = new ArrayList<>();
        for (int expressionIndex = 0; expressionIndex < _expressions.size(); expressionIndex++) {
            GroupByClauseSparkIteratorExpression expression = _expressions.get(expressionIndex);
            inputSchema = df.schema();
            columnNamesArray = inputSchema.fieldNames();
            columnNames = Arrays.asList(columnNamesArray);

            if (expression.getExpression() != null) {
                // if a variable is defined in-place with groupby, execute a let on the variable
                variableAccessExpressions.add(expression.getVariableReference());
                String newVariableName = expression.getVariableReference().getVariableName();
                RuntimeIterator newVariableExpression = expression.getExpression();
                int duplicateVariableIndex = columnNames.indexOf(newVariableName);

                List<String> allColumns = DataFrameUtils.getColumnNames(inputSchema, duplicateVariableIndex, null);
                List<String> UDFcolumns = DataFrameUtils.getColumnNames(inputSchema, -1, _dependencies);

                df.sparkSession().udf().register("letClauseUDF",
                        new LetClauseUDF(newVariableExpression, inputSchema, UDFcolumns), DataTypes.BinaryType);

                String selectSQL = DataFrameUtils.getSQL(allColumns, true);
                String udfSQL = DataFrameUtils.getSQL(UDFcolumns, false);

                df.createOrReplaceTempView("input");
                df = df.sparkSession().sql(
                        String.format("select %s letClauseUDF(array(%s)) as `%s` from input",
                                selectSQL, udfSQL, newVariableName)
                );

            } else {
                if (!columnNames.contains(expression.getVariableReference().getVariableName())) {
                    throw new InvalidGroupVariableException(
                            "Variable " + expression.getVariableReference().getVariableName() + " cannot be used in group clause", expression.getIteratorMetadata()
                    );
                }
                variableAccessExpressions.add(expression.getVariableReference());
            }
        }

        // determine grouping data types after all variable introductions are completed
        inputSchema = df.schema();
        Set<String> groupingVariables = new HashSet<String>();

        df.createOrReplaceTempView("input");

        // Determine the return type for grouping UDF
        List<StructField> typedFields = new ArrayList<>();
        String appendedGroupingColumnsName = "grouping_columns";
        for (int columnIndex = 0; columnIndex < _expressions.size(); columnIndex++) {
            groupingVariables.add(_expressions.get(columnIndex).getVariableReference().getVariableName());
            // every expression contains an int column for null/empty/true/false/string/double check
            String columnName = columnIndex + "-nullEmptyBooleanCheckField";
            typedFields.add(DataTypes.createStructField(columnName, DataTypes.IntegerType, false));
            columnName = columnIndex + "-stringField";
            DataType columnType = DataTypes.StringType;
            typedFields.add(DataTypes.createStructField(columnName, columnType, true));
            columnName = columnIndex + "-doubleField";
            columnType = DataTypes.DoubleType;
            typedFields.add(DataTypes.createStructField(columnName, columnType, true));
        }

        String serializerUDFName = "serialize";
        df.sparkSession().udf().register(serializerUDFName,
                new GroupClauseSerializeAggregateResultsUDF(),
                DataTypes.createArrayType(DataTypes.BinaryType));
        

        List<String> allColumns = DataFrameUtils.getColumnNames(inputSchema);
        List<String> UDFcolumns = DataFrameUtils.getColumnNames(inputSchema, -1, groupingVariables);

        df.sparkSession().udf().register("createGroupingColumns",
                new GroupClauseCreateColumnsUDF(variableAccessExpressions, UDFcolumns),
                DataTypes.createStructType(typedFields));

        String selectSQL = DataFrameUtils.getSQL(allColumns, true);
        String udfSQL = DataFrameUtils.getSQL(UDFcolumns, false);

        String createColumnsSQL = String.format(
                "select %s createGroupingColumns(array(%s)) as `%s` from input",
                selectSQL, udfSQL, appendedGroupingColumnsName
        );

        List<String> groupbyVariableNames = new ArrayList<>();
        for (VariableReferenceIterator variableAccessExpression : variableAccessExpressions) {
            groupbyVariableNames.add(variableAccessExpression.getVariableName());
        }
        String projectSQL = DataFrameUtils.getGroupbyProjectSQL(
                inputSchema,
                -1,
                false,
                serializerUDFName,
                groupbyVariableNames
        );

        return df.sparkSession().sql(
                String.format(
                        "select %s from (%s) group by `%s`",
                        projectSQL, createColumnsSQL, appendedGroupingColumnsName
                )
        );
    }

    public Set<String> getVariableDependencies()
    {
        Set<String> result = new HashSet<String>();
        for(GroupByClauseSparkIteratorExpression iterator : _expressions)
        {
            result.addAll(iterator.getExpression().getVariableDependencies());
        }
        result.removeAll(_child.getVariablesBoundInCurrentFLWORExpression());
        result.addAll(_child.getVariableDependencies());
        return result;
    }

    public Set<String> getVariablesBoundInCurrentFLWORExpression()
    {
        Set<String> result = new HashSet<String>();
        for(GroupByClauseSparkIteratorExpression iterator : _expressions)
        {
            result.add(iterator.getVariableReference().getVariableName());
        }
        result.addAll(_child.getVariablesBoundInCurrentFLWORExpression());
        return result;
    }
    
    public void print(StringBuffer buffer, int indent)
    {
        super.print(buffer, indent);
        for(GroupByClauseSparkIteratorExpression iterator : _expressions)
        {
            for (int i = 0; i < indent + 1; ++i)
            {
                buffer.append("  ");
            }
            buffer.append("Variable " + iterator.getVariableReference().getVariableName());
            buffer.append("\n");
            iterator.getExpression().print(buffer, indent+1);
        }
    }
}
