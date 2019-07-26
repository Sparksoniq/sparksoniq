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

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import sparksoniq.exceptions.IteratorFlowException;
import sparksoniq.exceptions.SparksoniqRuntimeException;
import sparksoniq.jsoniq.item.Item;
import sparksoniq.jsoniq.runtime.iterator.RuntimeIterator;
import sparksoniq.jsoniq.runtime.iterator.primary.VariableReferenceIterator;
import sparksoniq.jsoniq.runtime.metadata.IteratorMetadata;
import sparksoniq.jsoniq.runtime.tupleiterator.RuntimeTupleIterator;
import sparksoniq.jsoniq.runtime.tupleiterator.SparkRuntimeTupleIterator;
import sparksoniq.jsoniq.tuple.FlworTuple;
import sparksoniq.semantics.DynamicContext;
import sparksoniq.spark.DataFrameUtils;
import sparksoniq.spark.closures.OLD_LetClauseMapClosure;
import sparksoniq.spark.udf.LetClauseUDF;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class LetClauseSparkIterator extends SparkRuntimeTupleIterator {

    private String _variableName;           // for efficient use in local iteration
    private RuntimeIterator _expression;
    private DynamicContext _tupleContext;   // re-use same DynamicContext object for efficiency
    private FlworTuple _nextLocalTupleResult;
    Set<String> _dependencies;

    public LetClauseSparkIterator(RuntimeTupleIterator child, VariableReferenceIterator variableReference, RuntimeIterator expression, IteratorMetadata iteratorMetadata) {
        super(child, iteratorMetadata);
        _variableName = variableReference.getVariableName();
        _expression = expression;
        _dependencies = _expression.getVariableDependencies();
    }

    @Override
    public boolean isRDD() {
        if (this._child == null) {
            return false;
        } else {
            return _child.isRDD();
        }
    }

    @Override
    public boolean isDataFrame() {
        if (this._child == null) {
            return false;
        } else {
            return _child.isDataFrame();
        }
    }

    @Override
    public FlworTuple next() {
        if (_hasNext == true) {
            FlworTuple result = _nextLocalTupleResult;      // save the result to be returned
            setNextLocalTupleResult();              // calculate and store the next result
            return result;
        }
        throw new IteratorFlowException("Invalid next() call in let flwor clause", getMetadata());
    }

    private void setNextLocalTupleResult() {
        // if first let clause, there are no more tuples
        if (this._child == null) {
            this._hasNext = false;
            return;
        }

        if (_child.hasNext()) {
            FlworTuple inputTuple = _child.next();
            _tupleContext.removeAllVariables();             // clear the previous variables
            _tupleContext.setBindingsFromTuple(inputTuple);      // assign new variables from new tuple

            List<Item> results = new ArrayList<>();
            _expression.open(_tupleContext);
            while (_expression.hasNext())
                results.add(_expression.next());
            _expression.close();

            FlworTuple newTuple = new FlworTuple(inputTuple, _variableName, results);
            _nextLocalTupleResult = newTuple;
            this._hasNext = true;
        } else {
            _child.close();
            this._hasNext = false;
        }
    }

    @Override
    public void open(DynamicContext context) {
        super.open(context);

        // isRDD checks omitted, as open is used for non-RDD(local) operations

        if (this._child != null) { //if it's not a start clause
            _child.open(_currentDynamicContext);
            _tupleContext = new DynamicContext(_currentDynamicContext);     // assign current context as parent

            setNextLocalTupleResult();
        } else {    //if it's a start clause, it returns only one tuple
            // expression is materialized
            List<Item> results = new ArrayList<>();
            _expression.open(this._currentDynamicContext);
            while (_expression.hasNext())
                results.add(_expression.next());
            _expression.close();

            FlworTuple newTuple = new FlworTuple(_variableName, results);
            _nextLocalTupleResult = newTuple;
        }
    }

    @Override
    public void close() {
        this._isOpen = false;
        result = null;
        if (_child != null) {
            _child.close();
        }
    }

    @Override
    public JavaRDD<FlworTuple> getRDD(DynamicContext context) {
        if (this._child != null) {
            this._rdd = _child.getRDD(context);
            this._rdd = this._rdd.map(new OLD_LetClauseMapClosure(_variableName, _expression));
            return _rdd;
        }
        throw new SparksoniqRuntimeException("Initial letClauses don't support RDDs");
    }

    @Override
    public Dataset<Row> getDataFrame(DynamicContext context) {
        //if it's not a start clause
        if (this._child != null) {
            Dataset<Row> df = _child.getDataFrame(context);

            StructType inputSchema = df.schema();
            
            int duplicateVariableIndex = Arrays.asList(inputSchema.fieldNames()).indexOf(_variableName);

            List<String> allColumns = DataFrameUtils.getColumnNames(inputSchema, duplicateVariableIndex, null);
            List<String> UDFcolumns = DataFrameUtils.getColumnNames(inputSchema, -1, _dependencies);

            df.sparkSession().udf().register("letClauseUDF",
                    new LetClauseUDF(_expression, inputSchema, UDFcolumns), DataTypes.createArrayType(DataTypes.BinaryType));

            String selectSQL = DataFrameUtils.getSQL(allColumns, true);
            String udfSQL = DataFrameUtils.getSQL(UDFcolumns, false);

            df.createOrReplaceTempView("input");
            df = df.sparkSession().sql(
                    String.format("select %s letClauseUDF(array(%s)) as `%s` from input",
                            selectSQL, udfSQL, _variableName)
            );
            return df;
        }
        throw new SparksoniqRuntimeException("Initial letClauses don't support DataFrames");
    }

    public Set<String> getVariableDependencies()
    {
        Set<String> result = new HashSet<String>();
        result.addAll(_expression.getVariableDependencies());
        if(_child != null)
        {
            result.removeAll(_child.getVariablesBoundInCurrentFLWORExpression());
            result.addAll(_child.getVariableDependencies());
        }
        return result;
    }

    public Set<String> getVariablesBoundInCurrentFLWORExpression()
    {
        Set<String> result = new HashSet<String>();
        if(_child != null)
        {
            result.addAll(_child.getVariablesBoundInCurrentFLWORExpression());
        }
        result.add(_variableName);
        return result;
    }
    
    public void print(StringBuffer buffer, int indent)
    {
        super.print(buffer, indent);
        for (int i = 0; i < indent + 1; ++i)
        {
            buffer.append("  ");
        }
        buffer.append("Variable " + _variableName);
        buffer.append("\n");
        _expression.print(buffer, indent+1);
    }
}
