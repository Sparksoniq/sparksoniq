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
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import sparksoniq.exceptions.IteratorFlowException;
import sparksoniq.jsoniq.item.Item;
import sparksoniq.jsoniq.runtime.iterator.RuntimeIterator;
import sparksoniq.jsoniq.runtime.iterator.primary.VariableReferenceIterator;
import sparksoniq.jsoniq.runtime.metadata.IteratorMetadata;
import sparksoniq.jsoniq.runtime.tupleiterator.RuntimeTupleIterator;
import sparksoniq.jsoniq.runtime.tupleiterator.SparkRuntimeTupleIterator;
import sparksoniq.jsoniq.tuple.FlworTuple;
import sparksoniq.semantics.DynamicContext;
import sparksoniq.spark.DataFrameUtils;
import sparksoniq.spark.SparkSessionManager;
import sparksoniq.spark.closures.ForClauseClosure;
import sparksoniq.spark.closures.ForClauseLocalToRowClosure;
import sparksoniq.spark.closures.ForClauseSerializeClosure;
import sparksoniq.spark.closures.InitialForClauseClosure;
import sparksoniq.spark.closures.OLD_ForClauseLocalToRDDClosure;
import sparksoniq.spark.udf.ForClauseUDF;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ForClauseSparkIterator extends SparkRuntimeTupleIterator {

    private String _variableName;           // for efficient use in local iteration
    private RuntimeIterator _expression;
    Set<String> _dependencies;
    private DynamicContext _tupleContext;   // re-use same DynamicContext object for efficiency
    private FlworTuple _nextLocalTupleResult;
    private FlworTuple _inputTuple;     // tuple received from child, used for tuple creation

    public ForClauseSparkIterator(RuntimeTupleIterator child, VariableReferenceIterator variableReference,
                                  RuntimeIterator assignmentExpression, IteratorMetadata iteratorMetadata) {
        super(child, iteratorMetadata);
        _variableName = variableReference.getVariableName();
        _expression = assignmentExpression;
        _dependencies = _expression.getVariableDependencies();
    }

    @Override
    public boolean isRDD() {
        return (_expression.isRDD() || (_child != null && _child.isRDD()));
    }

    @Override
    public boolean isDataFrame() {
        return (_expression.isRDD() || (_child != null && _child.isDataFrame()));
    }


    @Override
    public void open(DynamicContext context) {
        super.open(context);

        // isRDD checks omitted, as open is used for non-RDD(local) operations

        if (this._child != null) { //if it's not a start clause
            _child.open(_currentDynamicContext);
            _tupleContext = new DynamicContext(_currentDynamicContext);     // assign current context as parent

            setNextLocalTupleResult();

        } else {    //if it's a start clause, get results using only the _expression
            _expression.open(this._currentDynamicContext);
            setResultFromExpression();
        }
    }

    @Override
    public FlworTuple next() {
        if (_hasNext == true) {
            FlworTuple result = _nextLocalTupleResult;      // save the result to be returned
            // calculate and store the next result
            if (_child == null) {       // if it's the initial for clause, call the correct function
                setResultFromExpression();
            } else {
                setNextLocalTupleResult();
            }
            return result;
        }
        throw new IteratorFlowException("Invalid next() call in let flwor clause", getMetadata());
    }

    private void setNextLocalTupleResult() {
        if (_expression.isOpen()) {
            if (setResultFromExpression()) {
                return;
            }
        }

        while (_child.hasNext()) {
            _inputTuple = _child.next();
            _tupleContext.removeAllVariables();             // clear the previous variables
            _tupleContext.setBindingsFromTuple(_inputTuple);      // assign new variables from new tuple

            _expression.open(_tupleContext);
            if (setResultFromExpression()) {
                return;
            }
        }

        // execution reaches here when there are no more results
        _child.close();
    }

    /**
     * _expression has to be open prior to call.
     *
     * @return true if _nextLocalTupleResult is set and _hasNext is true, false otherwise
     */
    private boolean setResultFromExpression() {
        if (_expression.hasNext()) {     // if expression returns a value, set it as next
            List<Item> results = new ArrayList<>();
            results.add(_expression.next());
            FlworTuple newTuple;
            if (_child == null) {   // if initial for clause
                newTuple = new FlworTuple(_variableName, results);
            } else {
                newTuple = new FlworTuple(_inputTuple, _variableName, results);
            }
            _nextLocalTupleResult = newTuple;
            this._hasNext = true;
            return true;
        } else {
            _expression.close();
            this._hasNext = false;
            return false;
        }
    }

    @Override
    public void close() {
        this._isOpen = false;
        result = null;
        if (_child != null) {
            this._child.close();
        }
    }


    @Override
    public JavaRDD<FlworTuple> getRDD(DynamicContext context) {
        JavaRDD<Item> initialRdd = null;
        this._rdd = SparkSessionManager.getInstance().getJavaSparkContext().emptyRDD();

        if (this._child == null) {
            initialRdd = _expression.getRDD(context);
            this._rdd = initialRdd.map(new InitialForClauseClosure(_variableName));
        } else {        //if it's not a start clause

            if (_child.isRDD()) {
                this._rdd = this._child.getRDD(context);
                this._rdd = this._rdd.flatMap(new ForClauseClosure(_expression, _variableName));

            } else {    // if child is locally evaluated
                // _expression is definitely an RDD if execution flows here

                _child.open(_currentDynamicContext);
                _tupleContext = new DynamicContext(_currentDynamicContext);     // assign current context as parent
                while (_child.hasNext()) {
                    _inputTuple = _child.next();
                    _tupleContext.removeAllVariables();             // clear the previous variables
                    _tupleContext.setBindingsFromTuple(_inputTuple);      // assign new variables from new tuple

                    JavaRDD<Item> expressionRDD = _expression.getRDD(_tupleContext);
                    this._rdd = this._rdd.union(expressionRDD.map(new OLD_ForClauseLocalToRDDClosure(_variableName, _inputTuple)));
                }
                _child.close();
            }
        }
        return _rdd;
    }

    @Override
    public Dataset<Row> getDataFrame(DynamicContext context) {
        // if it's a starting clause
        if (this._child == null) {
            // create initial RDD from expression
            JavaRDD<Item> initialRdd = _expression.getRDD(context);

            // define a schema
            List<StructField> fields = new ArrayList<>();
            StructField field = DataTypes.createStructField(_variableName, DataTypes.createArrayType(DataTypes.BinaryType, false), true);
            fields.add(field);
            StructType schema = DataTypes.createStructType(fields);

            JavaRDD<Row> rowRDD = initialRdd.map(new ForClauseSerializeClosure());

            // apply the schema to row RDD
            return SparkSessionManager.getInstance().getOrCreateSession().createDataFrame(rowRDD, schema);
        }

        if (_child.isDataFrame()) {
            Dataset<Row> df = this._child.getDataFrame(context);

            StructType inputSchema = df.schema();

            int duplicateVariableIndex = Arrays.asList(inputSchema.fieldNames()).indexOf(_variableName);

            List<String> allColumns = DataFrameUtils.getColumnNames(inputSchema, duplicateVariableIndex, null);
            List<String> UDFcolumns = DataFrameUtils.getColumnNames(inputSchema, -1, _dependencies);

            df.sparkSession().udf().register("forClauseUDF",
                    new ForClauseUDF(_expression, inputSchema, UDFcolumns), DataTypes.createArrayType(DataTypes.BinaryType));

            String selectSQL = DataFrameUtils.getSQL(allColumns, true);
            String udfSQL = DataFrameUtils.getSQL(UDFcolumns, false);

            df.createOrReplaceTempView("input");
            df = df.sparkSession().sql(
                    String.format("select %s explode(forClauseUDF(array(%s))) as `%s` from input",
                            selectSQL, udfSQL, _variableName)
            );
            return df;
        }

        // if child is locally evaluated
        // _expression is definitely an RDD if execution flows here
        Dataset<Row> df = null;
        _child.open(_currentDynamicContext);
        _tupleContext = new DynamicContext(_currentDynamicContext);     // assign current context as parent
        while (_child.hasNext()) {
            _inputTuple = _child.next();
            _tupleContext.removeAllVariables();                         // clear the previous variables
            _tupleContext.setBindingsFromTuple(_inputTuple);            // assign new variables from new tuple

            JavaRDD<Item> expressionRDD = _expression.getRDD(_tupleContext);

            // TODO - Optimization: Iterate schema creation only once
            Set<String> oldColumnNames = _inputTuple.getKeys();
            List<String> newColumnNames = new ArrayList<>();
            oldColumnNames.forEach(fieldName -> newColumnNames.add(fieldName));
            newColumnNames.add(_variableName);
            List<StructField> fields = new ArrayList<>();
            for (String columnName : newColumnNames) {
                StructField field = DataTypes.createStructField(columnName, DataTypes.BinaryType, true);
                fields.add(field);
            }
            StructType schema = DataTypes.createStructType(fields);

            JavaRDD<Row> rowRDD = expressionRDD.map(new ForClauseLocalToRowClosure(_inputTuple));

            if (df == null) {
                df = SparkSessionManager.getInstance().getOrCreateSession().createDataFrame(rowRDD, schema);
            } else {
                df = df.union(SparkSessionManager.getInstance().getOrCreateSession().createDataFrame(rowRDD, schema));
            }
        }
        _child.close();
        return df;
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
