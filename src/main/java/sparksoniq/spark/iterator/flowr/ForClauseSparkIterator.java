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
import sparksoniq.spark.SparkSessionManager;
import sparksoniq.spark.closures.ForClauseClosure;
import sparksoniq.spark.closures.ForClauseLocalToRDDClosure;
import sparksoniq.spark.closures.ForClauseSerializeClosure;
import sparksoniq.spark.closures.InitialForClauseClosure;

import java.util.ArrayList;
import java.util.List;

public class ForClauseSparkIterator extends SparkRuntimeTupleIterator {

    private String _variableName;           // for efficient use in local iteration
    private RuntimeIterator _expression;
    private DynamicContext _tupleContext;   // re-use same DynamicContext object for efficiency
    private FlworTuple _nextLocalTupleResult;
    private FlworTuple _inputTuple;     // tuple received from child, used for tuple creation

    public ForClauseSparkIterator(RuntimeTupleIterator child, VariableReferenceIterator variableReference,
                                  RuntimeIterator assignmentExpression, IteratorMetadata iteratorMetadata) {
        super(child, iteratorMetadata);
        _variableName = variableReference.getVariableName();
        _expression = assignmentExpression;
    }

    @Override
    public boolean isRDD() {
        return (_expression.isRDD() || (_child != null && _child.isRDD()));
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
                    this._rdd = this._rdd.union(expressionRDD.map(new ForClauseLocalToRDDClosure(_variableName, _inputTuple)));
                }
                _child.close();
            }
        }
        return _rdd;
    }

    @Override
    public Dataset<Row> getDataFrame(DynamicContext context) {
        this._df = null;

        if (this._child == null) {
            // create initial RDD from expression
            JavaRDD<Item> initialRdd = _expression.getRDD(context);

            // TODO: define a schema
            String schemaString = _variableName;
            List<StructField> fields = new ArrayList<>();
            for (String fieldName : schemaString.split(" ")) {
                StructField field = DataTypes.createStructField(fieldName, DataTypes.BinaryType, true);
                fields.add(field);
            }
            StructType schema = DataTypes.createStructType(fields);

            // TODO: convert initial RDD to row RDD
            JavaRDD<Row> rowRDD = initialRdd.map(new ForClauseSerializeClosure());

            // TODO: apply the schema to row RDD
            this._df = SparkSessionManager.getInstance().getOrCreateSession().createDataFrame(rowRDD, schema);

        } else {        //if it's not a start clause
            if (_child.isDataFrame()) {
                this._df = this._child.getDataFrame(context);

                // TODO: deserialize the byte array dataframe

                // TODO: Update schema

                // TODO: perform dataframe transformation

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
                    this._rdd = this._rdd.union(expressionRDD.map(new ForClauseLocalToRDDClosure(_variableName, _inputTuple)));
                }
                _child.close();
            }
        }
        return _df;
    }
}
