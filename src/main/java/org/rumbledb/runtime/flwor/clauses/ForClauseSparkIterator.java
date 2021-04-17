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

package org.rumbledb.runtime.flwor.clauses;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.rumbledb.api.Item;
import org.rumbledb.context.DynamicContext;
import org.rumbledb.context.DynamicContext.VariableDependency;
import org.rumbledb.context.Name;
import org.rumbledb.exceptions.ExceptionMetadata;
import org.rumbledb.exceptions.IteratorFlowException;
import org.rumbledb.exceptions.JobWithinAJobException;
import org.rumbledb.exceptions.UnsupportedFeatureException;
import org.rumbledb.expressions.ExecutionMode;
import org.rumbledb.expressions.flowr.FLWOR_CLAUSES;
import org.rumbledb.items.ItemFactory;
import org.rumbledb.runtime.CommaExpressionIterator;
import org.rumbledb.runtime.RuntimeIterator;
import org.rumbledb.runtime.RuntimeTupleIterator;
import org.rumbledb.runtime.flwor.FlworDataFrameUtils;
import org.rumbledb.runtime.flwor.NativeClauseContext;
import org.rumbledb.runtime.flwor.closures.ItemsToBinaryColumn;
import org.rumbledb.runtime.flwor.udfs.DataFrameContext;
import org.rumbledb.runtime.flwor.udfs.ForClauseUDF;
import org.rumbledb.runtime.flwor.udfs.WhereClauseUDF;
import org.rumbledb.runtime.logics.AndOperationIterator;
import org.rumbledb.runtime.misc.ComparisonIterator;
import org.rumbledb.runtime.navigation.PredicateIterator;
import org.rumbledb.runtime.primary.ArrayRuntimeIterator;

import sparksoniq.jsoniq.tuple.FlworTuple;
import sparksoniq.spark.DataFrameUtils;
import sparksoniq.spark.SparkSessionManager;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.TreeMap;


public class ForClauseSparkIterator extends RuntimeTupleIterator {


    private static final long serialVersionUID = 1L;

    // Properties
    private Name variableName; // for efficient use in local iteration
    private Name positionalVariableName; // for efficient use in local iteration
    private RuntimeIterator assignmentIterator;
    private boolean allowingEmpty;
    private DataFrameContext dataFrameContext;

    // Computation state
    private transient DynamicContext tupleContext; // re-use same DynamicContext object for efficiency
    private transient long position;
    private transient FlworTuple nextLocalTupleResult;
    private transient FlworTuple inputTuple; // tuple received from child, used for tuple creation
    private transient boolean isFirstItem;

    public ForClauseSparkIterator(
            RuntimeTupleIterator child,
            Name variableName,
            Name positionalVariableName,
            boolean allowingEmpty,
            RuntimeIterator assignmentIterator,
            ExecutionMode executionMode,
            ExceptionMetadata iteratorMetadata
    ) {
        super(child, executionMode, iteratorMetadata);
        this.variableName = variableName;
        this.positionalVariableName = positionalVariableName;
        this.assignmentIterator = assignmentIterator;
        this.allowingEmpty = allowingEmpty;
        this.assignmentIterator.getVariableDependencies();
        this.dataFrameContext = new DataFrameContext();
    }

    public Name getVariableName() {
        return this.variableName;
    }

    public Name getPositionalVariableName() {
        return this.positionalVariableName;
    }

    public RuntimeIterator getAssignmentIterator() {
        return this.assignmentIterator;
    }

    public boolean isAllowingEmpty() {
        return this.allowingEmpty;
    }

    @Override
    public void open(DynamicContext context) {
        super.open(context);

        if (this.child != null) { // if it's not a start clause
            this.child.open(this.currentDynamicContext);
            this.tupleContext = new DynamicContext(this.currentDynamicContext); // assign current context as parent
            this.position = 1;
            this.isFirstItem = true;
            setNextLocalTupleResult();
        } else { // if it's a start clause, get results using only the assignmentIterator
            this.assignmentIterator.open(this.currentDynamicContext);
            this.position = 1;
            this.isFirstItem = true;
            setResultFromExpression();
        }
    }

    @Override
    public void reset(DynamicContext context) {
        super.reset(context);

        if (this.child != null) { // if it's not a start clause
            this.child.reset(this.currentDynamicContext);
            this.tupleContext = new DynamicContext(this.currentDynamicContext); // assign current context as parent
            this.position = 1;
            this.isFirstItem = true;
            setNextLocalTupleResult();
        } else { // if it's a start clause, get results using only the assignmentIterator
            this.assignmentIterator.reset(this.currentDynamicContext);
            this.position = 1;
            this.isFirstItem = true;
            setResultFromExpression();
        }
    }

    @Override
    public FlworTuple next() {
        if (this.hasNext) {
            FlworTuple result = this.nextLocalTupleResult; // save the result to be returned
            // calculate and store the next result
            if (this.child == null) { // if it's the initial for clause, call the correct function
                setResultFromExpression();
            } else {
                setNextLocalTupleResult();
            }
            return result;
        }
        throw new IteratorFlowException("Invalid next() call in let flwor clause", getMetadata());
    }

    private void setNextLocalTupleResult() {
        if (this.assignmentIterator.isOpen()) {
            if (setResultFromExpression()) {
                return;
            }
        }

        while (this.child.hasNext()) {
            this.inputTuple = this.child.next();
            this.tupleContext.getVariableValues().removeAllVariables(); // clear the previous variables
            this.tupleContext.getVariableValues().setBindingsFromTuple(this.inputTuple, getMetadata());
            this.assignmentIterator.open(this.tupleContext);
            this.position = 1;
            this.isFirstItem = true;
            if (setResultFromExpression()) {
                return;
            }
        }

        // execution reaches here when there are no more results
        this.hasNext = false;
    }

    /**
     * assignmentIterator has to be open prior to call.
     *
     * @return true if nextLocalTupleResult is set and hasNext is true, false otherwise
     */
    private boolean setResultFromExpression() {
        if (this.assignmentIterator.hasNext()) { // if expression returns a value, set it as next

            // Set the for item
            if (this.child == null) { // if initial for clause
                this.nextLocalTupleResult = new FlworTuple();
            } else {
                this.nextLocalTupleResult = new FlworTuple(this.inputTuple);
            }
            this.nextLocalTupleResult.putValue(this.variableName, this.assignmentIterator.next());

            // Set the position item (if any)
            if (this.positionalVariableName != null) {
                this.nextLocalTupleResult.putValue(
                    this.positionalVariableName,
                    ItemFactory.getInstance().createLongItem(this.position)
                );
                ++this.position;
            }

            this.hasNext = true;
            this.isFirstItem = false;
            return true;
        }

        // If an item was already output by this expression and there is no more, we are done.
        if (!this.isFirstItem || !this.allowingEmpty) {
            this.assignmentIterator.close();
            this.hasNext = false;
            return false;
        }

        // If nothing was output yet by this expression but we allow empty, we need to bind
        // the empty sequence.
        if (this.child == null) { // if initial for clause
            this.nextLocalTupleResult = new FlworTuple();
        } else {
            this.nextLocalTupleResult = new FlworTuple(this.inputTuple);
        }
        this.nextLocalTupleResult.putValue(this.variableName, Collections.emptyList());
        // Set the position item (if any)
        if (this.positionalVariableName != null) {
            this.nextLocalTupleResult.putValue(
                this.positionalVariableName,
                ItemFactory.getInstance().createLongItem(0)
            );
        }
        this.hasNext = true;
        this.isFirstItem = false;
        return true;
    }

    @Override
    public void close() {
        this.isOpen = false;
        if (this.child != null) {
            this.child.close();
        }
        if (this.assignmentIterator.isOpen()) {
            this.assignmentIterator.close();
        }
    }

    @Override
    public Dataset<Row> getDataFrame(
            DynamicContext context,
            Map<Name, DynamicContext.VariableDependency> parentProjection
    ) {
        // if it's a starting clause
        if (this.child == null) {
            return getDataFrameStartingClause(context, parentProjection);
        }

        if (this.child.isDataFrame()) {
            if (this.assignmentIterator.isRDDOrDataFrame()) {
                return getDataFrameFromCartesianProduct(context, parentProjection);
            }

            return getDataFrameInParallel(context, parentProjection);
        }

        // if child is locally evaluated
        // assignmentIterator is definitely an RDD if execution flows here
        return getDataFrameFromUnion(context, parentProjection);
    }

    /**
     * 
     * Non-starting clause, the child clause (above in the syntax) is parallelizable, the expression as well, and the
     * expression does not depend on the input tuple.
     * 
     * @param context the dynamic context.
     * @param outputTupleVariableDependencies the desired project.
     * @return the resulting DataFrame.
     */
    private Dataset<Row> getDataFrameFromCartesianProduct(
            DynamicContext context,
            Map<Name, DynamicContext.VariableDependency> outputTupleVariableDependencies
    ) {
        // If the expression depends on this input tuple, we might still recognize an join.
        if (!LetClauseSparkIterator.isExpressionIndependentFromInputTuple(this.assignmentIterator, this.child)) {
            return getDataFrameFromJoin(context, outputTupleVariableDependencies);
        }

        // Since no variable dependency to the current FLWOR expression exists for the expression
        // evaluate the DataFrame with the parent context and calculate the cartesian product
        Dataset<Row> expressionDF;
        Map<Name, DynamicContext.VariableDependency> startingClauseDependencies = new HashMap<>();
        if (outputTupleVariableDependencies.containsKey(this.variableName)) {
            startingClauseDependencies.put(this.variableName, outputTupleVariableDependencies.get(this.variableName));
        }
        if (
            this.positionalVariableName != null
                && outputTupleVariableDependencies.containsKey(this.positionalVariableName)
        ) {
            startingClauseDependencies.put(
                this.positionalVariableName,
                outputTupleVariableDependencies.get(this.positionalVariableName)
            );
        }
        expressionDF = getDataFrameStartingClause(context, startingClauseDependencies);

        Dataset<Row> inputDF = this.child.getDataFrame(context, getProjection(outputTupleVariableDependencies));

        // Now we prepare the two views that we want to compute the Cartesian product of.
        String inputDFTableName = "input";
        String expressionDFTableName = "expression";
        inputDF.createOrReplaceTempView(inputDFTableName);
        expressionDF.createOrReplaceTempView(expressionDFTableName);

        // We gather the columns to select from the previous clause.
        // We need to project away the clause's variables from the previous clause.
        StructType inputSchema = inputDF.schema();
        List<Name> overridenVariables = new ArrayList<>();
        overridenVariables.add(this.variableName);
        if (this.positionalVariableName != null) {
            overridenVariables.add(this.positionalVariableName);
        }
        List<String> columnsToSelect = FlworDataFrameUtils.getColumnNames(
            inputSchema,
            outputTupleVariableDependencies,
            null,
            overridenVariables
        );

        // For the new variables, we need to disambiguate.
        columnsToSelect.add(expressionDFTableName + "`.`" + this.variableName.toString());
        if (this.positionalVariableName != null) {
            columnsToSelect.add(expressionDFTableName + "`.`" + this.positionalVariableName);
        }
        String projectionVariables = FlworDataFrameUtils.getSQLProjection(columnsToSelect, false);

        // And return the Cartesian product with the desired projection.
        return inputDF.sparkSession()
            .sql(
                String.format(
                    "select %s from %s, %s",
                    projectionVariables,
                    inputDFTableName,
                    expressionDFTableName
                )
            );
    }

    /**
     * 
     * Non-starting clause, the child clause (above in the syntax) is parallelizable, the expression as well, and the
     * expression is a predicate whose lhs does not depend on the input tuple.
     * 
     * @param context the dynamic context.
     * @param parentProjection the desired project.
     * @return the resulting DataFrame.
     */
    private Dataset<Row> getDataFrameFromJoin(
            DynamicContext context,
            Map<Name, DynamicContext.VariableDependency> parentProjection
    ) {
        if (!(this.assignmentIterator instanceof PredicateIterator)) {
            throw new JobWithinAJobException(
                    "A for clause expression cannot produce a big sequence of items for a big number of tuples, as this would lead to a data flow explosion. A piece of advice: if you use a predicate expression in your for clause, like for $"
                        + this.variableName.toString()
                        + " in json-file(\"...\")[$$.id eq $other-flwor-variable.id], Rumble may be able to detect a join.",
                    getMetadata()
            );
        }
        RuntimeIterator sequenceIterator = ((PredicateIterator) this.assignmentIterator).sequenceIterator();
        RuntimeIterator predicateIterator = ((PredicateIterator) this.assignmentIterator).predicateIterator();

        // If the left hand side depends on the input tuple, we do not how to handle it.
        if (!LetClauseSparkIterator.isExpressionIndependentFromInputTuple(sequenceIterator, this.child)) {
            throw new JobWithinAJobException(
                    "A for clause expression cannot produce a big sequence of items for a big number of tuples, as this would lead to a data flow explosion. In our efforts to detect a join, we did recognize a predicate expression in the for clause, but the left-hand-side of the predicate expression depends on the previous variables of this FLWOR expression. You can fix this by making sure it does not.",
                    getMetadata()
            );
        }

        return joinInputTupleWithSequenceOnPredicate(
            context,
            this.child.getDataFrame(context, getProjection(parentProjection)),
            parentProjection,
            (this.child == null)
                ? Collections.emptyList()
                : new ArrayList<Name>(this.child.getOutputTupleVariableNames()),
            sequenceIterator,
            predicateIterator,
            this.allowingEmpty,
            this.variableName,
            this.positionalVariableName,
            Name.CONTEXT_ITEM,
            getMetadata()
        );
    }

    public static Dataset<Row> joinInputTupleWithSequenceOnPredicate(
            DynamicContext context,
            Dataset<Row> inputTuples,
            Map<Name, DynamicContext.VariableDependency> outputTupleVariableDependencies,
            List<Name> variablesInInputTuple,
            RuntimeIterator sequenceIterator,
            RuntimeIterator predicateIterator,
            boolean allowingEmpty,
            Name forVariableName,
            Name positionalVariableName,
            Name sequenceVariableName,
            ExceptionMetadata metadata
    ) {
        String inputDFTableName = "inputTuples";
        String expressionDFTableName = "sequenceExpression";

        // Is this a join that we can optimize as an actual Spark join?
        List<RuntimeIterator> expressionSideEqualityCriteria = new ArrayList<>();
        List<RuntimeIterator> inputTupleSideEqualityCriteria = new ArrayList<>();
        boolean optimizableJoin = extractEqualityComparisonsForHashing(
            predicateIterator,
            expressionSideEqualityCriteria,
            inputTupleSideEqualityCriteria,
            sequenceVariableName
        );

        if (allowingEmpty) {
            optimizableJoin = false;
        }

        // Next we prepare the data frame on the expression side.
        Dataset<Row> expressionDF;

        Map<Name, VariableDependency> predicateDependencies = predicateIterator.getVariableDependencies();
        if (
            sequenceVariableName.equals(Name.CONTEXT_ITEM)
                && outputTupleVariableDependencies.containsKey(forVariableName)
        ) {
            predicateDependencies.put(Name.CONTEXT_ITEM, outputTupleVariableDependencies.get(forVariableName));
        }

        List<Name> variablesInExpressionSideTuple = new ArrayList<>();
        if (
            sequenceVariableName.equals(Name.CONTEXT_ITEM) && predicateDependencies.containsKey(Name.CONTEXT_POSITION)
        ) {
            Map<Name, DynamicContext.VariableDependency> startingClauseDependencies = new HashMap<>();
            startingClauseDependencies.put(sequenceVariableName, DynamicContext.VariableDependency.FULL);
            startingClauseDependencies.put(Name.CONTEXT_POSITION, DynamicContext.VariableDependency.FULL);

            optimizableJoin = false;
            expressionDF = getDataFrameStartingClause(
                sequenceIterator,
                sequenceVariableName,
                Name.CONTEXT_POSITION,
                false,
                context,
                startingClauseDependencies
            );
            variablesInExpressionSideTuple.add(sequenceVariableName);
            variablesInExpressionSideTuple.add(Name.CONTEXT_POSITION);
        } else {
            Map<Name, DynamicContext.VariableDependency> startingClauseDependencies = new HashMap<>();
            startingClauseDependencies.put(sequenceVariableName, DynamicContext.VariableDependency.FULL);
            expressionDF = getDataFrameStartingClause(
                sequenceIterator,
                sequenceVariableName,
                null,
                false,
                context,
                startingClauseDependencies
            );
            variablesInExpressionSideTuple.add(sequenceVariableName);
        }

        // If the join criterion uses the context count, then we need to add it to the expression side (it is a
        // constant).
        if (sequenceVariableName.equals(Name.CONTEXT_ITEM) && predicateDependencies.containsKey(Name.CONTEXT_COUNT)) {
            long size = expressionDF.count();
            expressionDF.createOrReplaceTempView(expressionDFTableName);
            expressionDF = expressionDF.sparkSession()
                .sql(
                    String.format(
                        "SELECT *, %s AS `%s` FROM %s",
                        Long.toString(size),
                        Name.CONTEXT_COUNT.getLocalName(),
                        expressionDFTableName
                    )
                );
            variablesInExpressionSideTuple.add(Name.CONTEXT_COUNT);
        }

        if (optimizableJoin) {
            System.err.println(
                "[INFO] Rumble detected that it can optimize your query and make it faster with an equi-join."
            );
        }



        // Now we prepare the iterators for the two sides of the equality criterion.
        RuntimeIterator expressionSideEqualityCriterion;
        RuntimeIterator inputTupleSideEqualityCriterion;

        if (expressionSideEqualityCriteria.size() == 1) {
            expressionSideEqualityCriterion = expressionSideEqualityCriteria.get(0);
        } else {
            expressionSideEqualityCriterion = new ArrayRuntimeIterator(
                    new CommaExpressionIterator(
                            expressionSideEqualityCriteria,
                            ExecutionMode.LOCAL,
                            metadata
                    ),
                    ExecutionMode.LOCAL,
                    metadata
            );
        }
        if (inputTupleSideEqualityCriteria.size() == 1) {
            inputTupleSideEqualityCriterion = inputTupleSideEqualityCriteria.get(0);
        } else {
            inputTupleSideEqualityCriterion = new ArrayRuntimeIterator(
                    new CommaExpressionIterator(
                            inputTupleSideEqualityCriteria,
                            ExecutionMode.LOCAL,
                            metadata
                    ),
                    ExecutionMode.LOCAL,
                    metadata
            );
        }

        // And we extend the expression and input tuple views with the hashes.
        if (optimizableJoin) {
            expressionDF = LetClauseSparkIterator.bindLetVariableInDataFrame(
                expressionDF,
                Name.createVariableInNoNamespace(SparkSessionManager.expressionHashColumnName),
                null,
                expressionSideEqualityCriterion,
                context,
                variablesInExpressionSideTuple,
                null,
                true
            );
            inputTuples = LetClauseSparkIterator.bindLetVariableInDataFrame(
                inputTuples,
                Name.createVariableInNoNamespace(SparkSessionManager.inputTupleHashColumnName),
                null,
                inputTupleSideEqualityCriterion,
                context,
                variablesInInputTuple,
                null,
                true
            );
        }



        // Now we prepare the two views that we want to compute the Cartesian product of.
        inputTuples.createOrReplaceTempView(inputDFTableName);
        expressionDF.createOrReplaceTempView(expressionDFTableName);

        // We gather the columns to select from the previous clause.
        // We need to project away the clause's variables from the previous clause.
        StructType inputSchema = inputTuples.schema();
        List<Name> variableNamesToExclude = new ArrayList<>();
        variableNamesToExclude.add(forVariableName);
        if (positionalVariableName != null) {
            variableNamesToExclude.add(positionalVariableName);
        }
        List<String> columnsToSelect = FlworDataFrameUtils.getColumnNames(
            inputSchema,
            outputTupleVariableDependencies,
            null,
            variableNamesToExclude
        );

        // We don't support positional variables yet for large joins.
        if (positionalVariableName != null) {
            throw new UnsupportedFeatureException(
                    "Rumble detected a large-scale join, but we do not support positional variables yet for these joins.",
                    metadata
            );
        }
        String projectionVariables = FlworDataFrameUtils.getSQLProjection(columnsToSelect, true);

        // We need to prepare the parameters fed into the predicate.
        List<Name> variablesInJointTuple = new ArrayList<>();
        variablesInJointTuple.addAll(variablesInInputTuple);
        List<StructField> fieldList = new ArrayList<StructField>();
        for (StructField f : inputSchema.fields()) {
            fieldList.add(f);
        }
        if (predicateDependencies.containsKey(sequenceVariableName)) {
            variablesInJointTuple.add(sequenceVariableName);
            fieldList.add(
                new StructField(sequenceVariableName.getLocalName(), DataTypes.BinaryType, true, Metadata.empty())
            );
        }
        if (
            sequenceVariableName.equals(Name.CONTEXT_ITEM) && predicateDependencies.containsKey(Name.CONTEXT_POSITION)
        ) {
            variablesInJointTuple.add(Name.CONTEXT_POSITION);
            fieldList.add(
                new StructField(Name.CONTEXT_POSITION.getLocalName(), DataTypes.BinaryType, true, Metadata.empty())
            );
        }
        if (sequenceVariableName.equals(Name.CONTEXT_ITEM) && predicateDependencies.containsKey(Name.CONTEXT_COUNT)) {
            variablesInJointTuple.add(Name.CONTEXT_COUNT);
            fieldList.add(
                new StructField(Name.CONTEXT_COUNT.getLocalName(), DataTypes.BinaryType, true, Metadata.empty())
            );
        }

        StructField[] fields = new StructField[fieldList.size()];
        fieldList.toArray(fields);
        StructType jointSchema = new StructType(fields);

        List<String> joinCriterionUDFcolumns = FlworDataFrameUtils.getColumnNames(
            jointSchema,
            predicateDependencies,
            variablesInJointTuple,
            null
        );

        // Now we need to register or join predicate as a UDF.
        inputTuples.sparkSession()
            .udf()
            .register(
                "joinUDF",
                new WhereClauseUDF(predicateIterator, context, jointSchema, joinCriterionUDFcolumns),
                DataTypes.BooleanType
            );

        String UDFParameters = FlworDataFrameUtils.getUDFParameters(joinCriterionUDFcolumns);

        // If we allow empty, we need a LEFT OUTER JOIN.
        if (allowingEmpty) {
            Dataset<Row> resultDF = inputTuples.sparkSession()
                .sql(
                    String.format(
                        "SELECT %s `%s`.`%s` AS `%s` FROM %s LEFT OUTER JOIN %s ON joinUDF(%s) = 'true'",
                        projectionVariables,
                        expressionDFTableName,
                        sequenceVariableName.getLocalName(),
                        forVariableName,
                        inputDFTableName,
                        expressionDFTableName,
                        UDFParameters
                    )
                );
            return resultDF;
        }

        if (optimizableJoin) {
            // Otherwise, it's a regular join.
            Dataset<Row> resultDF = inputTuples.sparkSession()
                .sql(
                    String.format(
                        "SELECT %s `%s`.`%s` AS `%s` FROM %s JOIN %s ON `%s` = `%s` WHERE joinUDF(%s) = 'true'",
                        projectionVariables,
                        expressionDFTableName,
                        sequenceVariableName.getLocalName(),
                        forVariableName,
                        inputDFTableName,
                        expressionDFTableName,
                        SparkSessionManager.expressionHashColumnName,
                        SparkSessionManager.inputTupleHashColumnName,
                        UDFParameters
                    )
                );
            return resultDF;
        }
        // Otherwise, it's a regular join.
        Dataset<Row> resultDF = inputTuples.sparkSession()
            .sql(
                String.format(
                    "SELECT %s `%s`.`%s` AS `%s` FROM %s JOIN %s ON joinUDF(%s) = 'true'",
                    projectionVariables,
                    expressionDFTableName,
                    sequenceVariableName.getLocalName(),
                    forVariableName,
                    inputDFTableName,
                    expressionDFTableName,
                    UDFParameters
                )
            );
        return resultDF;
    }

    private static boolean extractEqualityComparisonsForHashing(
            RuntimeIterator predicateIterator,
            List<RuntimeIterator> expressionSideEqualityCriteria,
            List<RuntimeIterator> inputTupleSideEqualityCriteria,
            Name expressionVariableName
    ) {
        boolean optimizableJoin = false;
        Stack<RuntimeIterator> candidateIterators = new Stack<>();
        candidateIterators.push(predicateIterator);
        while (!candidateIterators.isEmpty()) {
            RuntimeIterator iterator = candidateIterators.pop();
            if (iterator instanceof AndOperationIterator) {
                AndOperationIterator andIterator = ((AndOperationIterator) iterator);
                candidateIterators.push(andIterator.getLeftIterator());
                candidateIterators.push(andIterator.getRightIterator());
            } else if (iterator instanceof ComparisonIterator) {
                ComparisonIterator comparisonIterator = (ComparisonIterator) iterator;
                if (comparisonIterator.isValueEquality()) {
                    RuntimeIterator lhs = comparisonIterator.getLeftIterator();
                    RuntimeIterator rhs = comparisonIterator.getRightIterator();

                    Set<Name> leftDependencies = new HashSet<>(
                            lhs.getVariableDependencies().keySet()
                    );
                    Set<Name> rightDependencies = new HashSet<>(
                            rhs.getVariableDependencies().keySet()
                    );
                    if (leftDependencies.size() == 1 && leftDependencies.contains(expressionVariableName)) {
                        if (!rightDependencies.contains(expressionVariableName)) {
                            optimizableJoin = true;
                            expressionSideEqualityCriteria.add(lhs);
                            inputTupleSideEqualityCriteria.add(rhs);
                        }
                    }
                    if (rightDependencies.size() == 1 && rightDependencies.contains(expressionVariableName)) {
                        if (!leftDependencies.contains(expressionVariableName)) {
                            optimizableJoin = true;
                            expressionSideEqualityCriteria.add(rhs);
                            inputTupleSideEqualityCriteria.add(lhs);
                        }
                    }
                }
            }
        }
        return optimizableJoin;
    }

    /**
     * 
     * Non-starting clause, the child clause (above in the syntax) is local but the expression is parallelizable.
     * 
     * @param context the dynamic context.
     * @param parentProjection the desired project.
     * @return the resulting DataFrame.
     */
    private Dataset<Row> getDataFrameFromUnion(
            DynamicContext context,
            Map<Name, DynamicContext.VariableDependency> parentProjection
    ) {
        Dataset<Row> df = null;
        this.child.open(context);
        StructType schema = null;
        while (this.child.hasNext()) {
            // We first compute the new tuple variable values
            this.inputTuple = this.child.next();
            this.tupleContext = new DynamicContext(context);
            // IMPORTANT: this must be a new context object every time
            // because of lazy evaluation.
            this.tupleContext.getVariableValues().setBindingsFromTuple(this.inputTuple, getMetadata()); // assign new
                                                                                                        // variables
                                                                                                        // from new

            Map<Name, DynamicContext.VariableDependency> startingClauseDependencies = new HashMap<>();
            if (parentProjection.containsKey(this.variableName)) {
                startingClauseDependencies.put(this.variableName, parentProjection.get(this.variableName));
            }
            if (
                this.positionalVariableName != null
                    && parentProjection.containsKey(this.positionalVariableName)
            ) {
                startingClauseDependencies.put(
                    this.positionalVariableName,
                    parentProjection.get(this.positionalVariableName)
                );
            }
            Dataset<Row> lateralView = getDataFrameStartingClause(this.tupleContext, startingClauseDependencies);
            lateralView.createOrReplaceTempView("lateralView");

            // We then get the (singleton) input tuple as a data frame

            List<byte[]> serializedRowColumns = new ArrayList<>();
            for (Name columnName : this.inputTuple.getLocalKeys()) {
                serializedRowColumns.add(
                    FlworDataFrameUtils.serializeItemList(
                        this.inputTuple.getLocalValue(columnName, getMetadata()),
                        this.dataFrameContext.getKryo(),
                        this.dataFrameContext.getOutput()
                    )
                );
            }

            Row row = RowFactory.create(serializedRowColumns.toArray());

            JavaRDD<Row> inputTupleRDD = JavaSparkContext.fromSparkContext(
                lateralView.sparkSession()
                    .sparkContext()
            ).parallelize(Collections.singletonList(row), 1);
            if (schema == null) {
                schema = generateSchema();
            }
            Dataset<Row> inputTupleDataFrame = SparkSessionManager.getInstance()
                .getOrCreateSession()
                .createDataFrame(inputTupleRDD, schema);
            inputTupleDataFrame.createOrReplaceTempView("inputTuple");

            // And we join.
            inputTupleDataFrame = inputTupleDataFrame.sparkSession()
                .sql("select * FROM inputTuple JOIN lateralView");

            if (df == null) {
                df = inputTupleDataFrame;
            } else {
                df = df.union(inputTupleDataFrame);
            }
        }
        this.child.close();
        return df;
    }

    /**
     * 
     * Non-starting clause and the child clause (above in the syntax) is parallelizable.
     * 
     * @param context the dynamic context.
     * @param outputTuplesVariableDependencies the desired project.
     * @return the resulting DataFrame.
     */
    private Dataset<Row> getDataFrameInParallel(
            DynamicContext context,
            Map<Name, DynamicContext.VariableDependency> outputTuplesVariableDependencies
    ) {

        // the expression is locally evaluated
        Dataset<Row> df = this.child.getDataFrame(context, getProjection(outputTuplesVariableDependencies));
        StructType inputSchema = df.schema();
        List<Name> variableNamesToExclude = new ArrayList<>();
        variableNamesToExclude.add(this.variableName);
        if (this.positionalVariableName != null) {
            variableNamesToExclude.add(this.positionalVariableName);
        }
        List<String> allColumns = FlworDataFrameUtils.getColumnNames(
            inputSchema,
            outputTuplesVariableDependencies,
            null,
            variableNamesToExclude
        );

        Dataset<Row> nativeQueryResult = tryNativeQuery(
            df,
            this.variableName,
            this.positionalVariableName,
            this.allowingEmpty,
            this.assignmentIterator,
            allColumns,
            inputSchema,
            context
        );
        if (nativeQueryResult != null) {
            return nativeQueryResult;
        }
        System.out.println("using UDF");

        List<String> UDFcolumns;
        if (this.child != null) {
            UDFcolumns = FlworDataFrameUtils.getColumnNames(
                inputSchema,
                this.assignmentIterator.getVariableDependencies(),
                new ArrayList<Name>(this.child.getOutputTupleVariableNames()),
                null
            );
        } else {
            UDFcolumns = Collections.emptyList();
        }

        df.sparkSession()
            .udf()
            .register(
                "forClauseUDF",
                new ForClauseUDF(this.assignmentIterator, context, inputSchema, UDFcolumns),
                DataTypes.createArrayType(DataTypes.BinaryType)
            );

        String projectionVariables = FlworDataFrameUtils.getSQLProjection(allColumns, true);
        String UDFParameters = FlworDataFrameUtils.getUDFParameters(UDFcolumns);

        df.createOrReplaceTempView("input");
        if (this.positionalVariableName == null) {
            if (this.allowingEmpty) {
                df = df.sparkSession()
                    .sql(
                        String.format(
                            "select %s explode_outer(forClauseUDF(%s)) as `%s` from input",
                            projectionVariables,
                            UDFParameters,
                            this.variableName
                        )
                    );
            } else {
                df = df.sparkSession()
                    .sql(
                        String.format(
                            "select %s explode(forClauseUDF(%s)) as `%s` from input",
                            projectionVariables,
                            UDFParameters,
                            this.variableName
                        )
                    );
            }
        } else {
            if (this.allowingEmpty) {
                df = df.sparkSession()
                    .sql(
                        String.format(
                            "SELECT %s for_vars.`%s`, IF(for_vars.`%s` IS NULL, 0, for_vars.`%s` + 1) AS `%s` "
                                + "FROM input "
                                + "LATERAL VIEW OUTER posexplode(forClauseUDF(%s)) for_vars AS `%s`, `%s` ",
                            projectionVariables,
                            this.variableName,
                            this.positionalVariableName,
                            this.positionalVariableName,
                            this.positionalVariableName,
                            UDFParameters,
                            this.positionalVariableName,
                            this.variableName
                        )
                    );
            } else {
                df = df.sparkSession()
                    .sql(
                        String.format(
                            "SELECT %s for_vars.`%s`, for_vars.`%s` + 1 AS `%s` "
                                + "FROM input "
                                + "LATERAL VIEW posexplode(forClauseUDF(%s)) for_vars AS `%s`, `%s` ",
                            projectionVariables,
                            this.variableName,
                            this.positionalVariableName,
                            this.positionalVariableName,
                            UDFParameters,
                            this.positionalVariableName,
                            this.variableName
                        )
                    );
            }
        }
        return df;
    }

    private StructType generateSchema() {
        List<StructField> fields = new ArrayList<>();
        for (Name columnName : this.inputTuple.getLocalKeys()) {
            // all columns store items serialized to binary format
            StructField field = DataTypes.createStructField(columnName.toString(), DataTypes.BinaryType, true);
            fields.add(field);
        }
        return DataTypes.createStructType(fields);
    }

    /**
     * 
     * Starting clause and the expression is parallelizable.
     * 
     * @param context the dynamic context.
     * @param parentProjection the desired project.
     * @return the resulting DataFrame.
     */
    private Dataset<Row> getDataFrameStartingClause(
            DynamicContext context,
            Map<Name, DynamicContext.VariableDependency> parentProjection
    ) {
        return getDataFrameStartingClause(
            this.assignmentIterator,
            this.variableName,
            this.positionalVariableName,
            this.allowingEmpty,
            context,
            parentProjection
        );
    }

    /**
     * 
     * Starting clause and the expression is parallelizable.
     * 
     * @param iterator the expression iterator
     * @param variableName the name of the for variable
     * @param positionalVariableName the name of the positional variable (or null if none)
     * @param allowingEmpty whether the allowing empty option is present
     * @param context the dynamic context.
     * @param outputDependencies the desired project.
     * @return the resulting DataFrame.
     */
    public static Dataset<Row> getDataFrameStartingClause(
            RuntimeIterator iterator,
            Name variableName,
            Name positionalVariableName,
            boolean allowingEmpty,
            DynamicContext context,
            Map<Name, DynamicContext.VariableDependency> outputDependencies
    ) {
        Dataset<Row> df = null;;
        if (iterator.isDataFrame()) {
            Dataset<Row> rows = iterator.getDataFrame(context);

            rows.createOrReplaceTempView("assignment");
            if (DataFrameUtils.isSequenceOfObjects(rows)) {
                String[] fields = rows.schema().fieldNames();
                String columnNames = FlworDataFrameUtils.getSQLProjection(Arrays.asList(fields), false);
                df = rows.sparkSession()
                    .sql(
                        String.format(
                            "SELECT struct(%s) AS `%s` FROM assignment",
                            columnNames,
                            variableName
                        )
                    );
            } else {
                df = rows.sparkSession()
                    .sql(
                        String.format(
                            "SELECT `%s` AS `%s` FROM assignment",
                            SparkSessionManager.atomicJSONiqItemColumnName,
                            variableName
                        )
                    );
            }
        } else {
            // create initial RDD from expression
            JavaRDD<Item> expressionRDD = iterator.getRDD(context);
            df = getDataFrameFromItemRDD(variableName, expressionRDD);
        }
        if (positionalVariableName == null && !allowingEmpty) {
            return df;
        }
        if (positionalVariableName == null && allowingEmpty) {
            df.createOrReplaceTempView("input");
            df = df.sparkSession()
                .sql(
                    String.format(
                        "SELECT input.`%s` FROM VALUES(1) FULL OUTER JOIN input",
                        variableName
                    )
                );
            return df;
        }
        // Add column for positional variable, similar to count clause.
        Dataset<Row> dfWithIndex = CountClauseSparkIterator.addSerializedCountColumn(
            df,
            outputDependencies,
            positionalVariableName
        );
        if (!allowingEmpty) {
            return dfWithIndex;
        }
        dfWithIndex.createOrReplaceTempView("inputWithIndex");

        dfWithIndex = dfWithIndex.sparkSession()
            .sql(
                String.format(
                    "SELECT inputWithIndex.`%s`, IF(inputWithIndex.`%s` IS NULL, 0, inputWithIndex.`%s`) AS `%s` FROM VALUES(1) FULL OUTER JOIN inputWithIndex",
                    variableName,
                    positionalVariableName,
                    positionalVariableName,
                    positionalVariableName
                )
            );
        return dfWithIndex;
    }

    private static Dataset<Row> getDataFrameFromItemRDD(Name variableName, JavaRDD<Item> expressionRDD) {
        // define a schema
        List<StructField> fields = Collections.singletonList(
            DataTypes.createStructField(variableName.toString(), DataTypes.BinaryType, true)
        );
        StructType schema = DataTypes.createStructType(fields);

        JavaRDD<Row> rowRDD = expressionRDD.map(new ItemsToBinaryColumn());

        // apply the schema to row RDD
        return SparkSessionManager.getInstance().getOrCreateSession().createDataFrame(rowRDD, schema);
    }

    @Override
    public Map<Name, DynamicContext.VariableDependency> getVariableDependencies() {
        Map<Name, DynamicContext.VariableDependency> result =
            new TreeMap<>(this.assignmentIterator.getVariableDependencies());
        if (this.child != null) {
            for (Name var : this.child.getOutputTupleVariableNames()) {
                result.remove(var);
            }
            result.putAll(this.child.getVariableDependencies());
        }
        return result;
    }

    @Override
    public Set<Name> getOutputTupleVariableNames() {
        Set<Name> result = new HashSet<>();
        if (this.child != null) {
            result.addAll(this.child.getOutputTupleVariableNames());
        }
        result.add(this.variableName);
        if (this.positionalVariableName != null) {
            result.add(this.positionalVariableName);
        }
        return result;
    }

    @Override
    public void print(StringBuffer buffer, int indent) {
        super.print(buffer, indent);
        for (int i = 0; i < indent + 1; ++i) {
            buffer.append("  ");
        }
        buffer.append("Variable ").append(this.variableName.toString()).append("\n");
        for (int i = 0; i < indent + 1; ++i) {
            buffer.append("  ");
        }
        if (this.positionalVariableName != null) {
            buffer.append("Positional variable ").append(this.positionalVariableName.toString()).append("\n");
        }
        this.assignmentIterator.print(buffer, indent + 1);
    }

    @Override
    public Map<Name, DynamicContext.VariableDependency> getProjection(
            Map<Name, DynamicContext.VariableDependency> parentProjection
    ) {
        if (this.child == null) {
            return null;
        }

        // start with an empty projection.

        // copy over the projection needed by the parent clause.
        Map<Name, DynamicContext.VariableDependency> projection =
            new TreeMap<>(parentProjection);

        // remove the variables that this for clause binds.
        projection.remove(this.variableName);
        if (this.positionalVariableName != null) {
            projection.remove(this.positionalVariableName);
        }

        // add the variable dependencies needed by this for clause's expression.
        Map<Name, DynamicContext.VariableDependency> exprDependency = this.assignmentIterator
            .getVariableDependencies();
        for (Name variable : exprDependency.keySet()) {
            if (projection.containsKey(variable)) {
                if (projection.get(variable) != exprDependency.get(variable)) {
                    // If the projection already needed a different kind of dependency, we fall back to the full
                    // sequence of items.
                    if (
                        this.child != null && this.child.getOutputTupleVariableNames().contains(variable)
                    ) {
                        projection.put(variable, DynamicContext.VariableDependency.FULL);
                    }
                }
            } else {
                if (this.child != null && this.child.getOutputTupleVariableNames().contains(variable)) {
                    projection.put(variable, exprDependency.get(variable));
                }
            }
        }
        return projection;
    }

    /**
     * Try to generate the native query for the for clause and run it, if successful return the resulting dataframe,
     * otherwise it returns null
     *
     * @param dataFrame input dataframe for the query
     * @param newVariableName name of the new bound variable
     * @param positionalVariableName name of the positional variable (or null if absent)
     * @param allowingEmpty boolean signaling allowing empty flag in expression
     * @param iterator for variable assignment expression iterator
     * @param allColumns other columns required in following clauses
     * @param inputSchema input schema of the dataframe
     * @param context current dynamic context of the dataframe
     * @return resulting dataframe of the for clause if successful, null otherwise
     */
    public static Dataset<Row> tryNativeQuery(
            Dataset<Row> dataFrame,
            Name newVariableName,
            Name positionalVariableName,
            boolean allowingEmpty,
            RuntimeIterator iterator,
            List<String> allColumns,
            StructType inputSchema,
            DynamicContext context
    ) {
        NativeClauseContext forContext = new NativeClauseContext(FLWOR_CLAUSES.FOR, inputSchema, context);
        NativeClauseContext nativeQuery = iterator.generateNativeQuery(forContext);
        if (nativeQuery == NativeClauseContext.NoNativeQuery) {
            return null;
        }
        System.out.println(
            "[INFO] Rumble was able to optimize a for clause to a native SQL query: " + nativeQuery.getResultingQuery()
        );
        System.out.println("[INFO] (the lateral view part is " + nativeQuery.getLateralViewPart() + ")");
        String selectSQL = FlworDataFrameUtils.getSQLProjection(allColumns, true);
        dataFrame.createOrReplaceTempView("input");

        // let's distinguish 4 cases
        if (positionalVariableName == null) {
            if (allowingEmpty) {
                List<String> lateralViewPart = nativeQuery.getLateralViewPart();
                if (lateralViewPart.size() == 0) {
                    // no array unboxing in the operation
                    // already covered in the generation and handled through UDF
                    // this branch should never happen in practice
                    return null;
                } else {
                    // we have at least an array unboxing operation
                    // col is the default name of explode

                    // to deal with allowing empty
                    // first we add an artificial unique id to the dataset and re-register the input table
                    String rowIdField = "idx-9384-3948-1272-4375";
                    dataFrame = dataFrame.sparkSession()
                        .sql("select *, monotonically_increasing_id() as `" + rowIdField + "` from input");
                    dataFrame.createOrReplaceTempView("input");

                    // then we create the virtual exploded table as before
                    // but this time we store it and also get the index field
                    StringBuilder lateralViewString = new StringBuilder();
                    int arrIndex = 0;
                    for (String lateralView : lateralViewPart) {
                        ++arrIndex;
                        lateralViewString.append(" lateral view ");
                        lateralViewString.append(lateralView);
                        lateralViewString.append(" arr");
                        lateralViewString.append(arrIndex);
                    }
                    Dataset<Row> lateralViewDf = dataFrame.sparkSession()
                        .sql(
                            String.format(
                                "select `%s`, arr%d.col%s as `%s` from input %s",
                                rowIdField,
                                arrIndex,
                                nativeQuery.getResultingQuery(),
                                newVariableName,
                                lateralViewString
                            )
                        );
                    lateralViewDf.createOrReplaceTempView("lateral");

                    // to return the correct number of empty results we perform a left join between input and
                    // lateral
                    return dataFrame.sparkSession()
                        .sql(
                            String.format(
                                "select %s lateral.`%s` from input left join lateral on input.`%s` = lateral.`%s`",
                                selectSQL,
                                newVariableName,
                                rowIdField,
                                rowIdField
                            )
                        );
                }
            } else {
                List<String> lateralViewPart = nativeQuery.getLateralViewPart();
                if (lateralViewPart.size() == 0) {
                    // no array unboxing in the operation
                    return dataFrame.sparkSession()
                        .sql(
                            String.format(
                                "select %s %s as `%s` from input",
                                selectSQL,
                                nativeQuery.getResultingQuery(),
                                newVariableName
                            )
                        );
                } else {
                    // we have at least an array unboxing operation
                    // col is the default name of explode
                    StringBuilder lateralViewString = new StringBuilder();
                    int arrIndex = 0;
                    for (String lateralView : lateralViewPart) {
                        ++arrIndex;
                        lateralViewString.append(" lateral view ");
                        lateralViewString.append(lateralView);
                        lateralViewString.append(" arr");
                        lateralViewString.append(arrIndex);
                    }
                    return dataFrame.sparkSession()
                        .sql(
                            String.format(
                                "select %s arr%d.col%s as `%s` from input %s",
                                selectSQL,
                                arrIndex,
                                nativeQuery.getResultingQuery(),
                                newVariableName,
                                lateralViewString
                            )
                        );
                }
            }
        } else {
            // common part for positional variable handling
            List<String> lateralViewPart = nativeQuery.getLateralViewPart();
            if (lateralViewPart.size() == 0) {
                // if allowing empty we do not deal with this
                if (allowingEmpty) {
                    return null;
                }
                // no array unboxing in the operation
                // therefore position is for sure 1
                return dataFrame.sparkSession()
                    .sql(
                        String.format(
                            "select %s %s as `%s`, 1 as `%s` from input",
                            selectSQL,
                            nativeQuery.getResultingQuery(),
                            newVariableName,
                            positionalVariableName
                        )
                    );
            } else {
                // we have at least an array unboxing operation
                // pos, col are the default name of posexplode function
                // to deal with positional variable
                // we first add unique index to guarantee grouping correctly
                String rowIdField = "idx-9384-3948-1272-4375";
                dataFrame = dataFrame.sparkSession()
                    .sql("select *, monotonically_increasing_id() as `" + rowIdField + "` from input");
                dataFrame.createOrReplaceTempView("input");

                // then we collect all values from lateral view
                // and group by original tuple
                // this is basically equivalent to flattening the array, in case of multiple unboxing operation
                StringBuilder lateralViewString = new StringBuilder();
                int arrIndex = 0;
                for (String lateralView : lateralViewPart) {
                    ++arrIndex;
                    lateralViewString.append(" lateral view ");
                    lateralViewString.append(lateralView);
                    lateralViewString.append(" arr");
                    lateralViewString.append(arrIndex);
                }
                dataFrame = dataFrame.sparkSession()
                    .sql(
                        String.format(
                            "select `%s`, %s collect_list(arr%d.col%s) as grouped from input %s group by %s `%s`",
                            rowIdField,
                            selectSQL,
                            arrIndex,
                            nativeQuery.getResultingQuery(),
                            lateralViewString,
                            selectSQL,
                            rowIdField
                        )
                    );


                if (allowingEmpty) {
                    // register a support table to keep the empty values
                    dataFrame.sparkSession()
                        .sql("select `" + rowIdField + "` from input")
                        .createOrReplaceTempView("allrows");

                    // register previously created table
                    dataFrame.createOrReplaceTempView("input");

                    // insert null values
                    dataFrame = dataFrame.sparkSession()
                        .sql(
                            String.format(
                                "select allrows.`%s`, %s grouped from allrows left join input on allrows.`%s` = input.`%s`",
                                rowIdField,
                                selectSQL,
                                rowIdField,
                                rowIdField
                            )
                        );
                    dataFrame.createOrReplaceTempView("input");

                    // we use a lateral view to handle proper counting and NULL handling
                    return dataFrame.sparkSession()
                        .sql(
                            String.format(
                                "select %s IF(exploded.pos IS NULL, 0, exploded.pos + 1) as `%s`, exploded.col as `%s`  from input lateral view outer posexplode(grouped) exploded",
                                selectSQL,
                                positionalVariableName,
                                newVariableName
                            )
                        );
                } else {
                    // register previously created table
                    dataFrame.createOrReplaceTempView("input");

                    // finally we unwrap it with a single posexplode
                    return dataFrame.sparkSession()
                        .sql(
                            String.format(
                                "select %s (exploded.pos + 1) as `%s`, exploded.col as `%s`  from input lateral view posexplode(grouped) exploded",
                                selectSQL,
                                positionalVariableName,
                                newVariableName
                            )
                        );
                }
            }
        }
    }
}
