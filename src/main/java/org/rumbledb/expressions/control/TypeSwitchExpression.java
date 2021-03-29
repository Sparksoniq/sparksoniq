package org.rumbledb.expressions.control;


import org.rumbledb.compiler.VisitorConfig;
import org.rumbledb.exceptions.ExceptionMetadata;
import org.rumbledb.expressions.AbstractNodeVisitor;
import org.rumbledb.expressions.ExecutionMode;
import org.rumbledb.expressions.Expression;
import org.rumbledb.expressions.Node;

import java.util.ArrayList;
import java.util.List;

public class TypeSwitchExpression extends Expression {

    private final Expression testCondition;
    private final List<TypeswitchCase> cases;
    private final TypeswitchCase defaultCase;

    public TypeSwitchExpression(
            Expression testCondition,
            List<TypeswitchCase> cases,
            TypeswitchCase defaultCase,
            ExceptionMetadata metadataFromContext
    ) {

        super(metadataFromContext);
        this.testCondition = testCondition;
        this.cases = cases;
        this.defaultCase = defaultCase;
    }

    public Expression getTestCondition() {
        return this.testCondition;
    }

    public List<TypeswitchCase> getCases() {
        return this.cases;
    }

    public TypeswitchCase getDefaultCase() {
        return this.defaultCase;
    }

    @Override
    public List<Node> getChildren() {
        List<Node> result = new ArrayList<>();
        result.add(this.testCondition);
        for (TypeswitchCase c : this.cases) {
            result.add(c.getReturnExpression());
        }
        result.add(this.defaultCase.getReturnExpression());
        return result;
    }

    @Override
    public void serializeToJSONiq(StringBuffer sb, int indent) {
        indentIt(sb, indent);
        sb.append("typeswitch (");
        testCondition.serializeToJSONiq(sb, 0);
        sb.append(")\n");
        for (TypeswitchCase c : this.cases) {
            indentIt(sb, indent + 1);
            sb.append("case ($");
            sb.append(c.getVariableName().toString() + " as ");
            for (int i = 0; i < c.getUnion().size(); i++) {
                c.getUnion().get(i).toString();
                if (i == c.getUnion().size() - 1) {
                    sb.append(") ");
                } else {
                    sb.append(" | ");
                }
            }
            sb.append("return (");
            c.getReturnExpression().serializeToJSONiq(sb, 0);
            sb.append(")\n");
        }

        if (defaultCase != null) {
            indentIt(sb, indent + 1);
            // TODO seems somehow wrong, shouldve been Expression not the case
            sb.append("default ($");
            sb.append(defaultCase.getVariableName().toString() + " as ");
            for (int i = 0; i < defaultCase.getUnion().size(); i++) {
                defaultCase.getUnion().get(i).toString();
                if (i == defaultCase.getUnion().size() - 1) {
                    sb.append(") ");
                } else {
                    sb.append(" | ");
                }
            }
            sb.append(")\n");

            sb.append("return (");
            defaultCase.getReturnExpression().serializeToJSONiq(sb, 0);
            sb.append(")\n");
        }
    }

    @Override
    public void initHighestExecutionMode(VisitorConfig visitorConfig) {
        this.highestExecutionMode = this.defaultCase.getReturnExpression().getHighestExecutionMode(visitorConfig);

        if (this.highestExecutionMode == ExecutionMode.RDD) {
            for (TypeswitchCase c : this.cases) {
                if (!c.getReturnExpression().getHighestExecutionMode(visitorConfig).isRDDOrDataFrame()) {
                    this.highestExecutionMode = ExecutionMode.LOCAL;
                    break;
                }
            }
        }
    }

    @Override
    public <T> T accept(AbstractNodeVisitor<T> visitor, T argument) {
        return visitor.visitTypeSwitchExpression(this, argument);
    }
}
