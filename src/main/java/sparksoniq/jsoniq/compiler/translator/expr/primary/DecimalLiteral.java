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
 package sparksoniq.jsoniq.compiler.translator.expr.primary;

import java.math.BigDecimal;

import sparksoniq.jsoniq.compiler.translator.metadata.ExpressionMetadata;
import sparksoniq.semantics.visitor.AbstractExpressionOrClauseVisitor;


public class DecimalLiteral extends PrimaryExpression {

    public BigDecimal getValue() {
        return value;
    }

    private BigDecimal value;

    public DecimalLiteral(BigDecimal _value, ExpressionMetadata metadata){
        super(metadata);
        this.value = _value;
    }

    @Override
    public String serializationString(boolean prefix){
        String result = "(primaryExpr ";
        result += this.getValue();
        result += ")";
        return result;
    }

    @Override
    public  <T> T accept(AbstractExpressionOrClauseVisitor<T> visitor, T argument){
        return visitor.visitDecimal(this, argument);
    }
}
