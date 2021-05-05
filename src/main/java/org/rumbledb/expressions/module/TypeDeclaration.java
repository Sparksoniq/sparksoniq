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

package org.rumbledb.expressions.module;


import org.rumbledb.api.Item;
import org.rumbledb.compiler.VisitorConfig;
import org.rumbledb.context.Name;
import org.rumbledb.exceptions.ExceptionMetadata;
import org.rumbledb.expressions.AbstractNodeVisitor;
import org.rumbledb.expressions.ExecutionMode;
import org.rumbledb.expressions.Node;

import java.util.Collections;
import java.util.List;

public class TypeDeclaration extends Node {

    private final Name name;
    private final Item definitionItem;

    public TypeDeclaration(
            Name name,
            Item definitionItem,
            ExceptionMetadata metadata
    ) {
        super(metadata);
        this.definitionItem = definitionItem;
        this.name = name;
    }

    public Item getDefinition() {
        return this.definitionItem;
    }

    public Name getName() {
        return this.name;
    }

    @Override
    public <T> T accept(AbstractNodeVisitor<T> visitor, T argument) {
        return visitor.visitTypeDeclaration(this, argument);
    }

    @Override
    public void initHighestExecutionMode(VisitorConfig visitorConfig) {
        this.highestExecutionMode = ExecutionMode.LOCAL;
    }

    /**
     * Prints the node tree to a string buffer.
     *
     * @param buffer a string buffer to write to
     * @param indent the current level of indentation
     */
    public void print(StringBuffer buffer, int indent) {
        for (int i = 0; i < indent; ++i) {
            buffer.append("  ");
        }
        buffer.append("TypeDeclaration " + this.getName());
        buffer.append(" as " + this.definitionItem.toString());
        buffer.append("\n");
    }

    @Override
    public void serializeToJSONiq(StringBuffer sb, int indent) {
        indentIt(sb, indent);
        sb.append("declare type " + this.name + " as " + definitionItem + ";\n");
    }

    @Override
    public List<Node> getChildren() {
        return Collections.emptyList();
    }
}

