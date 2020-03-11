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

package org.rumbledb.types;

public enum ItemTypes {
    Item,

    JSONItem,
    ObjectItem,
    ArrayItem,

    AtomicItem,
    StringItem,
    IntegerItem,
    DecimalItem,
    DoubleItem,
    BooleanItem,

    DurationItem,
    YearMonthDurationItem,
    DayTimeDurationItem,

    DateTimeItem,
    DateItem,
    TimeItem,

    AnyURIItem,

    HexBinaryItem,
    Base64BinaryItem,

    FunctionItem,

    NullItem;

    public static String getItemTypeName(String fullTypeName) {
        String itemPostfix = "Item";
        if (!itemPostfix.equals(fullTypeName) && fullTypeName.endsWith("Item")) {
            return Character.toLowerCase(fullTypeName.charAt(0))
                +
                fullTypeName.substring(1, fullTypeName.length() - itemPostfix.length());
        }
        return fullTypeName;
    }
}