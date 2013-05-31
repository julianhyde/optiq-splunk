/*
// Licensed to Julian Hyde under one or more contributor license
// agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
//
// Julian Hyde licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/
package net.hydromatic.optiq.impl.splunk;

import net.hydromatic.linq4j.*;
import net.hydromatic.linq4j.expressions.Expression;
import net.hydromatic.optiq.*;
import net.hydromatic.optiq.impl.TableInSchemaImpl;
import net.hydromatic.optiq.impl.java.JavaTypeFactory;
import net.hydromatic.optiq.impl.splunk.search.SplunkConnection;

import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;

import java.lang.reflect.Type;
import java.util.*;

/**
 * Splunk schema.
 *
 * @author jhyde
 */
public class SplunkSchema implements Schema {
    /** The name of the one and only table. */
    public static final String SPLUNK_TABLE_NAME = "splunk";

    public final QueryProvider queryProvider;
    public final SplunkConnection splunkConnection;
    private final JavaTypeFactory typeFactory;
    private final Expression expression;
    private final SplunkTable table;
    private final List<TableInSchema> tableList;

    public SplunkSchema(
        QueryProvider queryProvider,
        SplunkConnection splunkConnection,
        JavaTypeFactory typeFactory,
        Expression expression)
    {
        this.queryProvider = queryProvider;
        this.splunkConnection = splunkConnection;
        this.typeFactory = typeFactory;
        this.expression = expression;
        RelDataType stringType = typeFactory.createType(String.class);
        final RelDataType rowType =
            typeFactory.createStructType(
                new RelDataTypeFactory.FieldInfoBuilder()
                    .add("source", stringType)
                    .add("sourcetype", stringType)
                    .add("_extra", stringType));
        final Type elementType = typeFactory.getJavaClass(rowType);
        this.table =
            new SplunkTable(elementType, rowType, this, SPLUNK_TABLE_NAME);
        this.tableList =
            Collections.<TableInSchema>singletonList(
                new TableInSchemaImpl(this, "splunk", TableType.TABLE, table));
    }

    public Expression getExpression() {
        return expression;
    }

    public Collection<TableInSchema> getTables() {
        return tableList;
    }

    public JavaTypeFactory getTypeFactory() {
        return typeFactory;
    }

    public List<TableFunction> getTableFunctions(String name) {
        return Collections.emptyList();
    }

    public Table getTable(String name) {
        return name.equals(SPLUNK_TABLE_NAME)
            ? table
            : null;
    }

    public QueryProvider getQueryProvider() {
        return queryProvider;
    }

    public Map<String, List<TableFunction>> getTableFunctions() {
        return Collections.emptyMap();
    }

    public <T> Table<T> getTable(String name, Class<T> elementType) {
        //noinspection unchecked
        return getTable(name);
    }

    public Schema getSubSchema(String name) {
        return null;
    }

    public Collection<String> getSubSchemaNames() {
        return Collections.emptyList();
    }
}

// End SplunkSchema.java
