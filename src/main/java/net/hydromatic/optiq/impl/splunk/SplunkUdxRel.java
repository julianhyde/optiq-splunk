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

import net.hydromatic.linq4j.expressions.*;
import net.hydromatic.optiq.impl.splunk.search.SplunkConnection;
import net.hydromatic.optiq.rules.java.EnumerableRel;
import net.hydromatic.optiq.rules.java.EnumerableRelImplementor;

import org.eigenbase.rel.TableAccessRelBase;
import org.eigenbase.relopt.*;

import java.lang.reflect.Constructor;
import java.util.*;

/**
 * Relational expression representing a scan of a Splunk table.
 *
 * @author jhyde
 */
public class SplunkUdxRel extends TableAccessRelBase implements EnumerableRel {
    final SplunkTable splunkTable;
    final String search;
    final String earliest;
    final String latest;
    final List<String> fieldList;

    protected SplunkUdxRel(
        RelOptCluster cluster,
        RelOptTable table,
        SplunkTable splunkTable,
        String search,
        String earliest,
        String latest,
        List<String> fieldList)
    {
        super(
            cluster,
            cluster.traitSetOf(CallingConvention.ENUMERABLE),
            table);
        this.splunkTable = splunkTable;
        this.search = search;
        this.earliest = earliest;
        this.latest = latest;
        this.fieldList = fieldList;

        assert splunkTable != null;
        assert search != null;
    }

    private static final Constructor CONSTRUCTOR =
        Types.lookupConstructor(
            SplunkQuery.class,
            SplunkConnection.class,
            String.class,
            String.class,
            String.class,
            List.class);

    public BlockExpression implement(EnumerableRelImplementor implementor) {
        Expression expression =
            Expressions.new_(
                CONSTRUCTOR,
                Expressions.field(
                    Types.castIfNecessary(
                        SplunkSchema.class, splunkTable.schema.getExpression()),
                    "splunkConnection"),
                Expressions.constant(search),
                Expressions.constant(earliest),
                Expressions.constant(latest),
                fieldList == null
                    ? Expressions.constant(null)
                    : constantStringList(fieldList));
        return Blocks.toBlock(expression);
    }

    private static Expression constantStringList(final List<String> strings) {
        return Expressions.call(
            Arrays.class,
            "asList",
            Expressions.newArrayInit(
                Object.class,
                new AbstractList<Expression>() {
                    @Override
                    public Expression get(int index) {
                        return Expressions.constant(strings.get(index));
                    }

                    @Override
                    public int size() {
                        return strings.size();
                    }
                }));
    }
}

// End SplunkUdxRel.java
