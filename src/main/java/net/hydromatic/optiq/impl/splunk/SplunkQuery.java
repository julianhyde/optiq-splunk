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

import net.hydromatic.optiq.impl.splunk.search.*;
import net.hydromatic.optiq.impl.splunk.util.StringUtils;

import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * Query against Splunk.
 *
 * @author jhyde
 */
public class SplunkQuery<T> extends AbstractEnumerable<T>
{
    private final SplunkConnection splunkConnection;
    private final String search;
    private final String earliest;
    private final String latest;
    private final List<String> fieldList;

    private static final Object DUMMY_RECORD = new int[0];

    public SplunkQuery(
        SplunkConnection splunkConnection,
        String search,
        String earliest,
        String latest,
        List<String> fieldList)
    {
        this.splunkConnection = splunkConnection;
        this.search = search;
        this.earliest = earliest;
        this.latest = latest;
        this.fieldList = fieldList;
        assert splunkConnection != null;
        assert search != null;
    }

    public String toString() {
        return "SplunkQuery {" + search + "}";
    }

    public Iterator<T> iterator() {
        return Linq4j.enumeratorIterator(enumerator());
    }

    public Enumerator<T> enumerator() {
        final MySearchResultListener<T> listener =
            new MySearchResultListener<T>(splunkConnection);
        Thread thread = new Thread(listener);
        thread.start();
        return new Enumerator<T>() {
            boolean done = false;
            T current;

            public T current() {
                return current;
            }

            public boolean moveNext() {
                if (done) {
                    return false;
                }
                try {
                    current = listener.queue.take();
                    if (current == DUMMY_RECORD) {
                        done = true;
                        current = null;
                        return false;
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                return true;
            }

            public void reset() {
                throw new UnsupportedOperationException();
            }
        };
    }

    private class MySearchResultListener<T>
        implements SearchResultListener, Runnable
    {
        private final SplunkConnection splunkConnection;
        private final ArrayBlockingQueue<T> queue =
            new ArrayBlockingQueue<T>(100);
        private String[] fieldNames;

        public MySearchResultListener(
            SplunkConnection splunkConnection)
        {
            this.splunkConnection = splunkConnection;
        }

        public void run() {
            try {
                Map<String, String> args = new HashMap<String, String>();
                if (fieldList != null) {
                    String fields =
                        StringUtils.encodeList(fieldList, ',').toString();
                    args.put("field_list", fields);
                }
                if (earliest != null) {
                    args.put("earliest_time", earliest);
                }
                if (latest != null) {
                    args.put("latest_time", latest);
                }
                splunkConnection.getSearchResults(search, args, this);
            } finally {
                queue.add((T) DUMMY_RECORD);
            }
        }

        public boolean processSearchResult(String[] fieldValues) {
            T t;
            if (fieldNames.length == 1) {
                t = (T) fieldValues[0];
            } else {
                t = (T) fieldValues;
            }
            try {
                queue.put(t);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            return true;
        }

        public void setFieldNames(String[] fieldNames) {
            this.fieldNames = fieldNames;
        }
    }
}

// End SplunkQuery.java
