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
package net.hydromatic.optiq.impl.splunk.search;

import net.hydromatic.optiq.impl.splunk.util.HttpUtils;
import net.hydromatic.optiq.impl.splunk.util.StringUtils;

import au.com.bytecode.opencsv.CSVReader;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static net.hydromatic.optiq.impl.splunk.util.HttpUtils.*;

/**
 * Connection to Splunk.
 */
public class SplunkConnection {
    private static final Logger LOGGER =
        Logger.getLogger(SplunkConnection.class.getName());

    private static final Pattern SESSION_KEY =
        Pattern.compile(
            "<response>\\s*<sessionKey>([0-9a-f]+)</sessionKey>\\s*</response>");

    final URL url;
    final String username, password;
    String sessionKey;
    final Map<String, String> requestHeaders = new HashMap<String, String>();

    public SplunkConnection(String url, String username, String password)
        throws MalformedURLException
    {
        this(new URL(url), username, password);
    }

    public SplunkConnection(URL url, String username, String password) {
        this.url      = url;
        this.username = username;
        this.password = password;
        connect();
    }

    private static void close(Closeable c) {
        try {
            c.close();
        } catch (Exception ignore) {
            // ignore
        }
    }

    private void connect()
    {
        BufferedReader rd = null;

        try {
            String loginUrl =
                String.format(
                    "%s://%s:%d/services/auth/login",
                    url.getProtocol(),
                    url.getHost(),
                    url.getPort());

            StringBuilder data = new StringBuilder();
            appendURLEncodedArgs(
                data, "username", username, "password", password);

            rd = new BufferedReader(
                new InputStreamReader(
                    post(
                        loginUrl,
                        data,
                        requestHeaders)));

            String line;
            StringBuilder reply = new StringBuilder();
            while ((line = rd.readLine()) != null) {
                reply.append(line);
                reply.append("\n");
            }

            Matcher m = SESSION_KEY.matcher(reply);
            if (m.find()) {
                sessionKey = m.group(1);
                requestHeaders.put("Authorization", "Splunk " + sessionKey);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            close(rd);
        }
    }

    public void getSearchResults(
        String search,
        Map<String, String> otherArgs,
        SearchResultListener srl)
    {
        String searchUrl =
            String.format(
                "%s://%s:%d/services/search/jobs/export",
                url.getProtocol(),
                url.getHost(),
                url.getPort());

        StringBuilder data = new StringBuilder();
        Map<String, String> args = new HashMap<String, String>();
        if (otherArgs != null) {
            args.putAll(otherArgs);
        }
        args.put("search", search);
        // override these args
        args.put("output_mode", "csv");
        args.put("preview", "0");

        //TODO: remove this once the csv parser can handle leading spaces
        args.put("check_connection", "0");

        appendURLEncodedArgs(data, args);
        try {
            // wait at most 30 minutes for first result
            parseResults(
                post(searchUrl, data, requestHeaders, 10000, 1800000),
                srl);
        } catch (Exception e) {
            StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            LOGGER.warning(e.getMessage() + "\n" + sw);
        }
    }

    private static void parseResults(InputStream in, SearchResultListener srl)
        throws IOException
    {
        CSVReader csvr = new CSVReader(new InputStreamReader(in));
        try {
            String [] header = csvr.readNext();

            if (header != null
                && header.length > 0
                && !(header.length == 1 && header[0].isEmpty()))
            {
                srl.setFieldNames(header);

                String[] line;
                while ((line = csvr.readNext()) != null) {
                    if (line.length == header.length) {
                        srl.processSearchResult(line);
                    }
                }
            }
        } catch (IOException ignore) {
            StringWriter sw = new StringWriter();
            ignore.printStackTrace(new PrintWriter(sw));
            LOGGER.warning(ignore.getMessage() + "\n" + sw);
        } finally {
            HttpUtils.close(csvr); // CSVReader closes the inputstream too
        }
    }

    static class DummySearchResultListener implements SearchResultListener {
        String[] fieldNames = null;
        int resultCount = 0;
        boolean print = false;

        public DummySearchResultListener(boolean print) {
            this.print = print;
        }

        public void setFieldNames(String[] fieldNames) {
            this.fieldNames = fieldNames;
        }

        public boolean processSearchResult(String[] values)
        {
            resultCount++;
            if (print) {
                for (int i = 0; i < this.fieldNames.length; ++i) {
                    System.out.printf("%s=%s\n", this.fieldNames[i], values[i]);
                }
                System.out.println();
            }
            return true;
        }

        public int getResultCount() {
            return resultCount;
        }
    }

    public static void parseArgs(String[] args, Map<String, String> map)
    {
        for (int i = 0; i < args.length; i++) {
            String argName = args[i++];
            String argValue = i < args.length ? args[i] : "";

            if (!argName.startsWith("-")) {
                throw new IllegalArgumentException(
                    "invalid argument name: " + argName
                    + ". Argument names must start with -");
            }
            map.put(argName.substring(1), argValue);
        }
    }

    public static void printUsage(String errorMsg) {
        String[] strings = {
            "Usage: java Connection -<arg-name> <arg-value>",
            "The following <arg-name> are valid",
            "search        - required, search string to execute",
            "field_list    - "
            + "required, list of fields to request, comma delimited",
            "uri           - "
            + "uri to splunk's mgmt port, default: https://localhost:8089",
            "username      - "
            + "username to use for authentication, default: admin",
            "password      - "
            + "password to use for authentication, default: changeme",
            "earliest_time - earliest time for the search, default: -24h",
            "latest_time   - latest time for the search, default: now",
            "-print        - whether to print results or just the summary"
        };
        System.err.println(errorMsg);
        for (String s : strings) {
            System.err.println(s);
        }
        System.exit(1);
    }

    public static void main(String[] args) throws MalformedURLException
    {
        Map<String, String> argsMap = new HashMap<String, String>();
        argsMap.put("uri",           "https://localhost:8089");
        argsMap.put("username",      "admin");
        argsMap.put("password",      "changeme");
        argsMap.put("earliest_time", "-24h");
        argsMap.put("latest_time",   "now");
        argsMap.put("-print",        "true");

        parseArgs(args, argsMap);


        String search = argsMap.get("search"),
            field_list = argsMap.get("field_list");

        if (search == null) {
            printUsage("Missing required argument: search");
        }
        if (field_list == null) {
            printUsage("Missing required argument: field_list");
        }

        List<String> fieldList = StringUtils.decodeList(field_list, ',');

        SplunkConnection c =
            new SplunkConnection(
                argsMap.get("uri"),
                argsMap.get("username"),
                argsMap.get("password"));

        Map<String, String> searchArgs = new HashMap<String, String>();
        searchArgs.put("earliest_time", argsMap.get("earliest_time"));
        searchArgs.put("latest_time", argsMap.get("latest_time"));
        searchArgs.put(
            "field_list",
            StringUtils.encodeList(fieldList, ',').toString());


        DummySearchResultListener dummy =
            new DummySearchResultListener(
                Boolean.valueOf(argsMap.get("-print")));
        long start = System.currentTimeMillis();
        c.getSearchResults(search, searchArgs, dummy);

        System.out.printf(
            "received %d results in %dms\n",
            dummy.getResultCount(),
            (System.currentTimeMillis() - start));
    }
}

// End SplunkConnection.java
