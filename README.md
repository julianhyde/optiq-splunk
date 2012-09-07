optiq-splunk
============

Splunk adapter for Optiq.

Also provides a JDBC driver for Splunk.

Prerequisites
=============

First <a href="https://github.com/julianhyde/optiq/blob/master/README.md">install Optiq</a>.

Download and build
==================

    $ git clone git://github.com/julianhyde/optiq-splunk.git
    $ cd optiq-splunk
    $ mvn compile

Example
=======

Optiq-splunk provides a JDBC driver:

    Class.forName("net.hydromatic.optiq.impl.splunk.Driver");
    Connection connection = DriverManager.getConnection("jdbc:splunk:");
    Statement statement = connection.createStatement();
    ResultSet resultSet = statement.executeQuery(
        "select \"source\", count(*)\n"
        + "from \"splunk\".\"splunk\"\n"
        + "group by \"source\"");
    print(resultSet);
    resultSet.close();
    statement.close();
    connection.close();

You can also register a SplunkSchema as a schema within an Optiq instance.
Then you can combine with other data sources.

Status
======

The following features are complete.

* JDBC driver.
* Basic queries.
* Rules to push down filter, project and aggregation onto Splunk.

Backlog
=======

* Rule to push down join

More information
================

* License: Apache License, Version 2.0.
* Author: Julian Hyde
* Blog: http://julianhyde.blogspot.com
* Project page: http://www.hydromatic.net/optiq-splunk
* Source code: http://github.com/julianhyde/optiq-splunk
* Developers list: http://groups.google.com/group/optiq-dev

