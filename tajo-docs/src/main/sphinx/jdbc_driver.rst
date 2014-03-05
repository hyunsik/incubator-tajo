*************************************
Tajo JDBC Driver
*************************************

Apache Tajo™ provides JDBC driver
which enables Java applciations to easily access Apache Tajo in a RDBMS-like manner.
In this section, we explain how to get JDBC driver and an example code.

How to get JDBC driver
=======================

Tajo provides some necesssary jar files packaged by maven. In order get the jar files,
please follow the below commands.

.. code-block:: bash

  $ cd tajo-x.y.z-incubating
  $ mvn clean package -DskipTests -Pdist -Dtar
  $ ls -l tajo-dist/target/tajo-x.y.z-incubating/share/jdbc-dist


CLASSPATH
=======================

In order to use the JDBC driver, you should set the jar files included in 
``tajo-dist/target/tajo-x.y.z-incubating/share/jdbc-dist`` to your ``CLASSPATH``.

.. code-block:: bash

  CLASSPATH=path/to/tajo-jdbc/*:${TAJO_HOME}/conf:$(hadoop classpath)

.. note::

  You can get ${hadoop classpath} by executing ``bin/hadoop classpath``.

An Example JDBC Client
=======================

.. code-block:: java

  import java.sql.Connection;
  import java.sql.ResultSet;
  import java.sql.Statement;
  import java.sql.DriverManager;

  public class TajoJDBCClient {
    
    ....

    public static void main(String[] args) throws Exception {
      Class.forName("org.apache.tajo.jdbc.TajoDriver").newInstance();
      Connection conn = DriverManager.getConnection("jdbc:tajo://127.0.0.1:26002");

      Statement stmt = null;
      ResultSet rs = null;
      try {
        stmt = conn.createStatement();
        rs = stmt.executeQuery("select * from table1");
        while (rs.next()) {
          System.out.println(rs.getString(1) + "," + rs.getString(3));
        }
      } finally {
        if (rs != null) rs.close();
        if (stmt != null) stmt.close();
        if (conn != null) conn.close();
      }
    }
  }
