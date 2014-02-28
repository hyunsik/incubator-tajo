*****************
Build Source Code
*****************

You prepare the prerequisites and the source code, you can build the source code now. The first step

The first step of the installation procedure is to configure the source tree for your system and choose the options you would like. This is done by running the configure script. For a default installation simply enter:

You can compile source code and get a binary archive as follows:

.. code-block:: bash

  $ cd tajo
  $ mvn clean package -DskipTests -Pdist -Dtar
  $ ls tajo-dist/target/tajo-x.y.z-SNAPSHOT.tar.gz


====================================
Additional Build Instructions
====================================

Maven build goals:

  * Clean                     : mvn clean
  * Compile                   : mvn compile
  * Run tests                 : mvn test
  * Run integrating tests     : mvn verify
  * Create JAR                : mvn package
  * Run findbugs              : mvn compile findbugs:findbugs
  * Install JAR in M2 cache   : mvn install
  * Build distribution        : mvn package [-Pdist][-Dtar]

Build options:
  * Use -Dtar to create a TAR with the distribution (using -Pdist)

Tests options:
  * Use -DskipTests to skip tests when running the following Maven goals:
    ``package``,  ``install``, ``deploy`` or ``verify``
  * ``-Dtest=<TESTCLASSNAME>,<TESTCLASSNAME#METHODNAME>,....``
  * ``-Dtest.exclude=<TESTCLASSNAME>``
  * ``-Dtest.exclude.pattern=**/<TESTCLASSNAME1>.java,**/<TESTCLASSNAME2>.java``

Building distributions:

====================================
Create binary distribution
====================================

.. code-block:: bash

  $ mvn package -Pdist -DskipTests -Dtar
