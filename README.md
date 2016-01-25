# MasterThesis
MasterThesis

*How to use the BigBench Flink engine*

**Environment Variable in run.sh**
- SCALA_QUERIES+=(2 4 7 9 12 14 21 22 26 28)
- JAVA_QUERIES+=(1 5 6 8 11 13 15 17 20 23 24 25 29)
- PACKAGE_NAME="de.tub.cs.bigbench."
- JAR_NAME_SCALA="flink_scala.jar"
- JAR_NAME_JAVA="flink_java.jar"
- NOT_IMPLEMENTED_QUERIES+=(3 10 16 18 19 27 30)

*Please make jar_package file like the above configuration then move it to engines/flink/queries/*

*In order to run a specific query from command line >>* **./bin/bigBench runQuery -q 30 -U -b**

*Work from 2 contributors*

### Philip - Scala
**Queries: 02, 04, 07, 09, 12, 14, 21, 22, 26, 28**

### Guido - Java
*Tested queries: 01, 06, 08, 11, 13, 15, 17, 24, 25, 29*  
*Other implemented queries: 05, 20 (included into the flink_0.10 branch)*
