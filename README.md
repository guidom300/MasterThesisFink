# Master Thesis Flink
Big Bench Java implementation running on Flink

*How to use the Big Bench Flink engine*

**Environment Variable in run.sh**
- JAVA_QUERIES+=(1 5 6 8 11 13 15 17 20 23 24 25 29)
- PACKAGE_NAME="de.tub.cs.bigbench."
- JAR_NAME_JAVA="flink_java.jar"

*The jar file needs to be moved to the engines/flink/queries/*

*Command line >>* **./bin/bigBench runQuery -q [number] -U -b**

*Tested queries: 01, 06, 08, 11, 13, 15, 17, 24, 25, 29*  
*Other implemented queries: 05, 20 (included into the flink_0.10 branch)*
