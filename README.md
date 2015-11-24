# MasterThesisTest
MasterThesisTest

*How to use Big-Bench-Flink*

**Environment Variable in run.sh**
- SCALA_QUERIES+=(2 4 7 9 12 14 21 22 26 28)
- JAVA_QUERIES+=(1 5 6 8 11 15 17 24 25 29)
- PACKAGE_NAME="de.tub.cs.bigbench."
- JAR_NAME_SCALA="flink_scala.jar"
- JAR_NAME_JAVA="flink_java.jar"

*Please make jar_package file like the above configuration then move it to engines/flink/queries/*

*Then you can run a commend line >>* **./bin/bigBench runQuery -q 30 -U -b**


*Studyiung Table API; example is TPCH Query*

*Query Folder Scala from Philip, Java from Guido*

### Philip 
**Query: 02, 04, 07, 09, 12, 14, 21, 22, 26, 28**

### Guido
**Query: 01 (done), 05 (done), 06 (check data), 8 (check data), 11 (done), 15 (done), 17 (done), 24 (done), 25 (done-mahout) 29 (done)**

**We have to make a document about Flink API compatitable to Hive Functino on Google Docs**
