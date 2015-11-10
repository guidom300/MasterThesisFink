package de.tub.cs.bigbench;


import org.apache.mahout.clustering.syntheticcontrol.kmeans.*;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;

public class RunMahout {

    private static String initPath = ""
    private static String inputPath ="";
    private static String outputPath ="";
    private static String target ="";
    private static String categories ="";
    private static String types ="";
    private static String passes ="";
    private static String features ="";



    public static void main(String... args) throws Exception {
        /* concatenate and copy csv files from HDFS to local file system
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.readCsvFile(null).types(null).writeAsCsv("TMP_OUTPUT_NO_HEADER");
        env.execute();

        BufferedWriter writer = new BufferedWriter(new FileWriter("FINALOUTPUT"));
        writer.write("HEADER\n");

        BufferedReader reader = new BufferedReader(new FileReader("TMP_OUTPUT_NO_HEADER"));
        String line;
        while ((line = reader.readLine()) != null) {
            writer.write(line);
        }
        writer.close();
        reader.close();
        */


//        String[] trainParams = {
//                "--tempDir", "/Users/gm/bigbench/data-generator/output/data2.csv", "--i", "/Users/gm/bigbench/data-generator/output/out2.csv",
//                "--c", "c_customer_sk", "--o", "2", "--predictors", "college_education", "male", "label",
//                "--types", "n", "n", "n", "--passes", "20", "--features", "20", "--rate", "1", "--lambda", "0.5"};
//        TrainLogistic.main(trainParams);

//        final String[] runParams = {
//                "--input", "/Users/gm/bigbench/data-generator/output/data2.csv", "--model", "/Users/gm/bigbench/data-generator/output/out2.csv" ,"--auc" ,"--confusion" ,"--quiet"};
//        RunLogistic.main(runParams);

        final String[] runParams = {
                "--tempDir", initPath, "-i", inputPath, "-c", initPath, "-o", outputPath, "-dm", "org.apache.mahout.common.distance.CosineDistanceMeasure",
                "-x", "10", "-ow", "-cl", "-xm"};
        Job.main(runParams);
    }
}

//echo "Command "mahout kmeans -i "$TEMP_DIR/Vec" -c "$TEMP_DIR/init-clusters" -o "$TEMP_DIR/kmeans-clusters" -dm org.apache.mahout.common.distance.CosineDistanceMeasure -x 10 -ow -cl  -xm $BIG_BENCH_ENGINE_HIVE_MAHOUT_EXECUTION
//        echo "tmp output: $TEMP_DIR/kmeans-clusters"

//     	hadoop fs -put -f $QUERY_DIR/init-clusters $TEMP_DIR/init-clusters
//runCmdWithErrorCheck mahout kmeans --tempDir "$MAHOUT_TEMP_DIR" -i "$TEMP_DIR/Vec" -c "$TEMP_DIR/init-clusters" -o "$TEMP_DIR/kmeans-clusters" -dm org.apache.mahout.common.distance.CosineDistanceMeasure
// -x 10 -ow -cl  -xm $BIG_BENCH_ENGINE_HIVE_MAHOUT_EXECUTION
//        RETURN_CODE=$?