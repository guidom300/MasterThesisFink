package de.tub.cs.bigbench;

import org.apache.flink.core.fs.FileSystem;
import org.apache.mahout.classifier.sgd.RunLogistic;
import org.apache.mahout.classifier.sgd.TrainLogistic;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.util.Collector;
import java.util.ArrayList;
import java.util.Arrays;

public class Query5 {

    //Config
    public static final String[] q05_cd_education_status_IN = new String[]{"Advanced Degree", "College", "4 yr Degree", "2 yr Degree"};
    public static final String q05_cd_gender = "M";
    public static final String q05_i_category = "Books";

    //Mapping
    public static final String items_path = "/Users/gm/bigbench/data-generator/output/item.dat";
    public static final String customers_path = "/Users/gm/bigbench/data-generator/output/customer.dat";
    public static final String customers_demographics_path = "/Users/gm/bigbench/data-generator/output/customer_demographics.dat";
    public static final String web_clickstreams_path = "/Users/gm/bigbench/data-generator/output/web_clickstreams.dat";

    public static final String items_mask = "1000000000001000000000";
    public static final String customers_mask = "101000000000000000";
    public static final String customers_demographics_mask = "110100000";
    public static final String web_clickstreams_mask = "000101";

    //INPUT
    public static final String TMP_LOG_REG_IN_FILE = "/tmp/input_log.csv";

    //OUTPUT
    public static final String TMP_LOG_REG_MODEL_FILE = "/tmp/output_log.csv";


    public static void main(String[] args) throws Exception {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // item -> i_item_sk, i_category
        DataSet<Tuple2<Long, String>> items = env.readCsvFile(items_path)
                .lineDelimiter("\n")
                .fieldDelimiter('|')
                .includeFields(items_mask)
                .ignoreInvalidLines()
                .types(Long.class, String.class);

        // customers -> c_customer_sk, c_current_cdemo_sk
        DataSet<Tuple2<Long, Long>> customers = env.readCsvFile(customers_path)
                .lineDelimiter("\n")
                .fieldDelimiter('|')
                .includeFields(customers_mask)
                .ignoreInvalidLines()
                .types(Long.class, Long.class);

        //cutomers_demographics -> cd_demo_sk, cd_gender, cd_education_status
        DataSet<Tuple3<Long, String, String>> customers_demographics = env.readCsvFile(customers_demographics_path)
                .lineDelimiter("\n")
                .fieldDelimiter('|')
                .includeFields(customers_demographics_mask)
                .ignoreInvalidLines()
                .types(Long.class, String.class, String.class);

        // web_clickstreams -> wcs_item_sk, wcs_user_sk
        DataSet<Tuple2<Long, Long>> web_clickstreams = env.readCsvFile(web_clickstreams_path)
                .lineDelimiter("\n")
                .fieldDelimiter('|')
                .includeFields(web_clickstreams_mask)
                .ignoreInvalidLines()
                .types(Long.class, Long.class);

        /* web_clickstreams -> wcs_item_sk, wcs_user_sk
        DataSet<String> web_clickstreams = env.readTextFile("/Users/gm/bigbench/data-generator/output/web_clickstreams.dat");
        //web_clickstreams.first(1).print();
        DataSet<Tuple6<String, String, String, String, String, String>> tuple = web_clickstreams.flatMap(new Tokenizer());

        tuple.writeAsCsv("/Users/gm/bigbench/data-generator/output/outttt.csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);*/



        DataSet<Tuple4<Long, Integer, Integer, String>> q05_tmp_cust_clicks =
                customers
                .join(customers_demographics)
                .where(1)                   //ct.c_current_cdemo_sk
                .equalTo(0)                 //cdt.cd_demo_sk
                .with(new CTJoinCDT())
                .join(web_clickstreams)
                .where(1)                   //ct.c_customer_sk
                .equalTo(1)                 //wcst.wcs_user_sk
                .with(new CTJoinWCST())
                .join(items)
                .where(4)                   //wcst.wcs_item_sk
                .equalTo(0)                 //it.i_item_sk
                .with(new CTJoinIT());


        //flatMap onto category, emit 1, if fits then SUM (WordCount-Like)
        q05_tmp_cust_clicks
                .flatMap(new CheckCategory())
                .groupBy(0, 1, 2)
                .aggregate(Aggregations.SUM, 3)
                .map(new LabelFunction())
                .writeAsCsv(TMP_LOG_REG_IN_FILE).setParallelism(1);

        env.execute();

    }

    // *************************************************************************
    //     DATA TRASFORMATIONS
    // *************************************************************************

    // Join customer - customers_demographics function
    public static class CTJoinCDT
            implements JoinFunction<Tuple2<Long, Long>, Tuple3<Long, String, String>, Tuple4<Long, Long, String, String>> {

        @Override
        public Tuple4<Long, Long, String, String> join(Tuple2<Long, Long> ct, Tuple3<Long, String, String> cdt) throws Exception {
            return new Tuple4<>(ct.f1, ct.f0, cdt.f1, cdt.f2);
        }

    }

    // Join customer - web_clickstreams function
    public static class CTJoinWCST
            implements JoinFunction<Tuple4<Long, Long, String, String>, Tuple2<Long, Long>, Tuple5<Long, Long, String, String, Long>> {

        @Override
        public Tuple5<Long, Long, String, String, Long> join(Tuple4<Long, Long, String, String> ct, Tuple2<Long, Long> wcst) throws Exception {
            return new Tuple5<>(ct.f0, ct.f1, ct.f2, ct.f3, wcst.f0);
        }
    }

    // Join customer - web_clickstreams function
    public static class CTJoinIT
            implements JoinFunction<Tuple5<Long, Long, String, String, Long>, Tuple2<Long, String>, Tuple4<Long, Integer, Integer, String>> {

        @Override
        public Tuple4<Long, Integer, Integer, String> join(Tuple5<Long, Long, String, String, Long> ct, Tuple2<Long, String> it) throws Exception {
            return new Tuple4<>(ct.f1, (Arrays.asList(q05_cd_education_status_IN).contains(ct.f3))? 1 : 0, (ct.f2.equals(q05_cd_gender))? 1 : 0, it.f1);
        }
    }

    public static class CheckCategory implements FlatMapFunction<Tuple4<Long, Integer, Integer, String>, Tuple4<Long, Integer, Integer, Integer>> {

        @Override
        public void flatMap(Tuple4<Long, Integer, Integer, String> t, Collector<Tuple4<Long, Integer, Integer, Integer>> out) throws Exception {
            if(t.f3.equals(q05_i_category))
            {
                out.collect(new Tuple4<>(t.f0, t.f1, t.f2, 1));
            }
        }
    }

    public static class LabelFunction implements MapFunction<Tuple4<Long, Integer, Integer, Integer>, Tuple4<Long, Integer, Integer, Integer>> {

        @Override
        public Tuple4<Long, Integer, Integer, Integer> map(Tuple4<Long, Integer, Integer, Integer> in) throws Exception {
            return new Tuple4<>(in.f0, in.f1, in.f2, (in.f3 > 2)? 1 : 0);
        }
    }

    public static final class NullTokenizer implements FlatMapFunction<String, ArrayList<String>> {

        @Override
        public void flatMap(String value, Collector<ArrayList<String>> out) {
            // normalize and split the line
            String[] charvalues = value.split("|");
            Integer c_pipe = 0;

            for(String v :  charvalues)
                if(v.equals("|"))
                    c_pipe++;

            String[] tokens = value.split("\\|");
            ArrayList<String> tuple = new ArrayList<String>();

            for (String s : tokens) {
                tuple.add(s);
            }

            if(c_pipe.equals(tokens.length))
                tuple.add("");

            out.collect(tuple);
        }
    }
}
