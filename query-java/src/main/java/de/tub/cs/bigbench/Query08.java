package de.tub.cs.bigbench;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;

import static org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint.BROADCAST_HASH_SECOND;

/**
 * Created by gm on 25/11/15.
 */
public class Query08 {
    public static String q08_date_dim_mask;
    public static String q08_web_page_mask;
    public static String q08_web_sales_mask;

    public static String input_path;
    public static String output_path;
    public static String web_clickstreams_path;
    public static String date_dim_path;
    public static String web_page_path;
    public static String web_sales_path;

    // Conf
    public static final String startDate = "2001-09-02";
    public static final String endDate = "2002-09-02";
    public static final Long q08_seconds_before_purchase = (long)259200;   // 3 days in sec = 3*24*60*60
    public static final String web_page_type_filter = "review";

    public static void main(String[] args) throws Exception {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        PropertiesConfiguration config = new PropertiesConfiguration("config.properties");

        if(parseParameters(args) == 1)
            return;

        q08_date_dim_mask = config.getString("q08_date_dim_mask");
        q08_web_page_mask = config.getString("q08_web_page_mask");
        q08_web_sales_mask = config.getString("q08_web_sales_mask");

        web_clickstreams_path = input_path + "/web_clickstreams/web_clickstreams.dat";
        date_dim_path = input_path + "/date_dim/date_dim.dat";
        web_page_path = input_path + "/web_page/web_page.dat";
        web_sales_path = input_path + "/web_sales/web_sales.dat";

        // get input data
        //web_clickstreams -> wcs_click_date_sk (Long), wcs_click_time_sk (Long), wcs_sales_sk (Long),
        // wcs_web_page_sk (Long), wcs_user_sk (Long)
        DataSet<WebClick> web_clickstreams = getWebClickDataSet(env);
        //date_dim -> d_date_sk, d.d_date
        DataSet<DateDim> date_dim = getDateDimDataSet(env);
        //web_page -> w.wp_web_page_sk (Long), wp_type(String)
        DataSet<WebPage> web_page = getWebPageDataSet(env);
        //web_sales-> ws.ws_sold_date_sk (Long), ws_order_number (Long),  ws_net_paid (Double)
        DataSet<WebSales> web_sales = getWebSalesDataSet(env);


        //Filter by year
        DataSet<Tuple1<Long>> dd =
                date_dim
                        .filter(new FilterDateDim())
                        .project(0);


        DataSet<Tuple4<Long, Long, String, String>> q08_map_output =
                web_clickstreams
                        .join(dd, BROADCAST_HASH_SECOND)
                        .where(0)
                        .equalTo(0)
                        .with(new WebClickStreamLeftJoinDD())
                        .join(web_page, BROADCAST_HASH_SECOND)
                        .where(3)
                        .equalTo(0)
                        .with(new WebClickStreamJoinWebPage())
                        .sortPartition(0, Order.ASCENDING).setParallelism(1)
                        .sortPartition(1, Order.ASCENDING).setParallelism(1)
                        .sortPartition(2, Order.ASCENDING).setParallelism(1)
                        .sortPartition(3, Order.ASCENDING).setParallelism(1);

        DataSet<Tuple1<Long>> salesWithViewedReviews =
                q08_map_output
                        .reduceGroup(new FilterSalesReview())
                        .groupBy(0)
                        .first(1);;

        DataSet<Tuple2<Double, Long>> allSalesInYear =
                web_sales
                        .join(dd)
                        .where(0)
                        .equalTo(0)
                        .with(new WebSalesJoinDD());

        DataSet<Tuple2<Integer, Double>> q08_all_sales =
                allSalesInYear
                        .reduceGroup(new MyGroupReducer());

        DataSet<Tuple2<Integer, Double>> q08_review_sales =
                allSalesInYear
                        .join(salesWithViewedReviews)
                        .where(1)
                        .equalTo(0)
                        .with(new AllSalesLeftJoinSalesReviews())
                        .groupBy(0)
                        .aggregate(Aggregations.SUM, 1);

        DataSet<Tuple2<Double, Double>> result =
                q08_review_sales
                        .join(q08_all_sales)
                        .where(0)
                        .equalTo(0)
                        .with(new RSJoinAS());

        result.writeAsCsv(output_path, "\n", ",", FileSystem.WriteMode.OVERWRITE);

        env.execute("Query8");
    }


    // *************************************************************************
    //     DATA TRASFORMATIONS
    // *************************************************************************

    public static class WebClickMapping implements FlatMapFunction<ArrayList<String>, WebClick> {

        private WebClick tuple = new WebClick();

        @Override
        public void flatMap(ArrayList<String> t, Collector<WebClick> out) throws Exception {
            if(!(t.get(4).equals("") || t.get(5).equals(""))) {
                tuple.f0 = Long.valueOf(t.get(0)); tuple.f1 = Long.valueOf(t.get(1)); tuple.f2 = t.get(2); tuple.f3 = Long.valueOf(t.get(4)); tuple.f4 = Long.valueOf(t.get(5));
                out.collect(tuple);
            }
        }
    }

    //Filter DateDim between startDate && endDate
    public static class FilterDateDim implements FilterFunction<DateDim> {

        private DateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        @Override
        public boolean filter(DateDim dd) throws Exception {
            return !(format.parse(dd.f1).after(format.parse(endDate)) || format.parse(dd.f1).before(format.parse(startDate))) ;
        }
    }

    @FunctionAnnotation.ForwardedFieldsFirst("f0; f1; f2; f3; f4")
    public static class WebClickStreamLeftJoinDD implements JoinFunction<WebClick, Tuple1<Long>, WebClick> {

        private WebClick out = new WebClick();

        @Override
        public WebClick join(WebClick wc, Tuple1<Long> dd) throws Exception {
            out.f0 = wc.f0; out.f1 = wc.f1; out.f2 = wc.f2; out.f3 = wc.f3; out.f4 = wc.f4;
            return out;
        }
    }

    @FunctionAnnotation.ForwardedFieldsFirst("f4->f0; f2")
    @FunctionAnnotation.ForwardedFieldsSecond("f1->f3")
    public static class WebClickStreamJoinWebPage
            implements JoinFunction<WebClick, WebPage, Tuple4<Long, Long, String, String>> {

        private Tuple4<Long, Long, String, String> out = new Tuple4<>();

        @Override
        public Tuple4<Long, Long, String, String> join(WebClick wc, WebPage wp) throws Exception {
            out.f0 = wc.f4; out.f1 = wc.f0 * (long)86400 + wc.f1; out.f2 = wc.f2; out.f3 = wp.f1;
            return out;
        }
    }

    public static class FilterSalesReview implements GroupReduceFunction<Tuple4<Long, Long, String, String>, Tuple1<Long>> {

        private Tuple1<Long> tuple = new Tuple1<>();

        @Override
        public void reduce(Iterable<Tuple4<Long, Long, String, String>> in, Collector<Tuple1<Long>> out) throws Exception {
            Long current_key = (long)-1;
            Long last_review_date = (long)-1;
            Long last_sales_sk = null;

            for (Tuple4<Long, Long, String, String> curr : in) {
                if (!(current_key.equals(curr.f0))) {
                    current_key = curr.f0;
                    last_review_date = (long) -1;
                    last_sales_sk = null;
                }
                Long tstamp_inSec = (long) curr.f1;
                if (curr.f3.equals(web_page_type_filter)) {
                    last_review_date = tstamp_inSec;
                } else {
                    if (!curr.f2.equals("")) {    //wcs_sales_sk IS NOT NULL
                        if ((last_review_date > 0) && ((tstamp_inSec - last_review_date) <= q08_seconds_before_purchase)
                                && (last_sales_sk != Long.valueOf(curr.f2))) {
                            last_sales_sk = Long.valueOf(curr.f2);
                            tuple.f0 = Long.valueOf(curr.f2);
                            out.collect(tuple);
                        }
                    }
                }
            }
        }
    }

    @FunctionAnnotation.ForwardedFieldsFirst("f2->f0; f1")
    public static class WebSalesJoinDD implements JoinFunction<WebSales, Tuple1<Long>, Tuple2<Double, Long>> {

        private Tuple2<Double, Long> out = new Tuple2<>();

        @Override
        public Tuple2<Double, Long> join(WebSales ws, Tuple1<Long> dd) throws Exception {
            out.f0 = ws.f2; out.f1 = ws.f1;
            return out;
        }
    }

    public static class MyGroupReducer
            implements GroupReduceFunction<Tuple2<Double, Long>, Tuple2<Integer, Double>> {

        private Tuple2<Integer, Double> tuple = new Tuple2<>();

        @Override
        public void reduce(Iterable<Tuple2<Double, Long>> in, Collector<Tuple2<Integer, Double>> out) throws Exception {
            Double sum_ws_net_paid = 0.0;
            for (Tuple2<Double, Long> curr : in) {
                sum_ws_net_paid += curr.f0;
            }
            tuple.f0 = 1; tuple.f1 = sum_ws_net_paid;
            out.collect(tuple);
        }
    }

    @FunctionAnnotation.ForwardedFieldsFirst("f0->f1")
    public static class AllSalesLeftJoinSalesReviews implements JoinFunction<Tuple2<Double, Long>, Tuple1<Long>, Tuple2<Integer, Double>> {

        private Tuple2<Integer, Double> out = new Tuple2<>();

        @Override
        public Tuple2<Integer, Double> join(Tuple2<Double, Long> all, Tuple1<Long> rev) throws Exception {
            out.f0 = 1; out.f1 = all.f0;
            return out;
        }
    }

    public static class RSJoinAS implements JoinFunction<Tuple2<Integer, Double>, Tuple2<Integer, Double>, Tuple2<Double, Double>> {

        private Tuple2<Double, Double> out = new Tuple2<>();

        @Override
        public Tuple2<Double, Double> join(Tuple2<Integer, Double> rs, Tuple2<Integer, Double> as) throws Exception {
            out.f0 = rs.f1; out.f1 = as.f1 - rs.f1;
            return out;
        }
    }

    // *************************************************************************
    //     DATA TYPES
    // *************************************************************************

    public static class WebClick extends Tuple5<Long, Long, String, Long, Long> {

        public WebClick() {
        }

        public WebClick(Long wcs_click_date_sk, Long wcs_click_time_sk, String wcs_sales_sk, Long wcs_web_page_sk, Long wcs_user_sk) {
            this.f0 = wcs_click_date_sk;
            this.f1 = wcs_click_time_sk;
            this.f2 = wcs_sales_sk;
            this.f3 = wcs_web_page_sk;
            this.f4 = wcs_user_sk;
        }

        public Long getDate() {
            return this.f0;
        }

        public Long getTime() {
            return this.f1;
        }

        public String getSale() {
            return this.f2;
        }

        public Long getWebPage() {
            return this.f3;
        }

        public Long getUser() {
            return this.f4;
        }
    }

    public static class DateDim extends Tuple2<Long, String> {

        public Long getDateKey() {
            return this.f0;
        }

        public String getDate() {
            return this.f1;
        }
    }

    public static class WebPage extends Tuple2<Long, String> {

        public Long getPageKey() {
            return this.f0;
        }

        public String getType() {
            return this.f1;
        }
    }

    public static class WebSales extends Tuple3<Long, Long, Double> {

        public Long getDate() { return this.f0; }
        public Long getOrderNumber() { return this.f1; }
        public Double getNetPaid() { return this.f2; }
    }


    // *************************************************************************
    //     UTIL METHODS
    // *************************************************************************

    private static int parseParameters(String[] args){
        if(args.length == 3){
            input_path = args[0];
            output_path = args[1];
            return 0;
        }
        else{
            System.err.println("Usage: Each query needs 2 arguments.");
            return 1;
        }
    }

    private static DataSet<WebClick> getWebClickDataSet(ExecutionEnvironment env) {
        return env.readTextFile(web_clickstreams_path)
                .flatMap(new NullTokenizer())
                .flatMap(new WebClickMapping());
    }

    /* web_clickstreams -> wcs_item_sk, wcs_user_sk
        DataSet<String> web_clickstreams = env.readTextFile("/Users/gm/bigbench/data-generator/output/web_clickstreams.dat");
        //web_clickstreams.first(1).print();
        DataSet<Tuple6<String, String, String, String, String, String>> tuple = web_clickstreams.flatMap(new Tokenizer());*/

    private static DataSet<DateDim> getDateDimDataSet(ExecutionEnvironment env) {
        return env.readCsvFile(date_dim_path)
                .fieldDelimiter("|")
                .includeFields(q08_date_dim_mask)
                .tupleType(DateDim.class);
    }

    private static DataSet<WebPage> getWebPageDataSet(ExecutionEnvironment env) {
        return env.readCsvFile(web_page_path)
                .fieldDelimiter("|")
                .includeFields(q08_web_page_mask)
                .ignoreInvalidLines()
                .tupleType(WebPage.class);
    }

    private static DataSet<WebSales> getWebSalesDataSet(ExecutionEnvironment env) {
        return env.readCsvFile(web_sales_path)
                .fieldDelimiter("|")
                .includeFields(q08_web_sales_mask)
                .ignoreInvalidLines()
                .tupleType(WebSales.class);
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
