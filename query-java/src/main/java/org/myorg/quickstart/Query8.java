package org.myorg.quickstart;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * Created by gm on 26/10/15.
 */
public class Query8 {
    public static final String web_clickstreams_mask = "111011";
    public static final String date_dim_mask = "1010000000000000000000000000";
    public static final String web_page_mask = "10000000010000";
    public static final String web_sales_mask = "1000000000000000010000000000010000";

    public static final String web_clickstreams_path = "/Users/gm/bigbench/data-generator/output/web_clickstreams.dat";
    public static final String date_dim_path = "/Users/gm/bigbench/data-generator/output/date_dim.dat";
    public static final String web_page_path = "/Users/gm/bigbench/data-generator/output/web_page.dat";
    public static final String web_sales_path = "/Users/gm/bigbench/data-generator/output/web_sales.dat";

    // Conf
    public static final String startDate = "2001-09-02";
    public static final String endDate = "2002-09-02";
    public static final Long q08_seconds_before_purchase = (long)259200;   // 3 days in sec = 3*24*60*60

    public static final String web_page_type_filter = "review";

    public static void main(String[] args) throws Exception {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

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
                .join(dd)
                .where(0)
                .equalTo(0)
                .with(new WebClickStreamLeftJoinDD())
                .join(web_page)
                .where(3)
                .equalTo(0)
                .with(new WebClickStreamJoinWebPage())
        //        .partitionByHash(4)
        //        .mapPartition(new PartitionMapper())
                .sortPartition(0, Order.ASCENDING).setParallelism(1)
                .sortPartition(1, Order.ASCENDING).setParallelism(1)
                .sortPartition(2, Order.ASCENDING).setParallelism(1)
                .sortPartition(3, Order.ASCENDING).setParallelism(1);
        //        .writeAsCsv("/Users/gm/bigbench/data-generator/output/q8_tmp1.csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        DataSet<Tuple1<Long>> salesWithViewedReviews =
        q08_map_output
                .reduceGroup(new FilterSalesReview());

        DataSet<Tuple2<Double, Long>> allSalesInYear =
        web_sales
                .join(dd)
                .where(0)
                .equalTo(0)
                .with(new WebSalesJoinDD());

        DataSet<Tuple1<Double>> q08_all_sales =
        allSalesInYear
                .aggregate(Aggregations.SUM, 0)
                .project(0);

        q08_all_sales.print();


        DataSet<Tuple1<Double>> q08_review_sales =
        allSalesInYear
                .join(salesWithViewedReviews)
                .where(1)
                .equalTo(0)
                .with(new AllSalesLeftJoinSalesReviews())
                .aggregate(Aggregations.SUM, 0)
                .project(0);

                //writeAsCsv("/Users/gm/bigbench/data-generator/output/q8_tmp.csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);


        //sales_which_read_reviews.writeAsCsv("/Users/gm/bigbench/data-generator/output/q8_tmp.csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        q08_review_sales.print();


        //env.execute();

        /*DataSet<Tuple4<Long, Integer, Double, Double>> q11_review_stats =
        p
                .join(s)
                .where(0)
                .equalTo(0).print();
                //.with(new PJoinS());


        //q11_review_stats.writeAsCsv("/tmp/q11.csv").setParallelism(1);

        //DataSet<Tuple1<Double>> avg_rating = q11_review_stats.project(2);
        //avg_rating.print();

        /*
        List<Tuple2<String, Integer>> r_count = new ArrayList<Tuple2<String, Integer>>();
        myResult.output(new LocalCollectionOutputFormat(outData));
        PearsonsCorrelation PC = new PearsonsCorrelation();
        Double cor = PC.correlation(q11_review_stats.project(1), q11_review_stats.project(2));
        */

        // emit result
        //result.writeAsCsv(outputPath, "\n", "|");

        // execute program


    }


    // *************************************************************************
    //     DATA TRASFORMATIONS
    // *************************************************************************

    public static class WebClickMapping implements FlatMapFunction<ArrayList<String>, WebClick> {
        @Override
        public void flatMap(ArrayList<String> t, Collector<WebClick> out) throws Exception {
            if(!(t.get(4).equals("") || t.get(5).equals("")))
                out.collect(new WebClick(Long.valueOf(t.get(0)), Long.valueOf(t.get(1)), t.get(2), Long.valueOf(t.get(4)), Long.valueOf(t.get(5))));
        }
    }

    //Filter DateDim between startDate && endDate
    public static class FilterDateDim implements FilterFunction<DateDim> {

        @Override
        public boolean filter(DateDim dd) throws Exception {

            DateFormat format = new SimpleDateFormat("yyyy-MM-dd");

            return !(format.parse(dd.getDate()).after(format.parse(endDate)) || format.parse(dd.getDate()).before(format.parse(startDate))) ;
        }
    }

    public static class WebClickStreamLeftJoinDD implements JoinFunction<WebClick, Tuple1<Long>, WebClick> {
        @Override
        public WebClick join(WebClick wc, Tuple1<Long> dd) throws Exception {
            return new WebClick(wc.f0, wc.f1, wc.f2, wc.f3, wc.f4);
        }
    }

    public static class WebClickStreamJoinWebPage
            implements JoinFunction<WebClick, WebPage, Tuple4<Long, Long, String, String>> {
        //wcs_user_sk,
        //(wcs_click_date_sk * 86400L + wcs_click_time_sk) AS tstamp_inSec
        //        wcs_sales_sk, wp_type

        @Override
        public Tuple4<Long, Long, String, String> join(WebClick wc, WebPage wp) throws Exception {
            return new Tuple4<>(wc.getUser(), wc.getDate() * (long)86400 + wc.getTime(), wc.getSale(), wp.getType());
        }
    }

    public static class FilterSalesReview implements GroupReduceFunction<Tuple4<Long, Long, String, String>, Tuple1<Long>> {
        @Override
        public void reduce(Iterable<Tuple4<Long, Long, String, String>> in, Collector<Tuple1<Long>> out) throws Exception {
            ArrayList<Long> uniqSales = new ArrayList<Long>();
            Long current_key = null;
            Long last_review_date = (long)-1;
            Long last_sales_sk = null;
            Long tstamp_inSec = null;

            for (Tuple4<Long, Long, String, String> curr : in) {
                if (current_key != curr.f0) {
                    current_key = curr.f0;
                    last_review_date = (long) -1;
                    last_sales_sk = null;
                }
                tstamp_inSec = (long) curr.f1;
                if (curr.f3.equals(web_page_type_filter)) {
                    last_review_date = tstamp_inSec;
                } else {
                    if (!curr.f2.equals("")) {    //wcs_sales_sk IS NOT NULL
                        if ((last_review_date > 0) && ((tstamp_inSec - last_review_date) <= q08_seconds_before_purchase)
                                && (last_sales_sk != Long.valueOf(curr.f2))) {
                            last_sales_sk = Long.valueOf(curr.f2);
                            if(!(uniqSales.contains(Long.valueOf(curr.f2)))){
                                out.collect(new Tuple1<>(Long.valueOf(curr.f2)));
                                uniqSales.add(Long.valueOf(curr.f2));
                            }
                        }
                    }
                }
            }
        }
    }

    public static class WebSalesJoinDD implements JoinFunction<WebSales, Tuple1<Long>, Tuple2<Double, Long>> {
        @Override
        public Tuple2<Double, Long> join(WebSales ws, Tuple1<Long> dd) throws Exception {
            return new Tuple2<>(ws.getNetPaid(), ws.getOrderNumber());
        }
    }

    public static class MyGroupReducer
            implements GroupReduceFunction<Tuple3<Integer, Long, Double>, Tuple5<Integer, Long, Double, Double, Long>> {
        //SELECT  wcs_user_sk,(wcs_click_date_sk * 86400L + wcs_click_time_sk) AS tstamp_inSec,
        // --every wcs_click_date_sk equals
        // one day => convert to seconds date*24*60*60=date*86400 and add time_sk wcs_sales_sk, wp_type
        @Override
        public void reduce(Iterable<Tuple3<Integer, Long, Double>> rows, Collector<Tuple5<Integer, Long, Double, Double, Long>> out) throws Exception {
            Integer cat = null;
            Long x = null;
            Double y = 0.0;

            for (Tuple3<Integer, Long, Double> curr : rows) {
                cat = curr.f0;
                x = curr.f1;
                y += curr.f2;
            }
            out.collect(new Tuple5<>(cat, x, y, x * y, x * x));
        }
    }

    public static class AllSalesLeftJoinSalesReviews implements JoinFunction<Tuple2<Double, Long>, Tuple1<Long>, Tuple2<Double, Long>> {
        @Override
        public Tuple2<Double, Long> join(Tuple2<Double, Long> all, Tuple1<Long> rev) throws Exception {
            return new Tuple2<>(all.f0, all.f1);
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

    /*
    private static DataSet<WebClick> getWebClickDataSet(ExecutionEnvironment env) {
        return env.readCsvFile(web_clickstreams_path)
                .fieldDelimiter("|")
                .includeFields(web_clickstreams_mask)
                .ignoreInvalidLines()
                .tupleType(WebClick.class);
    }
    */

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
                .includeFields(date_dim_mask)
                .tupleType(DateDim.class);
    }

    private static DataSet<WebPage> getWebPageDataSet(ExecutionEnvironment env) {
        return env.readCsvFile(web_page_path)
                .fieldDelimiter("|")
                .includeFields(web_page_mask)
                .ignoreInvalidLines()
                .tupleType(WebPage.class);
    }

    private static DataSet<WebSales> getWebSalesDataSet(ExecutionEnvironment env) {
        return env.readCsvFile(web_sales_path)
                .fieldDelimiter("|")
                .includeFields(web_sales_mask)
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