package de.tub.cs.bigbench;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.*;

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

import java.text.DateFormat;;
import java.text.SimpleDateFormat;
import java.lang.Math;


/**
 * Created by gm on 25/10/15.
 */
public class Query11 {

    public static String q11_product_reviews_mask;
    public static String q11_date_dim_mask;
    public static String q11_web_sales_mask;

    public static String input_path;
    public static String output_path;

    public static String product_reviews_path;
    public static String datedim_path;
    public static String websales_path;

    // Conf
    public static final String startDate = "2003-01-02";
    public static final String endDate = "2003-02-02";

    public static void main(String[] args) throws Exception {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        PropertiesConfiguration config = new PropertiesConfiguration("config.properties");

        if(parseParameters(args) == 1)
            return;

        q11_product_reviews_mask = config.getString("q11_product_reviews_mask");
        q11_date_dim_mask = config.getString("q11_date_dim_mask");
        q11_web_sales_mask = config.getString("q11_web_sales_mask");

        product_reviews_path = input_path + "/product_reviews/product_reviews.dat";
        datedim_path = input_path + "/date_dim/date_dim.dat";
        websales_path = input_path + "/web_sales/web_sales.dat";


        // get input data
        //product_reviews -> pr_review_rating (Integer), pr_item_sk (Long)
        DataSet<ProdReviews> product_reviews = getProdReviewsDataSet(env);
        DataSet<DateDim> date_dim = getDateDimDataSet(env);
        //web_sales -> ws_item_sk, ws_sold_date_sk, ws_net_paid
        DataSet<WebSales> web_sales = getWebSalesDataSet(env);

        DataSet<Tuple3<Long, Integer, Double>> p =
            product_reviews
                .map(new Ones())
                .groupBy(1)
                .aggregate(Aggregations.SUM, 2)
                .and(Aggregations.SUM, 0)
                .map(new GetP());

        DataSet<Tuple1<Long>> dd =
            date_dim
                .filter(new FilterDateDim())
                .project(0);

        DataSet<Tuple2<Long, Double>> s =
            web_sales
                .joinWithTiny(dd)
                .where(0)
                .equalTo(0)
                .with(new WsJoinDateDim())
                .groupBy(0)
                .aggregate(Aggregations.SUM, 1)
                .sortPartition(0, Order.ASCENDING).setParallelism(1);

        DataSet<Tuple3<Integer, Integer, Double>> q11_review_stats_tmp =
            p
                .joinWithTiny(s)
                .where(0)
                .equalTo(0)
                .with(new PJoinS());

        DataSet<Tuple3<Integer, Double, Double>> means =
            q11_review_stats_tmp
                .groupBy(0)
                .reduceGroup(new Reducer());

        DataSet<Tuple1<Double>> result =
            q11_review_stats_tmp
                .joinWithTiny(means)
                .where(0)
                .equalTo(0)
                .with(new CorrJoinMeans())
                .aggregate(Aggregations.SUM, 0)
                .and(Aggregations.SUM, 1)
                .and(Aggregations.SUM, 2)
                .map(new Correlation());

        result.writeAsCsv(output_path, "\n", ",", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        //env.execute();
        env.execute("Query11");

    }

    // *************************************************************************
    //     DATA TRASFORMATIONS
    // *************************************************************************

    public static class FilterDateDim implements FilterFunction<DateDim> {

        private DateFormat format = new SimpleDateFormat("yyyy-MM-dd");

        @Override
        public boolean filter(DateDim dd) throws Exception {
            return !(format.parse(dd.f1).after(format.parse(endDate)) || format.parse(dd.f1).before(format.parse(startDate))) ;
        }
    }

    @FunctionAnnotation.ForwardedFields("f0; f1")
    public static class Ones implements MapFunction<ProdReviews, Tuple3<Integer, Long, Integer>> {

        private Tuple3<Integer, Long, Integer> out = new Tuple3<>();

        @Override
        public Tuple3<Integer, Long, Integer> map(ProdReviews pr) throws Exception {
            out.f0 = pr.f0; out.f1 = pr.f1; out.f2 = 1;
            return out;
        }
    }

    @FunctionAnnotation.ForwardedFields("f1->f0")
    public static class GetP implements MapFunction<Tuple3<Integer, Long, Integer>, Tuple3<Long, Integer, Double>> {

        private Tuple3<Long, Integer, Double> out = new Tuple3<>();

        @Override
        public Tuple3<Long, Integer, Double> map(Tuple3<Integer, Long, Integer> row) throws Exception {
            out.f0 = row.f1; out.f1 = row.f2; out.f2 = (double)row.f0 / row.f2;
            return out;
        }
    }

    @FunctionAnnotation.ForwardedFieldsFirst("f1->f0; f2->f1")
    public static class WsJoinDateDim implements JoinFunction<WebSales, Tuple1<Long>, Tuple2<Long, Double>>{

        private Tuple2<Long, Double> out = new Tuple2<>();

        @Override
        public Tuple2<Long, Double> join(WebSales ws, Tuple1<Long> dd) throws Exception {
            out.f0 = ws.f1; out.f1 = ws.f2;
            return out;
        }
    }

    @FunctionAnnotation.ForwardedFieldsFirst("f1; f2")
    public static class PJoinS
            implements JoinFunction<Tuple3<Long, Integer, Double>, Tuple2<Long, Double>, Tuple3<Integer, Integer, Double>>{

        private Tuple3<Integer, Integer, Double> out = new Tuple3<>();

        @Override
        public Tuple3<Integer, Integer, Double> join(Tuple3<Long, Integer, Double> p, Tuple2<Long, Double> s) throws Exception {
            out.f0 = 1; out.f1 = p.f1; out.f2 = p.f2;
            return out;
        }
    }

    public static class Reducer implements GroupReduceFunction<Tuple3<Integer, Integer, Double>, Tuple3<Integer, Double, Double>> {

        private Tuple3<Integer, Double, Double> tuple = new Tuple3<>();

        @Override
        public void reduce(Iterable<Tuple3<Integer, Integer, Double>> rows, Collector<Tuple3<Integer, Double, Double>> out) throws Exception {
            Double sum_x = 0.0;
            Double sum_y = 0.0;
            Integer count = 0;

            for (Tuple3<Integer, Integer, Double> curr : rows) {
                sum_x += (double)curr.f1;
                sum_y += curr.f2;
                count++;
            }
            tuple.f0 = 1; tuple.f1 = sum_x / (double) count; tuple.f2 = sum_y / (double) count;
            out.collect(tuple);
        }
    }

    public static class CorrJoinMeans implements JoinFunction<Tuple3<Integer, Integer, Double>, Tuple3<Integer, Double, Double>, Tuple3<Double, Double, Double>>{

        private Tuple3<Double, Double, Double> tuple = new Tuple3<>();

        @Override
        public Tuple3<Double, Double, Double> join(Tuple3<Integer, Integer, Double> corr, Tuple3<Integer, Double, Double> means) throws Exception {
            tuple.f0 = ((double)corr.f1 - means.f1) * (corr.f2 - means.f2);
            tuple.f1 = ((double)corr.f1 - means.f1) * ((double)corr.f1 - means.f1);
            tuple.f2 = (corr.f2 - means.f2) * (corr.f2 - means.f2);
            return tuple;
        }
    }

    public static class Correlation implements MapFunction<Tuple3<Double, Double, Double>, Tuple1<Double>> {

        private Tuple1<Double> out = new Tuple1<>();

        @Override
        public Tuple1<Double> map(Tuple3<Double, Double, Double> row) throws Exception {
            out.f0 = row.f0 / Math.sqrt(row.f1 * row.f2);
            return out;
        }
    }

    // *************************************************************************
    //     DATA TYPES
    // *************************************************************************

    public static class ProdReviews extends Tuple2<Integer, Long> {

        public Integer getRating() { return this.f0; }
        public Long getItem() { return this.f1; }
    }

    public static class DateDim extends Tuple2<Long, String> {

        public Long getDateKey() { return this.f0; }
        public String getDate() { return this.f1; }
    }

    public static class WebSales extends Tuple3<Long, Long, Double> {

        public Long getDate() { return this.f0; }
        public Long getItem() { return this.f1; }
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

    private static DataSet<ProdReviews> getProdReviewsDataSet(ExecutionEnvironment env) {
        return env.readCsvFile(product_reviews_path)
                .fieldDelimiter("|")
                .includeFields(q11_product_reviews_mask)
                .ignoreInvalidLines()
                .tupleType(ProdReviews.class);
    }

    private static DataSet<DateDim> getDateDimDataSet(ExecutionEnvironment env) {
        return env.readCsvFile(datedim_path)
                .fieldDelimiter("|")
                .includeFields(q11_date_dim_mask)
                .tupleType(DateDim.class);
    }

    private static DataSet<WebSales> getWebSalesDataSet(ExecutionEnvironment env) {
        return env.readCsvFile(websales_path)
                .fieldDelimiter("|")
                .includeFields(q11_web_sales_mask)
                .ignoreInvalidLines()
                .tupleType(WebSales.class);
    }
}
