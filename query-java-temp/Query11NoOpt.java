package de.tub.cs.bigbench;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

import java.text.DateFormat;
import java.text.SimpleDateFormat;

/**
 * Created by gm on 02/12/15.
 */
public class Query11NoOpt {
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
                        .join(dd)
                        .where(0)
                        .equalTo(0)
                        .with(new DateDimJoinWs())
                        .groupBy(0)
                        .aggregate(Aggregations.SUM, 1)
                        .sortPartition(0, Order.ASCENDING).setParallelism(1);

        DataSet<Tuple3<Integer, Integer, Double>> q11_review_stats_tmp =
                p
                        .join(s)
                        .where(0)
                        .equalTo(0)
                        .with(new PJoinS2());

        DataSet<Tuple3<Integer, Double, Double>> means =
                q11_review_stats_tmp
                        .groupBy(0)
                        .reduceGroup(new Reducer());

        DataSet<Tuple1<Double>> result =
                q11_review_stats_tmp
                        .join(means)
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

    // Filter DateDim between startDate && endDate
    public static class FilterDateDim implements FilterFunction<DateDim> {

        @Override
        public boolean filter(DateDim dd) throws Exception {

            DateFormat format = new SimpleDateFormat("yyyy-MM-dd");

            return !(format.parse(dd.getDate()).after(format.parse(endDate)) || format.parse(dd.getDate()).before(format.parse(startDate))) ;
        }
    }

    public static class Ones implements MapFunction<ProdReviews, Tuple3<Integer, Long, Integer>> {
        @Override
        public Tuple3<Integer, Long, Integer> map(ProdReviews pr) throws Exception {
            return new Tuple3<>(pr.f0, pr.f1, 1);
        }
    }

    public static class GetP implements MapFunction<Tuple3<Integer, Long, Integer>, Tuple3<Long, Integer, Double>> {
        @Override
        public Tuple3<Long, Integer, Double> map(Tuple3<Integer, Long, Integer> row) throws Exception {
            return new Tuple3<>(row.f1, row.f2, ((double)row.f0 / row.f2));
        }
    }

    public static class DateDimJoinWs implements JoinFunction<WebSales, Tuple1<Long>, Tuple2<Long, Double>> {

        @Override
        public Tuple2<Long, Double> join(WebSales ws, Tuple1<Long> dd) throws Exception {
            return new Tuple2<>(ws.f1, ws.f2);
        }
    }

    public static class PJoinS
            implements JoinFunction<Tuple3<Long, Integer, Double>, Tuple2<Long, Double>, Tuple4<Long, Integer, Double, Double>>{

        @Override
        public Tuple4<Long, Integer, Double, Double> join(Tuple3<Long, Integer, Double> p, Tuple2<Long, Double> s) throws Exception {
            return new Tuple4<>(p.f0, p.f1, p.f2, s.f1);
        }
    }

    public static class PJoinS2
            implements JoinFunction<Tuple3<Long, Integer, Double>, Tuple2<Long, Double>, Tuple3<Integer, Integer, Double>>{

        @Override
        public Tuple3<Integer, Integer, Double> join(Tuple3<Long, Integer, Double> p, Tuple2<Long, Double> s) throws Exception {
            return new Tuple3<>(1, p.f1, p.f2);
        }
    }

    public static class Reducer implements GroupReduceFunction<Tuple3<Integer, Integer, Double>, Tuple3<Integer, Double, Double>> {

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
            out.collect(new Tuple3<>(1, sum_x / (double) count, sum_y / (double) count));
        }
    }

    public static class CorrJoinMeans implements JoinFunction<Tuple3<Integer, Integer, Double>, Tuple3<Integer, Double, Double>, Tuple3<Double, Double, Double>>{

        @Override
        public Tuple3<Double, Double, Double> join(Tuple3<Integer, Integer, Double> corr, Tuple3<Integer, Double, Double> means) throws Exception {
            return new Tuple3<>(((double)corr.f1 - means.f1) * (corr.f2 - means.f2), ((double)corr.f1 - means.f1) * ((double)corr.f1 - means.f1), (corr.f2 - means.f2) * (corr.f2 - means.f2));
        }
    }

    public static class Correlation implements MapFunction<Tuple3<Double, Double, Double>, Tuple1<Double>> {
        @Override
        public Tuple1<Double> map(Tuple3<Double, Double, Double> row) throws Exception {
            return new Tuple1<>(row.f0 / Math.sqrt(row.f1 * row.f2));
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
        if(args.length == 2){
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
