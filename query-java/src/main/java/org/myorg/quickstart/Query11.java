package org.myorg.quickstart;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.tuple.*;

import org.apache.flink.util.Collector;

import java.text.DateFormat;;
import java.text.SimpleDateFormat;
import java.lang.Math;


/**
 * Created by gm on 25/10/15.
 */
public class Query11 {

    public static final String product_reviews_mask = "00011000";
    public static final String date_dim_mask = "1010000000000000000000000000";
    public static final String web_sales_mask = "1001000000000000000000000000010000";

    public static final String product_reviews_path = "/Users/gm/bigbench/data-generator/output/product_reviews.dat";
    public static final String datedim_path = "/Users/gm/bigbench/data-generator/output/date_dim.dat";
    public static final String websales_path = "/Users/gm/bigbench/data-generator/output/web_sales.dat";

    // Conf
    public static final String startDate = "2003-01-02";
    public static final String endDate = "2003-02-02";

    public static void main(String[] args) throws Exception {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // get input data
        //product_reviews -> pr_review_rating (Integer), pr_item_sk (Long)
        DataSet<ProdReviews> product_reviews = getProdReviewsDataSet(env);
        DataSet<DateDim> date_dim = getDateDimDataSet(env);
        //web_sales -> ws_item_sk, ws_sold_date_sk, ws_net_paid
        DataSet<WebSales> web_sales = getWebSalesDataSet(env);

        DataSet<Tuple3<Long, Integer, Double>> p =
                product_reviews
                .map(new Ones())
                .groupBy(1)        // group on pr_item_sk
                .aggregate(Aggregations.SUM, 2) // compute count on ones
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

        /*
        DataSet<Tuple4<Long, Integer, Double, Double>> q11_review_stats =
        p
                .join(s)
                .where(0)
                .equalTo(0)
                .with(new PJoinS());
        */
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

        q11_review_stats_tmp
                .join(means)
                .where(0)
                .equalTo(0)
                .with(new CorrJoinMeans())
                .aggregate(Aggregations.SUM, 0)
                .and(Aggregations.SUM, 1)
                .and(Aggregations.SUM, 2)
                .map(new Correlation()).print();

        //env.execute();

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

    public static class DateDimJoinWs implements JoinFunction<WebSales, Tuple1<Long>, Tuple2<Long, Double>>{

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

    public static class Correlation implements MapFunction<Tuple3<Double, Double, Double>, Double> {
        @Override
        public Double map(Tuple3<Double, Double, Double> row) throws Exception {
            return row.f0 / Math.sqrt(row.f1 * row.f2);
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

    private static DataSet<ProdReviews> getProdReviewsDataSet(ExecutionEnvironment env) {
        return env.readCsvFile(product_reviews_path)
                .fieldDelimiter("|")
                .includeFields(product_reviews_mask)
                .ignoreInvalidLines()
                .tupleType(ProdReviews.class);
    }

    private static DataSet<DateDim> getDateDimDataSet(ExecutionEnvironment env) {
        return env.readCsvFile(datedim_path)
                .fieldDelimiter("|")
                .includeFields(date_dim_mask)
                .tupleType(DateDim.class);
    }

    private static DataSet<WebSales> getWebSalesDataSet(ExecutionEnvironment env) {
        return env.readCsvFile(websales_path)
                .fieldDelimiter("|")
                .includeFields(web_sales_mask)
                .ignoreInvalidLines()
                .tupleType(WebSales.class);
    }

}
