package org.myorg.quickstart;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.util.Collector;

import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;

/**
 * Created by gm on 26/10/15.
 */
public class Query15 {
    public static final String store_sales_mask = "10100001000000000000100";
    public static final String date_dim_mask = "1010000000000000000000000000";
    public static final String items_mask = "1000000000010000000000";

    public static final String store_sales_path = "/Users/gm/bigbench/data-generator/output/store_sales.dat";
    public static final String date_dim_path= "/Users/gm/bigbench/data-generator/output/date_dim.dat";
    public static final String items_path = "/Users/gm/bigbench/data-generator/output/item.dat";

    // Conf
    public static final String startDate = "2001-09-02";
    public static final String endDate = "2002-09-02";
    public static final Integer q15_store_sk = 10;

    public static void main(String[] args) throws Exception {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // get input data
        //store_sales -> ss_sold_date_sk (Long), s.ss_item_sk (Long), ss_store_sk (Long), ss_net_paid (Double)
        DataSet<StoreSales> store_sales = getStoreSalesDataSet(env);
        //date_dim -> d_date_sk, d.d_date
        DataSet<DateDim> date_dim = getDateDimDataSet(env);
        //items -> i_item_sk (Long) i_category_id (Integer)
        DataSet<Item> items = getItemDataSet(env);

        DataSet<Tuple1<Long>> dd = date_dim.project(0);

        DataSet<Tuple3<Integer, Long, Double>> t =
            store_sales
                .join(dd)
                .where(0)
                .equalTo(0)
                .with(new StoreSalesLeftJoinDD())
                .join(items)
                .where(1)       //store_sales -> ss_item_sk
                .equalTo(0)     //items -> i_item_sk
                .with(new StoreSalesJoinItem())
                .project(4, 0, 3);

        t
                .groupBy(0, 1)  //GROUP BY i.i_category_id, s.ss_sold_date_sk
                .aggregate(Aggregations.SUM, 2)
                .map(new Temp())
                .groupBy(0)
                .reduceGroup(new Reducer())
                .filter(new SlopeFilter())
                .sortPartition(0, Order.ASCENDING).setParallelism(1);

        env.execute();
    }


    // *************************************************************************
    //     DATA TRASFORMATIONS
    // *************************************************************************

    //Filter DateDim between startDate && endDate
    public static class FilterDateDim implements FilterFunction<DateDim> {

        @Override
        public boolean filter(DateDim dd) throws Exception {

            DateFormat format = new SimpleDateFormat("yyyy-MM-dd");

            return !(format.parse(dd.getDate()).after(format.parse(endDate)) || format.parse(dd.getDate()).before(format.parse(startDate))) ;
        }
    }

    public static class StoreSalesLeftJoinDD implements JoinFunction<StoreSales, Tuple1<Long>, StoreSales> {
        @Override
        public StoreSales join(StoreSales ss, Tuple1<Long> dd) throws Exception {
            return new StoreSales(ss.f0, ss.f1, ss.f2, ss.f3);
        }
    }

    //Filter StoreSales on ss_store_sk
    public static class StoreFilter implements FilterFunction<StoreSales> {

        @Override
        public boolean filter(StoreSales ss) throws Exception {
            return ss.getStore().equals((long) q15_store_sk);
        }
    }

    public static class StoreSalesJoinItem
            implements JoinFunction<StoreSales, Item, Tuple5<Long, Long, Long, Double, Integer>>{

        @Override
        public Tuple5<Long, Long, Long, Double, Integer> join(StoreSales ss, Item i) throws Exception {
            return new Tuple5<>(ss.f0, ss.f1, ss.f2, ss.f3, i.f1);
        }
    }

    public static class Temp implements MapFunction<Tuple3<Integer, Long, Double>, Tuple5<Integer, Long, Double, Double, Long>> {
        @Override
        public Tuple5<Integer, Long, Double, Double, Long> map(Tuple3<Integer, Long, Double> t) throws Exception {
            return new Tuple5<>(t.f0, t.f1, t.f2, t.f1 * t.f2, t.f1 * t.f1);
        }
    }

    public static class Reducer implements GroupReduceFunction<Tuple5<Integer, Long, Double, Double, Long>, Tuple3<Integer, Double, Double>> {
        //((count(x) * SUM(xy) - SUM(x) * SUM(y)) / (count(x) * SUM(xx) - SUM(x) * SUM(x)) ) AS slope,
        //(SUM(y) - ((count(x) * SUM(xy) - SUM(x) * SUM(y)) / (count(x) * SUM(xx) - SUM(x)*SUM(x)) ) * SUM(x)) / count(x)
        @Override
        public void reduce(Iterable<Tuple5<Integer, Long, Double, Double, Long>> in, Collector<Tuple3<Integer, Double, Double>> out) throws Exception {
            Integer cat = null;
            Integer count_x = 0;
            Double sum_x = 0.0;
            Double sum_y = 0.0;
            Double sum_xy = 0.0;
            Double sum_xx = 0.0;

            for (Tuple5<Integer, Long, Double, Double, Long>curr : in) {
                cat = curr.f0;
                sum_x += curr.f1;
                sum_y += curr.f2;
                sum_xy += curr.f3;
                sum_xx += curr.f4;
                count_x++;
            }
            out.collect(new Tuple3<>(cat, ((count_x * sum_xy - sum_x * sum_y) / (count_x * sum_xx - sum_x * sum_x) ),
                    (sum_y - ((count_x * sum_xy - sum_x * sum_y) / (count_x * sum_xx - sum_x * sum_x) ) * sum_x) / count_x));
        }
    }

    public static class SlopeFilter implements FilterFunction<Tuple3<Integer, Double, Double>> {

        @Override
        public boolean filter(Tuple3<Integer, Double, Double> row) throws Exception {
            return row.f1 <= 0;
        }
    }

    // *************************************************************************
    //     DATA TYPES
    // *************************************************************************

    public static class StoreSales extends Tuple4<Long, Long, Long, Double> {

        public StoreSales() { }

        public StoreSales(Long ss_sold_date_sk, Long ss_item_sk, Long ss_store_sk, Double ss_net_paid) {
            this.f0 = ss_sold_date_sk;
            this.f1 = ss_item_sk;
            this.f2 = ss_store_sk;
            this.f3 = ss_net_paid;
        }

        public Long getDate() { return this.f0; }
        public Long getItem() { return this.f1; }
        public Long getStore() { return this.f2; }
        public Double getNetPaid() { return this.f3; }
    }

    public static class DateDim extends Tuple2<Long, String> {

        public Long getDateKey() { return this.f0; }
        public String getDate() { return this.f1; }
    }

    public static class Item extends Tuple2<Long, Integer> {

        public Long getItem() { return this.f0; }
        public Integer getCategory() { return this.f1; }
    }


    // *************************************************************************
    //     UTIL METHODS
    // *************************************************************************

    private static DataSet<StoreSales> getStoreSalesDataSet(ExecutionEnvironment env) {
        return env.readCsvFile(store_sales_path)
                .fieldDelimiter("|")
                .includeFields(store_sales_mask)
                .ignoreInvalidLines()
                .tupleType(StoreSales.class)
                .filter(new StoreFilter());
    }

    private static DataSet<DateDim> getDateDimDataSet(ExecutionEnvironment env) {
        return env.readCsvFile(date_dim_path)
                .fieldDelimiter("|")
                .includeFields(date_dim_mask)
                .tupleType(DateDim.class)
                .filter(new FilterDateDim());
    }

    private static DataSet<Item> getItemDataSet(ExecutionEnvironment env) {
        return env.readCsvFile(items_path)
                .fieldDelimiter("|")
                .includeFields(items_mask)
                .ignoreInvalidLines()
                .tupleType(Item.class);
    }
}
