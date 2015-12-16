package org.myorg.quickstart;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.util.Collector;

import java.text.DateFormat;
import java.text.SimpleDateFormat;

/**
 * Created by gm on 06/11/15.
 */
public class Query23 {
    public static final String inventory_mask = "1111";
    public static final String date_dim_mask = "1000001010000000000000000000";

    public static final String inventory_path = "hdfs://localhost:9000/Users/gm/bigbench/data-generator/output/inventory.dat";
    public static final String date_dim_path= "/Users/gm/bigbench/data-generator/output/date_dim.dat";

    // Conf
    public static final Integer q23_year = 2001;
    public static final Integer q23_month = 1;
    public static final Double q23_coefficient = 1.3;

    public static void main(String[] args) throws Exception {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // get input data
        //inventory -> inv_date_sk (long), inv_item_sk (Long). inv_warehouse_sk (Long), inv_quantity_on_hand (Long)
        DataSet<Inventory> inventory = getInventoryDataSet(env);
        //date_dim -> d_date_sk, d.d_date
        DataSet<DateDim> date_dim = getDateDimDataSet(env);

        inventory.print();

        /*
        DataSet<Tuple4<Long, Long, Integer, Integer>> t =
                inventory
                        .join(date_dim)
                        .where(0)
                        .equalTo(0)
                        .with(new InventoryJoinDateDim());
                        */
/*
        t
            .groupBy(0, 1)  //GROUP BY i.i_category_id, s.ss_sold_date_sk
            .aggregate(Aggregations.SUM, 2)
            .map(new Computation()).writeAsCsv("/tmp/q15_1.csv").setParallelism(1);
        //        .groupBy(0)
        //        .reduceGroup(new MyGroupReducer())
        //        .filter(new SlopeFilter());
        //        .print();
        */

        //    .groupBy(0)
        //    .reduceGroup(new MyGroupReducer3())
        //        .filter(new SlopeFilter());
        //    .print();




        //    env.execute();

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

    //Filter DateDim above year and month
    public static class FilterDateDim implements FilterFunction<DateDim> {
        // AND d.d_year = ${hiveconf:q23_year} AND d_moy between ${hiveconf:q23_month} AND (${hiveconf:q23_month} + 1)
        @Override
        public boolean filter(DateDim dd) throws Exception {
            return dd.getYear().equals(q23_year) && (dd.getMonth().equals(q23_month)||dd.getMonth().equals(q23_month+1));
        }
    }

    public static class InventoryJoinDateDim implements JoinFunction<Inventory, DateDim, Tuple4<Long, Long, Integer, Integer>> {
        @Override
        public Tuple4<Long, Long, Integer, Integer> join(Inventory inventory, DateDim dateDim) throws Exception {
            return null;
        }
    }

    public static class Computation implements MapFunction<Tuple3<Integer, Long, Double>, Tuple5<Integer, Long, Double, Double, Long>> {
        @Override
        public Tuple5<Integer, Long, Double, Double, Long> map(Tuple3<Integer, Long, Double> row) throws Exception {
            return new Tuple5<>(row.f0, row.f1, row.f2, row.f1 * row.f2, row.f1 * row.f1);
        }
    }

    public static class MyGroupReducer2
            implements GroupReduceFunction<Tuple3<Integer, Long, Double>,  Tuple5<Integer, Long, Double, Double, Long>> {
        @Override
        public void reduce(Iterable<Tuple3<Integer, Long, Double>> rows, Collector<Tuple5<Integer, Long, Double, Double, Long>> out) throws Exception {
            Integer cat = null; //category
            Long x = (long)0;   //date
            Double y = 0.0;     //sum(net_paid)
            Double xy = 0.0;
            Long xx = (long)0;

            for (Tuple3<Integer, Long, Double> curr : rows) {
                cat = curr.f0;
                x = curr.f1;
                y += curr.f2;
            }
            out.collect(new Tuple5<>(cat, x, y, x * y, x * x));
        }
    }

    public static class MyGroupReducer3
            implements GroupReduceFunction<Tuple5<Integer, Long, Double, Double, Long>,  Tuple3<Integer, Double, Double>> {
        @Override
        public void reduce(Iterable<Tuple5<Integer, Long, Double, Double, Long>> rows, Collector<Tuple3<Integer, Double, Double>> out) throws Exception {
            Integer cat = null; //category
            Integer count_x = 0;
            Long sum_x = (long)0;
            Double sum_y = 0.0;
            Double sum_xy = 0.0;
            Long sum_xx = (long)0;

            for (Tuple5<Integer, Long, Double, Double, Long> curr : rows) {
                cat = curr.f0;
                sum_x += curr.f1;
                sum_y += curr.f2;
                sum_xy += curr.f3;
                sum_xx += curr.f4;
                count_x += 1;
            }
            out.collect(new Tuple3<>(cat, ((count_x * sum_xy - sum_x * sum_y) / (count_x * sum_xx - sum_x * sum_x) ), 0.0));
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

    public static class Inventory extends Tuple4<Long, Long, Long, Integer> {

        public Inventory() { }

        public Inventory(Long inv_date_sk, Long inv_item_sk, Long inv_warehouse_sk, Integer inv_quantity_on_hand) {
            this.f0 = inv_date_sk;
            this.f1 = inv_item_sk;
            this.f2 = inv_warehouse_sk;
            this.f3 = inv_quantity_on_hand;
        }

        public Long getDate() { return this.f0; }
        public Long getItem() { return this.f1; }
        public Long getStore() { return this.f2; }
        public Integer getQuantity() { return this.f3; }
    }

    public static class DateDim extends Tuple3<Long, Integer, Integer> {

        public Long getDateKey() { return this.f0; }
        public Integer getYear() { return this.f1; }
        public Integer getMonth() { return this.f2; }
    }


    // *************************************************************************
    //     UTIL METHODS
    // *************************************************************************

    private static DataSet<Inventory> getInventoryDataSet(ExecutionEnvironment env) {
        return env.readCsvFile(inventory_path)
                .fieldDelimiter("|")
                .includeFields(inventory_mask)
                .ignoreInvalidLines()
                .tupleType(Inventory.class);
    }

    private static DataSet<DateDim> getDateDimDataSet(ExecutionEnvironment env) {
        return env.readCsvFile(date_dim_path)
                .fieldDelimiter("|")
                .includeFields(date_dim_mask)
                .tupleType(DateDim.class)
                .filter(new FilterDateDim());
    }
}
