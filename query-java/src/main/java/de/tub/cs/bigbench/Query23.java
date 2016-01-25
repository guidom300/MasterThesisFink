package de.tub.cs.bigbench;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.operators.base.JoinOperatorBase;
import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

import java.text.DateFormat;
import java.text.SimpleDateFormat;

/**
 * Created by gm on 06/11/15.
 */
public class Query23 {
    public static String q23_inventory_mask;
    public static String q23_date_dim_mask;

    public static String inventory_path;
    public static String date_dim_path;
    public static String input_path;
    public static String output_path;

    // Conf
    public static final Integer q23_year = 2001;
    public static final Integer q23_month = 1;
    public static final Double q23_coefficient = 1.3;

    public static void main(String[] args) throws Exception {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        PropertiesConfiguration config = new PropertiesConfiguration("config.properties");

        if(parseParameters(args) == 1)
            return;

        q23_inventory_mask = config.getString("q23_inventory_mask");
        q23_date_dim_mask = config.getString("q23_date_dim_mask");

        inventory_path = input_path + "/inventory/inventory.dat";
        date_dim_path = input_path + "/date_dim/date_dim.dat";

        //inventory -> inv_date_sk (long), inv_item_sk (Long). inv_warehouse_sk (Long), inv_quantity_on_hand (Long)
        DataSet<Inventory> inventory = getInventoryDataSet(env);
        //date_dim -> d_date_sk, d.d_date
        DataSet<DateDim> date_dim = getDateDimDataSet(env);

        DataSet<Tuple4<Long, Long, Integer, Integer>> joined =
                inventory
                        .join(date_dim, JoinHint.REPARTITION_HASH_FIRST)
                        .where(0)
                        .equalTo(0)
                        .with(new InventoryJoinDateDim());

        DataSet<Tuple4<Long, Long, Integer, Double>> avg =
                joined
                        .groupBy(0, 1, 2)
                        .reduceGroup(new Avg());

        DataSet<Tuple5<Long, Long, Integer, Double, Double>> q23_tmp_inv_part =
                joined
                        .join(avg, JoinHint.REPARTITION_HASH_FIRST)
                        .where(0, 1, 2)
                        .equalTo(0, 1, 2)
                        .with(new JoinedJoinAvg())
                        .groupBy(0, 1, 2)
                        .reduceGroup(new StdDev());

        DataSet<Tuple4<Long, Long, Integer, Double>> temp_table =
                q23_tmp_inv_part
                        .filter(new MeanStdDev())
                        .map(new Ratio());
/*
        DataSet<Tuple6<Long, Long, Integer, Double, Integer, Double>> result =
                temp_table
                        .groupBy(0)
                        .sortGroup(1, Order.ASCENDING)
                        .reduceGroup(new SortedFilter());*/


        DataSet<Tuple4<Long, Long, Integer, Double>> inv1 =
                temp_table
                        .filter(new Inv1FilterMonth());

        DataSet<Tuple4<Long, Long, Integer, Double>> inv2 =
                temp_table
                        .filter(new Inv2FilterMonth());

        DataSet<Tuple6<Long, Long, Integer, Double, Integer, Double>> result =
                inv2
                        .join(inv1, JoinHint.REPARTITION_SORT_MERGE)
                        .where(0, 1)
                        .equalTo(0, 1)
                        .with(new SelfJoinInv())
                        .sortPartition(0, Order.ASCENDING).setParallelism(1)
                        .sortPartition(1, Order.ASCENDING).setParallelism(1);

        result.writeAsCsv(output_path, "\n", ",", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        // execute program
        env.execute("Query23");
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

    @FunctionAnnotation.ForwardedFieldsFirst("f2->f0; f1; f3")
    @FunctionAnnotation.ForwardedFieldsSecond("f2")
    public static class InventoryJoinDateDim implements JoinFunction<Inventory, DateDim, Tuple4<Long, Long, Integer, Integer>>{

        private Tuple4<Long, Long, Integer, Integer> out = new Tuple4<>();

        @Override
        public Tuple4<Long, Long, Integer, Integer> join(Inventory inv, DateDim dd) throws Exception {
            out.f0 = inv.f2; out.f1 = inv.f1; out.f2 = dd.f2; out.f3 = inv.f3;
            return out;
        }
    }

    public static class Avg
            implements GroupReduceFunction<Tuple4<Long, Long, Integer, Integer>,  Tuple4<Long, Long, Integer, Double>> {

        @Override
        public void reduce(Iterable<Tuple4<Long, Long, Integer, Integer>> rows, Collector<Tuple4<Long, Long, Integer, Double>> out) throws Exception {
            Long warehouse = null;
            Long item = null;
            Integer month = null;
            Double sum = 0.0;
            Integer count = 0;

            for (Tuple4<Long, Long, Integer, Integer> curr : rows) {
                warehouse = curr.f0;
                item = curr.f1;
                month = curr.f2;
                sum += curr.f3;
                count++;
            }
            out.collect(new Tuple4<>(warehouse, item, month, sum / count));
        }
    }

    @FunctionAnnotation.ForwardedFieldsFirst("f0; f1; f2; f3->f4")
    @FunctionAnnotation.ForwardedFieldsSecond("f3")
    public static class JoinedJoinAvg implements JoinFunction<Tuple4<Long, Long, Integer, Integer>,
            Tuple4<Long, Long, Integer, Double>, Tuple5<Long, Long, Integer, Double, Integer>>{

        private Tuple5<Long, Long, Integer, Double, Integer> out = new Tuple5<>();

        @Override
        public Tuple5<Long, Long, Integer, Double, Integer> join(Tuple4<Long, Long, Integer, Integer> j,
                                                                 Tuple4<Long, Long, Integer, Double> av) throws Exception {
            out.f0 = j.f0; out.f1 = j.f1; out.f2 = j.f2; out.f3 = av.f3; out.f4 = j.f3;
            return out;
        }
    }

    public static class StdDev
            implements GroupReduceFunction<Tuple5<Long, Long, Integer, Double, Integer>,  Tuple5<Long, Long, Integer, Double, Double>> {

        @Override
        public void reduce(Iterable<Tuple5<Long, Long, Integer, Double, Integer>> rows, Collector<Tuple5<Long, Long, Integer, Double, Double>> out) throws Exception {
            Long warehouse = null;
            Long item = null;
            Integer month = null;
            Double avg = null;
            Double sum = 0.0;
            Integer count = 0;

            for (Tuple5<Long, Long, Integer, Double, Integer> curr : rows) {
                warehouse = curr.f0;
                item = curr.f1;
                month = curr.f2;
                avg = curr.f3;
                sum += Math.pow((double)curr.f4 - avg, 2);
                count++;
            }
            out.collect(new Tuple5<>(warehouse, item, month, Math.sqrt(sum / (count - 1)), avg));
        }
    }

    public static class MeanStdDev implements FilterFunction<Tuple5<Long, Long, Integer, Double, Double>> {

        @Override
        public boolean filter(Tuple5<Long, Long, Integer, Double, Double> row) throws Exception {
            return row.f4 > 0 && row.f3 / row.f4 >= q23_coefficient;
        }
    }

    @FunctionAnnotation.ForwardedFields("f0; f1; f2")
    public static class Ratio implements MapFunction<Tuple5<Long, Long, Integer, Double, Double>,
            Tuple4<Long, Long, Integer, Double>> {

        private Tuple4<Long, Long, Integer, Double> out = new Tuple4<>();

        @Override
        public Tuple4<Long, Long, Integer, Double> map(Tuple5<Long, Long, Integer, Double, Double> t) throws Exception {
            out.f0 = t.f0; out.f1 = t.f1; out.f2 = t.f2; out.f3 = t.f3 / t.f4;
            return out;
        }
    }

    public static class Inv1FilterMonth implements FilterFunction<Tuple4<Long, Long, Integer, Double>> {

        @Override
        public boolean filter(Tuple4<Long, Long, Integer, Double> row) throws Exception {
            return row.f2.equals(q23_month);
        }
    }

    public static class Inv2FilterMonth implements FilterFunction<Tuple4<Long, Long, Integer, Double>> {

        @Override
        public boolean filter(Tuple4<Long, Long, Integer, Double> row) throws Exception {
            return row.f2.equals(q23_month + 1);
        }
    }

    @FunctionAnnotation.ForwardedFieldsFirst("f0; f1; f2; f3")
    @FunctionAnnotation.ForwardedFieldsSecond("f2->f4; f3->f5")
    public static class SelfJoinInv implements JoinFunction<Tuple4<Long, Long, Integer, Double>, Tuple4<Long, Long, Integer, Double>,
            Tuple6<Long, Long, Integer, Double, Integer, Double>>{

        private Tuple6<Long, Long, Integer, Double, Integer, Double> out = new Tuple6<>();

        @Override
        public Tuple6<Long, Long, Integer, Double, Integer, Double> join(Tuple4<Long, Long, Integer, Double> inv1,
                                                                         Tuple4<Long, Long, Integer, Double> inv2) throws Exception {
            out.f0 = inv1.f0; out.f1 = inv1.f1; out.f2 = inv1.f2; out.f3 = inv1.f3; out.f4 = inv2.f2; out.f5 = inv2.f3;
            return out;
        }
    }

    public static class SortedFilter implements GroupReduceFunction<Tuple4<Long, Long, Integer, Double>, Tuple6<Long, Long, Integer, Double, Integer, Double>> {

        private Tuple6<Long, Long, Integer, Double, Integer, Double> tuple = new Tuple6<>();
        private Double cov = -1.0;
        private Double cov_consecutive = -1.0;

        @Override
        public void reduce(Iterable<Tuple4<Long, Long, Integer, Double>> in, Collector<Tuple6<Long, Long, Integer, Double, Integer, Double>> out) {
            Long warehouse = null;
            Long item = null;
            Long next_item = null;

            for (Tuple4<Long, Long, Integer, Double> curr : in) {
                warehouse = curr.f0;
                next_item = curr.f1;
                if (curr.f2.equals(q23_month))
                    cov = curr.f3;
                if (curr.f2.equals(q23_month + 1))
                    cov_consecutive = curr.f3;

                if(item != null && item.equals(next_item)){
                    tuple.f0 = warehouse;
                    tuple.f1 = item;
                    tuple.f2 = q23_month;
                    tuple.f3 = cov;
                    tuple.f4 = q23_month + 1;
                    tuple.f5 = cov_consecutive;
                    out.collect(tuple);
                }
                item = next_item;
            }
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

    private static DataSet<Inventory> getInventoryDataSet(ExecutionEnvironment env) {
        return env.readCsvFile(inventory_path)
                .fieldDelimiter("|")
                .includeFields(q23_inventory_mask)
                .ignoreInvalidLines()
                .tupleType(Inventory.class);
    }

    private static DataSet<DateDim> getDateDimDataSet(ExecutionEnvironment env) {
        return env.readCsvFile(date_dim_path)
                .fieldDelimiter("|")
                .includeFields(q23_date_dim_mask)
                .tupleType(DateDim.class)
                .filter(new FilterDateDim());
    }
}
