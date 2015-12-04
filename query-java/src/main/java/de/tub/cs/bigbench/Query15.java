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
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;

import static org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint.BROADCAST_HASH_SECOND;

/**
 * Created by gm on 26/10/15.
 */
public class Query15 {
    public static String q15_store_sales_mask;
    public static String q15_date_dim_mask;
    public static String q15_items_mask;

    public static String input_path;
    public static String output_path;

    public static String store_sales_path;
    public static String date_dim_path;
    public static String items_path;

    // Conf
    public static final String startDate = "2001-09-02";
    public static final String endDate = "2002-09-02";
    public static final Integer q15_store_sk = 10;

    public static void main(String[] args) throws Exception {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        PropertiesConfiguration config = new PropertiesConfiguration("config.properties");

        if(parseParameters(args) == 1)
            System.exit(1);

        store_sales_path = input_path + "/store_sales/store_sales.dat";
        date_dim_path= input_path + "/date_dim/date_dim.dat";
        items_path = input_path + "/item/item.dat";

        q15_store_sales_mask = config.getString("q15_store_sales_mask");
        q15_date_dim_mask = config.getString("q15_date_dim_mask");
        q15_items_mask = config.getString("q15_items_mask");

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
                .join(dd, BROADCAST_HASH_SECOND)
                    .where(0)
                    .equalTo(0)
                    .with(new StoreSalesLeftJoinDD())
                    .join(items)
                    .where(1)
                    .equalTo(0)
                    .with(new StoreSalesJoinItem());

            t
                .groupBy(0, 1)
                .aggregate(Aggregations.SUM, 2)
                .map(new Temp())
                .groupBy(0)
                .reduceGroup(new Reducer())
                .filter(new SlopeFilter())
                .sortPartition(0, Order.ASCENDING).setParallelism(1)
                .writeAsCsv(output_path, "\n", ",", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute("Query15");
    }


    // *************************************************************************
    //     DATA TRASFORMATIONS
    // *************************************************************************

    //Filter DateDim between startDate && endDate
    public static class FilterDateDim implements FilterFunction<DateDim> {

        private DateFormat format = new SimpleDateFormat("yyyy-MM-dd");

        @Override
        public boolean filter(DateDim dd) throws Exception {
            return !(format.parse(dd.f1).after(format.parse(endDate)) || format.parse(dd.f1).before(format.parse(startDate))) ;
        }
    }

    @FunctionAnnotation.ForwardedFieldsFirst("f0; f1; f2; f3")
    public static class StoreSalesLeftJoinDD implements JoinFunction<StoreSales, Tuple1<Long>, StoreSales> {

        private StoreSales out = new StoreSales();

        @Override
        public StoreSales join(StoreSales ss, Tuple1<Long> dd) throws Exception {
            out.f0 = ss.f0; out.f1 = ss.f1; out.f2 = ss.f2; out.f3 = ss.f3;
            return out;
        }
    }

    @FunctionAnnotation.ForwardedFieldsFirst("f0->f1; f3->f2")
    @FunctionAnnotation.ForwardedFieldsSecond("f1->f0")
    public static class StoreSalesJoinItem
            implements JoinFunction<StoreSales, Item, Tuple3<Integer, Long, Double>>{

        private Tuple3<Integer, Long, Double> out = new Tuple3<>();

        @Override
        public Tuple3<Integer, Long, Double> join(StoreSales ss, Item i) throws Exception {
            out.f0 = i.f1; out.f1 = ss.f0; out.f2 = ss.f3;
            return out;
        }
    }

    public static class StoreFilter implements FilterFunction<StoreSales> {

        @Override
        public boolean filter(StoreSales ss) throws Exception {
            return ss.f2.equals((long) q15_store_sk);
        }
    }

    public static class Temp implements MapFunction<Tuple3<Integer, Long, Double>, Tuple5<Integer, Long, Double, Double, Long>> {

        private Tuple5<Integer, Long, Double, Double, Long> out = new Tuple5<>();

        @Override
        public Tuple5<Integer, Long, Double, Double, Long> map(Tuple3<Integer, Long, Double> t) throws Exception {
            out.f0 = t.f0; out.f1 = t.f1; out.f2 = t.f2; out.f3 = t.f1 * t.f2; out.f4 = t.f1 * t.f1;
            return out;
        }
    }

    public static class Reducer implements GroupReduceFunction<Tuple5<Integer, Long, Double, Double, Long>, Tuple3<Integer, Double, Double>> {

        private Tuple3<Integer, Double, Double> tuple = new Tuple3<>();

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
            tuple.f0 = cat; tuple.f1 = ((count_x * sum_xy - sum_x * sum_y) / (count_x * sum_xx - sum_x * sum_x));
            tuple.f2 = (sum_y - ((count_x * sum_xy - sum_x * sum_y) / (count_x * sum_xx - sum_x * sum_x) ) * sum_x) / count_x;
            out.collect(tuple);
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

    private static DataSet<StoreSales> getStoreSalesDataSet(ExecutionEnvironment env) {
        return env.readCsvFile(store_sales_path)
                .fieldDelimiter("|")
                .includeFields(q15_store_sales_mask)
                .ignoreInvalidLines()
                .tupleType(StoreSales.class)
                .filter(new StoreFilter());
    }

    private static DataSet<DateDim> getDateDimDataSet(ExecutionEnvironment env) {
        return env.readCsvFile(date_dim_path)
                .fieldDelimiter("|")
                .includeFields(q15_date_dim_mask)
                .tupleType(DateDim.class)
                .filter(new FilterDateDim());
    }

    private static DataSet<Item> getItemDataSet(ExecutionEnvironment env) {
        return env.readCsvFile(items_path)
                .fieldDelimiter("|")
                .includeFields(q15_items_mask)
                .ignoreInvalidLines()
                .tupleType(Item.class);
    }
}
