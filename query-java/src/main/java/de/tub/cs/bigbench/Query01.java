package de.tub.cs.bigbench;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;

import java.util.*;

/**
 * Created by gm on 23/11/15.
 */
public class Query01 {
    public static String q01_store_sales_mask;
    public static String q01_items_mask;

    public static String store_sales_path;
    public static String items_path;
    public static String input_path;
    public static String output_path;

    //Conf
    public static final Integer[] q01_i_category_id_IN = new Integer[]{1, 2, 3};
    public static final Integer q01_viewed_together_count = 50;
    public static final Integer q01_limit = 100;
    public static final Long[] q01_ss_store_sk_IN = new Long[]{(long)10, (long)20, (long)33, (long)40, (long)50};

    public static void main(String[] args) throws Exception {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        PropertiesConfiguration config = new PropertiesConfiguration("config.properties");

        if(parseParameters(args) == 1)
            return;

        q01_store_sales_mask = config.getString("q01_store_sales_mask");
        q01_items_mask = config.getString("q01_items_mask");

        store_sales_path = input_path + "/store_sales/store_sales.dat";
        items_path = input_path + "/item/item.dat";

        //store_sales-> ss_item_sk (Long), ss_store_sk (Long), ss_ticket_number (Long)
        DataSet<StoreSales> store_sales = getStoreSalesDataSet(env);
        //items -> i_item_sk (Long) i_category_id (Integer)
        DataSet<Item> items = getItemDataSet(env);

        DataSet<Sale> salesNumber =
                store_sales
                        .join(items)
                        .where(0)
                        .equalTo(0)
                        .with(new StoreSalesJoinItems());

        DataSet<SortedSet<Long>> soldItemsPerTicket = salesNumber
                .groupBy("ss_ticket_number")
                .reduceGroup(new DistinctReduce());

        DataSet<Tuple3<Long, Long, Integer>>
                pairs = soldItemsPerTicket
                .flatMap(new MakePairs())
                .groupBy(0,1)
                .aggregate(Aggregations.SUM, 2)
                .filter(new FilterCounts())
                .sortPartition(2, Order.DESCENDING).setParallelism(1)
                .first(q01_limit);

        pairs.writeAsCsv(output_path, "\n", ",", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute("Query01");
    }

    // *************************************************************************
    //     DATA TYPES
    // *************************************************************************
    public static class Item extends Tuple2<Long, Integer> {

        public Long getItem() { return this.f0; }
        public Integer getCategoryID() { return this.f1; }
    }

    public static class StoreSales extends Tuple3<Long, Long, Long> {

        public StoreSales() { }

        public StoreSales(Long ss_item_sk, Long ss_store_sk, Long ss_ticket_number) {
            this.f0 = ss_item_sk;
            this.f1 = ss_store_sk;
            this.f2 = ss_ticket_number;
        }

        public Long getItem() { return this.f0; }
        public Long getStore() { return this.f1; }
        public Long getTicket() { return this.f2; }
    }

    public static class Sale {
        public Long ss_ticket_number;
        public Long ss_item_sk;

        // Public constructor to make it a Flink POJO
        public Sale() {

        }

        public Sale(Long ss_ticket_number, Long ss_item_sk) {
            this.ss_ticket_number = ss_ticket_number;
            this.ss_item_sk = ss_item_sk;
        }

        @Override
        public String toString() {
            return ss_ticket_number + " " + ss_item_sk;
        }

    }

    // *************************************************************************
    //     DATA TRASFORMATIONS
    // *************************************************************************

    public static class FilterCategoryID implements FilterFunction<Item> {
        @Override
        public boolean filter(Item i) throws Exception {
            return Arrays.asList(q01_i_category_id_IN).contains(i.getCategoryID());
        }
    }

    public static class FilterStore implements FilterFunction<StoreSales>{

        @Override
        public boolean filter(StoreSales ss) throws Exception {
            return Arrays.asList(q01_ss_store_sk_IN).contains(ss.getStore());
        }
    }

    public static class StoreSalesJoinItems
            implements JoinFunction<StoreSales, Item, Sale> {

        @Override
        public Sale join(StoreSales ss, Item i) throws Exception {
            return new Sale(ss.getTicket(), ss.getItem());
        }
    }

    public static class DistinctReduce implements GroupReduceFunction<Sale, SortedSet<Long>> {

        @Override
        public void reduce(Iterable<Sale> in, org.apache.flink.util.Collector<SortedSet<Long>> out) throws Exception {

            SortedSet<Long> uniqItems = new TreeSet<Long>();
            Long key = null;

            // add all i_item_sk of the group to the set
            for (Sale t : in) {
                key = t.ss_ticket_number;
                uniqItems.add(t.ss_item_sk);
            }

            // emit all unique i_item_sk.
            out.collect(uniqItems);

        }
    }

    public static class PointAssigner
            implements FlatJoinFunction<Tuple2<Integer, Long>, Tuple2<Integer, Long>, Tuple3<Long, Long, Integer>> {

        @Override
        public void join(Tuple2<Integer, Long> item_a, Tuple2<Integer, Long> item_b, org.apache.flink.util.Collector<Tuple3<Long, Long, Integer>> out) {

            if (item_a.f1 < item_b.f1) {
                out.collect(new Tuple3<Long, Long, Integer>(item_a.f1, item_b.f1, 1));
            }
        }
    }

    public static class FilterCounts implements FilterFunction<Tuple3<Long, Long, Integer>> {
        @Override
        public boolean filter(Tuple3<Long, Long, Integer> t) throws ConfigurationException {
            return t.f2 > q01_viewed_together_count;
        }
    }

    public static class MakePairs implements FlatMapFunction<SortedSet<Long>, Tuple3<Long, Long, Integer>>
    {
        @Override
        public void flatMap(SortedSet<Long> longs, org.apache.flink.util.Collector<Tuple3<Long, Long, Integer>> out) throws Exception {
            for (Long item_a : longs) {
                for (Long item_b : longs) {
                    if(item_a < item_b)
                    {
                        out.collect(new Tuple3<Long, Long, Integer>(item_a, item_b, 1));
                    }
                }
            }
        }
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
                .includeFields(q01_store_sales_mask)
                .ignoreInvalidLines()
                .tupleType(StoreSales.class)
                .filter(new FilterStore());
    }

    private static DataSet<Item> getItemDataSet(ExecutionEnvironment env) {
        return env.readCsvFile(items_path)
                .fieldDelimiter("|")
                .includeFields(q01_items_mask)
                .ignoreInvalidLines()
                .tupleType(Item.class)
                .filter(new FilterCategoryID());
    }
}
