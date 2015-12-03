package de.tub.cs.bigbench;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.core.fs.FileSystem;

import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Created by gm on 11/11/15.
 */
public class Query29 {

    public static String q29_web_sales_mask;
    public static String q29_items_mask;

    public static String web_sales_path;
    public static String items_path;
    public static String input_path;
    public static String output_path;

    //Conf
    public static final Integer q29_limit = 100;

    public static void main(String[] args) throws Exception {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        PropertiesConfiguration config = new PropertiesConfiguration("config.properties");

        if(parseParameters(args) == 1)
            return;

        q29_web_sales_mask = config.getString("q29_web_sales_mask");
        q29_items_mask = config.getString("q29_items_mask");
        web_sales_path = input_path + "/web_sales/web_sales.dat";
        items_path = input_path + "/item/item.dat";

        //web_sales -> ws.ws_item_sk (Long), ws_order_number (Long)
        DataSet<WebSales> web_sales = getWebSalesDataSet(env);
        //items -> i_item_sk (Long) i_category_id (Integer)
        DataSet<Item> items = getItemDataSet(env);

        DataSet<Order> salesNumber =
                web_sales
                        .join(items)
                        .where(0)
                        .equalTo(0)
                        .with(new WebSalesJoinItems());


        DataSet<SortedSet<Integer>> collectedList = salesNumber
                .groupBy("ws_order_number")            // group DataSet by the first tuple field
                .reduceGroup(new DistinctReduce());

        DataSet<Tuple3<Integer, Integer, Integer>>  pairs =
                collectedList
                .flatMap(new MakePairs())
                .groupBy(0,1)
                .aggregate(Aggregations.SUM, 2)
                .sortPartition(2, org.apache.flink.api.common.operators.Order.DESCENDING).setParallelism(1)
                .sortPartition(0, org.apache.flink.api.common.operators.Order.ASCENDING).setParallelism(1)
                .sortPartition(1, org.apache.flink.api.common.operators.Order.ASCENDING).setParallelism(1)
                .first(q29_limit);

        pairs.writeAsCsv(output_path, "\n", ",", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute("Query29");
    }

    // *************************************************************************
    //     DATA TYPES
    // *************************************************************************
    public static class Item extends Tuple2<Long, Integer> {

        public Long getItem() { return this.f0; }
        public Integer getCategoryID() { return this.f1; }
    }

    public static class WebSales extends Tuple2<Long, Long> {

        public WebSales() { }

        public WebSales(Long ws_item_sk, Long ws_order_number) {
            this.f0 = ws_item_sk;
            this.f1 = ws_order_number;
        }

        public Long getItem() { return this.f0; }
        public Long getOrderNumber() { return this.f1; }
    }

    public static class Order {
        public Long ws_order_number;
        public Integer i_category_id;

        // Public constructor to make it a Flink POJO
        public Order() {

        }

        public Order(Long ws_order_number, Integer i_category_id) {
            this.ws_order_number = ws_order_number;
            this.i_category_id = i_category_id;
        }

        @Override
        public String toString() {
            return ws_order_number + " " + i_category_id;
        }

    }

    // *************************************************************************
    //     DATA TRASFORMATIONS
    // *************************************************************************

    @FunctionAnnotation.ForwardedFieldsFirst("f1->f0")
    @FunctionAnnotation.ForwardedFieldsSecond("f1")
    public static class WebSalesJoinItems
            implements JoinFunction<WebSales, Item, Order> {

        @Override
        public Order join(WebSales ws, Item i) throws Exception {
            return new Order(ws.f1, i.f1);
        }
    }

    public static class DistinctReduce implements GroupReduceFunction<Order, SortedSet<Integer>> {

        @Override
        public void reduce(Iterable<Order> in, org.apache.flink.util.Collector<SortedSet<Integer>> out) throws Exception {

            SortedSet<Integer> uniqItems = new TreeSet<Integer>();
            Long key = null;

            // add all i_item_sk of the group to the set
            for (Order t : in) {
                key = t.ws_order_number;
                uniqItems.add(t.i_category_id);
            }

            // emit all unique i_item_sk.
            out.collect(uniqItems);

        }
    }

    public static class MakePairs implements FlatMapFunction<SortedSet<Integer>, Tuple3<Integer, Integer, Integer>>
    {
        @Override
        public void flatMap(SortedSet<Integer> cat, org.apache.flink.util.Collector<Tuple3<Integer, Integer, Integer>> out) throws Exception {
            for (Integer category_a : cat) {
                for (Integer category_b : cat) {
                    if(category_a < category_b)
                    {
                        out.collect(new Tuple3<>(category_a, category_b, 1));
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

    private static DataSet<WebSales> getWebSalesDataSet(ExecutionEnvironment env) {
        return env.readCsvFile(web_sales_path)
                .fieldDelimiter("|")
                .includeFields(q29_web_sales_mask)
                .ignoreInvalidLines()
                .tupleType(WebSales.class);
    }

    private static DataSet<Item> getItemDataSet(ExecutionEnvironment env) {
        return env.readCsvFile(items_path)
                .fieldDelimiter("|")
                .includeFields(q29_items_mask)
                .ignoreInvalidLines()
                .tupleType(Item.class);
    }
}

