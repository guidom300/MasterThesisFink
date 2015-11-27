package de.tub.cs.bigbench;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;


/**
 * Created by gm on 13/11/15.
 */
public class Query24 {
    public static String q24_store_sales_mask;
    public static String q24_items_mask;
    public static String q24_web_sales_mask;
    public static String q24_item_marketprices_mask;

    public static String input_path;
    public static String output_path;

    public static String store_sales_path;
    public static String items_path;
    public static String item_marketprices_path;
    public static String web_sales_path;
    // Conf
    public static final Long q24_i_item_sk = (long)10000;

    public static void main(String[] args) throws Exception {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        PropertiesConfiguration config = new PropertiesConfiguration("config.properties");

        if(parseParameters(args) == 1)
            System.exit(1);


        q24_store_sales_mask = config.getString("q24_store_sales_mask");
        q24_items_mask = config.getString("q24_items_mask");
        q24_web_sales_mask = config.getString("q24_web_sales_mask");
        q24_item_marketprices_mask = config.getString("q24_item_marketprices_mask");

        store_sales_path = input_path + "/store_sales/store_sales.dat";
        items_path = input_path + "/item/item.dat";
        item_marketprices_path = input_path + "/item_marketprices/item_marketprices.dat";
        web_sales_path = input_path + "/web_sales/web_sales.dat";

        //items -> i_item_sk (Long), i_current_price (Double)
        DataSet<Item> items = getItemDataSet(env);

        //item_marketprices -> imp_item_sk (Long), imp_competitor_price (Double), imp_start_date (Long), imp_end_date (Long)
        DataSet<MarketPrice> item_marketprices = getMarketPriceDataSet(env);

        //web_sales-> ws_sold_date_sk (Long) , ws.ws_item_sk (Long), ws_quantity (Integer)
        DataSet<Sales> web_sales = getWebSalesDataSet(env);

        //store_sales-> ss_sold_date_sk, ss.ss_item_sk,  ss_quantity
        DataSet<Sales> store_sales = getStoreSalesDataSet(env);


        DataSet<TempTable1> temp_table1 =
                items
                        .join(item_marketprices)
                        .where(0)
                        .equalTo(0)
                        .with(new ItemsJoinMarketPrices())
                        .filter(new PricesComparison())
                        .map(new Computation())
                        .sortPartition(0, Order.ASCENDING).setParallelism(1)
                        .sortPartition(2, Order.ASCENDING).setParallelism(1);

        DataSet<TempTable2_3> temp_table2 =
                web_sales
                        .join(temp_table1)
                        .where(1)
                        .equalTo(0)
                        .with(new SalesJoinTempTable1())
                        .groupBy(0)
                        .reduceGroup(new Reducer());

        DataSet<TempTable2_3> temp_table3 =
                store_sales
                        .join(temp_table1)
                        .where(1)
                        .equalTo(0)
                        .with(new SalesJoinTempTable1())
                        .groupBy(0)
                        .reduceGroup(new Reducer());

        temp_table1
                .join(temp_table2)
                .where(0)
                .equalTo(0)
                .with(new TempTable1JoinTempTable2())
                .join(temp_table3)
                .where(0)
                .equalTo(0)
                .with(new TempTable1JoinTempTable3())
                .map(new CrossPriceComputation())
                .writeAsCsv(output_path, "\n", ",", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        // execute program
        env.execute("Query24");

    }


    // *************************************************************************
    //     DATA TRASFORMATIONS
    // *************************************************************************

    public static class FilterItemID implements FilterFunction<Item>{

        @Override
        public boolean filter(Item i) throws Exception {
            return i.getItem().equals(q24_i_item_sk);
        }
    }

    public static class ItemsJoinMarketPrices implements JoinFunction<Item, MarketPrice, Tuple5<Long, Double, Double, Long, Long>> {
        @Override
        public Tuple5<Long, Double, Double, Long, Long> join(Item item, MarketPrice marketPrice) throws Exception {
            return new Tuple5<>(item.getItem(), item.getPrice(), marketPrice.getCompPrice(), marketPrice.getStartDate(), marketPrice.getEndDate());
        }
    }

    public static class PricesComparison implements FilterFunction<Tuple5<Long, Double, Double, Long, Long>>{
        @Override
        public boolean filter(Tuple5<Long, Double, Double, Long, Long> row) throws Exception {
            return row.f2 < row.f1;
        }
    }

    public static class Computation implements MapFunction<Tuple5<Long, Double, Double, Long, Long>, TempTable1> {
        //i_item_sk, (imp_competitor_price - i_current_price)/i_current_price AS price_change, imp_start_date, (imp_end_date - imp_start_date) AS no_days_comp_price
        @Override
        public TempTable1 map(Tuple5<Long, Double, Double, Long, Long> t) throws Exception {
            return new TempTable1(t.f0, (t.f2 - t.f1) / t.f1, t.f3, t.f4 - t.f3);
        }
    }


    public static class SalesJoinTempTable1 implements JoinFunction<Sales, TempTable1, Tuple5<Long, Long, Integer, Long, Long>>{
        @Override
        public Tuple5<Long, Long, Integer, Long, Long> join(Sales sales, TempTable1 temp1) throws Exception {
            return new Tuple5<>(sales.getItem(), sales.getDate(), sales.getQuantity(), temp1.getStartDate(), temp1.getNoDaysCompPrice());
        }
    }

    public static class Reducer implements GroupReduceFunction<Tuple5<Long, Long, Integer, Long, Long>, TempTable2_3> {

        @Override
        public void reduce(Iterable<Tuple5<Long, Long, Integer, Long, Long>> in, Collector<TempTable2_3> out) throws Exception {
            Long item_sk = null;
            Integer current_quant = 0;
            Integer prev_quant = 0;

            for (Tuple5<Long, Long, Integer, Long, Long> curr : in) {
                item_sk = curr.f0;
                if((curr.f1 >= curr.f3) && (curr.f1 < curr.f3 + curr.f4))
                    current_quant += curr.f2;
                if((curr.f1 >= curr.f3 - curr.f4) && (curr.f1 < curr.f3))
                    prev_quant += curr.f2;
            }
            out.collect(new TempTable2_3(item_sk, current_quant, prev_quant));
        }
    }

    public static class TempTable1JoinTempTable2
            implements JoinFunction<TempTable1, TempTable2_3, Tuple4<Long, Double, Integer, Integer>>{
        @Override
        public Tuple4<Long, Double, Integer, Integer> join(TempTable1 tempTable1, TempTable2_3 tempTable2) throws Exception {
            return new Tuple4<>(tempTable1.getItem(), tempTable1.getPriceChange(), tempTable2.getCurrentQuantity(), tempTable2.getPrevQuantity());
        }
    }

    public static class TempTable1JoinTempTable3
            implements JoinFunction<Tuple4<Long, Double, Integer, Integer>, TempTable2_3, Tuple6<Long, Double, Integer, Integer, Integer, Integer>>{
        @Override
        public Tuple6<Long, Double, Integer, Integer, Integer, Integer> join(Tuple4<Long, Double, Integer, Integer> t, TempTable2_3 tempTable3) throws Exception {
            return new Tuple6<>(t.f0, t.f1, t.f2, t.f3, tempTable3.getCurrentQuantity(), tempTable3.getPrevQuantity());
        }
    }

    public static class CrossPriceComputation
            implements MapFunction<Tuple6<Long, Double, Integer, Integer, Integer, Integer>, Tuple2<Long, Double>> {
        //i_item_sk,
        //(current_ss_quant + current_ws_quant - prev_ss_quant - prev_ws_quant) / ((prev_ss_quant + prev_ws_quant) * price_change)
        @Override
        public Tuple2<Long, Double> map(Tuple6<Long, Double, Integer, Integer, Integer, Integer> row) throws Exception {
            return new Tuple2<>(row.f0,  (row.f4 + row.f2 - row.f5 - row.f3) / ((row.f5 + row.f3) * row.f1));
        }
    }

    // *************************************************************************
    //     DATA TYPES
    // *************************************************************************

    public static class Sales extends Tuple3<Long, Long, Integer> {

        public Sales() { }

        public Sales(Long sold_date_sk, Long item_sk, Integer quantity) {
            this.f0 = sold_date_sk;
            this.f1 = item_sk;
            this.f2 = quantity;
        }

        public Long getDate() { return this.f0; }
        public Long getItem() { return this.f1; }
        public Integer getQuantity() { return this.f2; }
    }

    public static class Item extends Tuple2<Long, Double> {
        public Long getItem() { return this.f0; }
        public Double getPrice() { return this.f1; }
    }

    public static class MarketPrice extends Tuple4<Long, Double, Long, Long> {
        public Long getItem() { return this.f0; }
        public Double getCompPrice() { return this.f1; }
        public Long getStartDate() { return this.f2; }
        public Long getEndDate() { return this.f3; }
    }

    public static class TempTable1 extends Tuple4<Long, Double, Long, Long> {

        public TempTable1() { }

        public TempTable1(Long i_item_sk, Double price_change, Long imp_start_date, Long no_days_comp_price) {
            this.f0 = i_item_sk;
            this.f1 = price_change;
            this.f2 = imp_start_date;
            this.f3 = no_days_comp_price;
        }

        public Long getItem() { return this.f0; }
        public Double getPriceChange() { return this.f1; }
        public Long getStartDate() { return this.f2; }
        public Long getNoDaysCompPrice() { return this.f3; }
    }

    public static class TempTable2_3 extends Tuple3<Long, Integer, Integer> {

        public TempTable2_3() { }

        public TempTable2_3(Long item_sk, Integer current_quant, Integer prev_quant) {
            this.f0 = item_sk;
            this.f1 = current_quant;
            this.f2 = prev_quant;
        }

        public Long getItem() { return this.f0; }
        public Integer getCurrentQuantity() { return this.f1; }
        public Integer getPrevQuantity() { return this.f2; }
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

    private static DataSet<Sales> getStoreSalesDataSet(ExecutionEnvironment env) {
        return env.readCsvFile(store_sales_path)
                .fieldDelimiter("|")
                .includeFields(q24_store_sales_mask)
                .ignoreInvalidLines()
                .tupleType(Sales.class);
    }

    private static DataSet<Sales> getWebSalesDataSet(ExecutionEnvironment env) {
        return env.readCsvFile(web_sales_path)
                .fieldDelimiter("|")
                .includeFields(q24_web_sales_mask)
                .ignoreInvalidLines()
                .tupleType(Sales.class);
    }

    private static DataSet<Item> getItemDataSet(ExecutionEnvironment env) {
        return env.readCsvFile(items_path)
                .fieldDelimiter("|")
                .includeFields(q24_items_mask)
                .ignoreInvalidLines()
                .tupleType(Item.class)
                .filter(new FilterItemID());
    }

    private static DataSet<MarketPrice> getMarketPriceDataSet(ExecutionEnvironment env) {
        return env.readCsvFile(item_marketprices_path)
                .fieldDelimiter("|")
                .includeFields(q24_item_marketprices_mask)
                .ignoreInvalidLines()
                .tupleType(MarketPrice.class);
    }
}
