package de.tub.cs.bigbench;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.operators.base.JoinOperatorBase;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

import static org.apache.flink.api.common.operators.base.JoinOperatorBase.*;


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

        //item_marketprices -> imp_sk (Long), imp_item_sk (Long), imp_competitor_price (Double), imp_start_date (Long), imp_end_date (Long)
        DataSet<MarketPrice> item_marketprices = getMarketPriceDataSet(env);

        //web_sales-> ws_sold_date_sk (Long) , ws.ws_item_sk (Long), ws_quantity (Integer)
        DataSet<Sales> web_sales = getWebSalesDataSet(env);

        //store_sales-> ss_sold_date_sk (Long), ss.ss_item_sk (Long),  ss_quantity (Integer)
        DataSet<Sales> store_sales = getStoreSalesDataSet(env);

        DataSet<TempTable1> temp_table1 =
            items
                .join(item_marketprices, JoinHint.BROADCAST_HASH_FIRST)
                .where(0)
                .equalTo(1)
                .with(new ItemsJoinMarketPrices())
                .filter(new PricesComparison())
                .map(new Computation())
                .sortPartition(0, Order.ASCENDING).setParallelism(1)
                .sortPartition(1, Order.ASCENDING).setParallelism(1)
                .sortPartition(3, Order.ASCENDING).setParallelism(1);

        DataSet<TempTable2_3> ws =
            web_sales
                .join(temp_table1)
                .where(1)
                .equalTo(0)
                .with(new SalesJoinTempTable1())
                .groupBy(0, 1, 2)
                .reduceGroup(new Reducer());

        DataSet<TempTable2_3> ss =
            store_sales
                .join(temp_table1)
                .where(1)
                .equalTo(0)
                .with(new SalesJoinTempTable1())
                .groupBy(0, 1, 2)
                .reduceGroup(new Reducer());

        DataSet<Tuple2<Long, Double>> result =
            ws
                .join(ss)
                .where(0, 1)
                .equalTo(0, 1)
                .with(new WSJoinSS())
                .groupBy(0)
                .reduceGroup(new CrossPriceComputation());

        result.writeAsCsv(output_path, "\n", ",", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        // execute program
        env.execute("Query24");
    }

    // *************************************************************************
    //     DATA TRASFORMATIONS
    // *************************************************************************

    public static class FilterItemID implements FilterFunction<Item>{

        @Override
        public boolean filter(Item i) throws Exception {
            return i.f0.equals(q24_i_item_sk);
        }
    }

    @FunctionAnnotation.ForwardedFieldsFirst("f0; f1")
    @FunctionAnnotation.ForwardedFieldsSecond("f0->f2; f2->f3; f3->f4; f4->f5")
    public static class ItemsJoinMarketPrices implements JoinFunction<Item, MarketPrice, Tuple6<Long, Double, Long, Double, Long, Long>> {

        private Tuple6<Long, Double, Long, Double, Long, Long> out = new Tuple6<>();

        @Override
        public Tuple6<Long, Double, Long, Double, Long, Long> join(Item item, MarketPrice marketPrice) throws Exception {
            out.f0 = item.f0; out.f1 = item.f1; out.f2 = marketPrice.f0;
            out.f3 = marketPrice.f2; out.f4 = marketPrice.f3; out.f5 = marketPrice.f4;
            return out;
        }
    }

    @FunctionAnnotation.ReadFields("f1; f3")
    public static class PricesComparison implements FilterFunction<Tuple6<Long, Double, Long, Double, Long, Long>>{
        @Override
        public boolean filter(Tuple6<Long, Double, Long, Double, Long, Long> row) throws Exception {
            return row.f3 < row.f1;
        }
    }

    @FunctionAnnotation.ForwardedFields("f0; f2->f1")
    public static class Computation implements MapFunction<Tuple6<Long, Double, Long, Double, Long, Long>, TempTable1> {
        //i_item_sk, (imp_competitor_price - i_current_price)/i_current_price AS price_change, imp_start_date, (imp_end_date - imp_start_date) AS no_days_comp_price

        private TempTable1 out = new TempTable1();

        @Override
        public TempTable1 map(Tuple6<Long, Double, Long, Double, Long, Long> t) throws Exception {
            out.f0 = t.f0; out.f1 = t.f2;
            out.f2 = (t.f3 - t.f1) / t.f1;
            out.f3 =  t.f4; out.f4 = t.f5 - t.f4;
            return out;
        }
    }

    @FunctionAnnotation.ForwardedFieldsFirst("f1->f0; f0->f3; f2->f6")
    @FunctionAnnotation.ForwardedFieldsSecond("f1; f2; f3->f4; f4->f5")
    public static class SalesJoinTempTable1 implements JoinFunction<Sales, TempTable1, Tuple7<Long, Long, Double, Long, Long, Long, Integer>>{

        private Tuple7<Long, Long, Double, Long, Long, Long, Integer> out = new Tuple7<>();

        @Override
        public Tuple7<Long, Long, Double, Long, Long, Long, Integer> join(Sales sales, TempTable1 temp1) throws Exception {
            out.f0 = sales.f1; out.f1 = temp1.f1; out.f2 = temp1.f2; out.f3 = sales.f0;
            out.f4 = temp1.f3; out.f5 = temp1.f4; out.f6 = sales.f2;
            return out;
        }
    }

    @FunctionAnnotation.ForwardedFields("f0; f1; f2")
    public static class Reducer implements GroupReduceFunction<Tuple7<Long, Long, Double, Long, Long, Long, Integer>, TempTable2_3> {

        private TempTable2_3 tuple = new TempTable2_3();

        @Override
        public void reduce(Iterable<Tuple7<Long, Long, Double, Long, Long, Long, Integer>> in, Collector<TempTable2_3> out) throws Exception {
            Long item_sk = null;
            Long imp_sk = null;
            Double price_change = null;
            Integer current_quant = 0;
            Integer prev_quant = 0;

            for (Tuple7<Long, Long, Double, Long, Long, Long, Integer> curr : in) {
                item_sk = curr.f0;
                imp_sk = curr.f1;
                price_change = curr.f2;
                if((curr.f3 >= curr.f4) && (curr.f3 < curr.f4 + curr.f5))
                    current_quant += curr.f6;
                if((curr.f3 >= curr.f4 - curr.f5) && (curr.f3 < curr.f4))
                    prev_quant += curr.f6;
            }
            tuple.f0 = item_sk; tuple.f1 = imp_sk; tuple.f2 = price_change; tuple.f3 = current_quant; tuple.f4 = prev_quant;
            out.collect(tuple);
        }
    }

    @FunctionAnnotation.ForwardedFieldsFirst("f0; f3->f1; f4->f2; f2->f5")
    @FunctionAnnotation.ForwardedFieldsSecond("f3; f4")
    public static class WSJoinSS
            implements JoinFunction<TempTable2_3, TempTable2_3, Tuple6<Long, Integer, Integer, Integer, Integer, Double>>{

        private Tuple6<Long, Integer, Integer, Integer, Integer, Double> out = new Tuple6<>();

        @Override
        public Tuple6<Long, Integer, Integer, Integer, Integer, Double> join(TempTable2_3 ws, TempTable2_3 ss) throws Exception {
            out.f0 = ws.f0; out.f1 = ws.f3; out.f2 = ws.f4; out.f3 = ss.f3; out.f4 = ss.f4; out.f5 = ws.f2;
            return out;
        }
    }

    @FunctionAnnotation.ForwardedFields("f0")
    public static class CrossPriceComputation implements GroupReduceFunction<Tuple6<Long, Integer, Integer, Integer, Integer, Double>, Tuple2<Long, Double>> {

        private Tuple2<Long, Double> tuple = new Tuple2<>();

        @Override
        public void reduce(Iterable<Tuple6<Long, Integer, Integer, Integer, Integer, Double>> in, Collector<Tuple2<Long, Double>> out) throws Exception {
            Long ws_item_sk = null;
            Integer count = 0;
            Double price = 0.0;

            for (Tuple6<Long, Integer, Integer, Integer, Integer, Double> curr : in) {
                ws_item_sk = curr.f0;
                price += (curr.f3 + curr.f1 - curr.f4 - curr.f2) / ((curr.f4 + curr.f2) * curr.f5);
                count++;
            }
            tuple.f0 = ws_item_sk; tuple.f1 = price / (double)count;
            out.collect(tuple);
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

    public static class MarketPrice extends Tuple5<Long, Long, Double, Long, Long> {
        public Long getSk() { return this.f0; }
        public Long getItem() { return this.f1; }
        public Double getCompPrice() { return this.f2; }
        public Long getStartDate() { return this.f3; }
        public Long getEndDate() { return this.f4; }
    }

    public static class TempTable1 extends Tuple5<Long, Long, Double, Long, Long> {

        public TempTable1() { }

        public TempTable1(Long i_item_sk, Long imp_sk, Double price_change, Long imp_start_date, Long no_days_comp_price) {
            this.f0 = i_item_sk;
            this.f1 = imp_sk;
            this.f2 = price_change;
            this.f3 = imp_start_date;
            this.f4 = no_days_comp_price;
        }

        public Long getItem() { return this.f0; }
        public Long getSk() { return this.f1; }
        public Double getPriceChange() { return this.f2; }
        public Long getStartDate() { return this.f3; }
        public Long getNoDaysCompPrice() { return this.f4; }
    }

    public static class TempTable2_3 extends Tuple5<Long, Long, Double, Integer, Integer> {

        public TempTable2_3() { }

        public TempTable2_3(Long item_sk, Long imp_sk, Double price_change, Integer current_quant, Integer prev_quant) {
            this.f0 = item_sk;
            this.f1 = imp_sk;
            this.f2 = price_change;
            this.f3 = current_quant;
            this.f4 = prev_quant;
        }

        public Long getItem() { return this.f0; }
        public Long getSk() { return this.f1; }
        public Double getPriceChange() { return this.f2; }
        public Integer getCurrentQuantity() { return this.f3; }
        public Integer getPrevQuantity() { return this.f4; }
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
