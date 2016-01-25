package de.tub.cs.bigbench;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by gm on 19/11/15.
 */public class Query20 {
    public static String q20_store_sales_mask;
    public static String q20_store_returns_mask;

    public static String input_path;
    public static String output_path;
    public static String temp_path;
    public static  String store_sales_path;
    public static  String store_returns_path;

    public static void main(String[] args) throws Exception {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        PropertiesConfiguration config = new PropertiesConfiguration("config.properties");

        if(parseParameters(args) == 1)
            return;

        q20_store_sales_mask = config.getString("q20_store_sales_mask");
        q20_store_returns_mask = config.getString("q20_store_returns_mask");

        store_sales_path = input_path + "/store_sales/store_sales.dat";
        store_returns_path = input_path + "/store_returns/store_returns.dat";

        // get input data

        //store_sales-> ss_item_sk (Long), ss_customer_sk (Long), ss_ticket_number (Long), ss_net_paid (Double)
        DataSet<Store> store_sales = getStoreSalesDataSet(env);

        //store_returns -> sr_item_sk (Long),  sr_customer_sk (Long), sr_ticket_number (Long), sr_return_amt (Double)
        DataSet<Store> store_returns = getStoreReturnsDataSet(env);

        DataSet<Order> orders =
            store_sales
                .groupBy(1)
                .reduceGroup(new GroupReduce());


        DataSet<Order> returned =
            store_returns
                .groupBy(1)
                .reduceGroup(new GroupReduce());

        DataSet<Result> result =
            orders
                .leftOuterJoin(returned)
                .where(0)
                .equalTo(0)
                .with(new SalesLeftOuterJoinReturned());

        result.writeAsCsv(temp_path, "\n", " ", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        // execute program
        env.execute("Query20");
    }


    // *************************************************************************
    //     DATA TRASFORMATIONS
    // *************************************************************************

    public static class GroupReduce
            implements GroupReduceFunction<Store, Order> {
        @Override
        public void reduce(Iterable<Store> in, Collector<Order> out) throws Exception {
            Set<Long> uniqTicketNumber = new HashSet<Long>();
            Long customer = null;
            Integer items_counter = 0;
            Double net_amt_sum = 0.0;

            for (Store t : in) {
                customer = t.getCustomer();
                uniqTicketNumber.add(t.getTicket());
                net_amt_sum += t.getNetAmt();
                items_counter++;
            }
            out.collect(new Order(customer, uniqTicketNumber.size(), items_counter, net_amt_sum));
        }
    }

    public static class SalesLeftOuterJoinReturned
            implements JoinFunction<Order, Order, Result> {

        @Override
        public Result join(Order sales, Order returned) {
            return new Result(sales.getCustomer(),
                    (returned == null || sales.getCount().equals(0)) ? 0.0 : round((returned.getCount() / sales.getCount()), 7),
                    (returned == null || sales.getItem().equals(0)) ? 0.0 : round((returned.getItem() / sales.getItem()),7),
                    (returned == null || sales.getMoney().equals(0.0)) ? 0.0 : round((returned.getMoney() / sales.getMoney()),7),
                    returned == null ? 0.0 : round((returned.getCount()), 0));
        }
    }


    // *************************************************************************
    //     DATA TYPES
    // *************************************************************************

    public static class Store extends Tuple4<Long, Long, Long, Double> {

        public Store() { }

        public Store(Long item, Long customer, Long ticket_number, Double net_amt) {
            this.f0 = item;
            this.f1 = customer;
            this.f2 = ticket_number;
            this.f3 = net_amt;
        }

        public Long getItem() { return this.f0; }
        public Long getCustomer() { return this.f1; }
        public Long getTicket() { return this.f2; }
        public Double getNetAmt() { return this.f3; }
    }

    public static class Order extends Tuple4<Long, Integer, Integer, Double> {

        public Order() { }

        public Order(Long customer, Integer count , Integer items, Double money) {
            this.f0 = customer;
            this.f1 = count;
            this.f2 = items;
            this.f3 = money;
        }


        public Long getCustomer() { return this.f0; }
        public Integer getCount() { return this.f1; }
        public Integer getItem() { return this.f2; }
        public Double getMoney() { return this.f3; }
    }

    public static class Result extends Tuple5<Long, Double, Double, Double, Double> {

        public Result() { }

        public Result(Long customer, Double orderRatio, Double itemsRatio, Double monetaryRatio, Double frequency) {
            this.f0 = customer;
            this.f1 = orderRatio;
            this.f2 = itemsRatio;
            this.f3 = monetaryRatio;
            this.f4 = frequency;
        }
    }


    // *************************************************************************
    //     UTIL METHODS
    // *************************************************************************

    private static int parseParameters(String[] args){
        if(args.length == 3){
            input_path = args[0];
            output_path = args[1];
            temp_path = args[2];
            return 0;
        }
        else{
            System.err.println("Usage: Each query needs 3 arguments.");
            return 1;
        }
    }

    private static DataSet<Store> getStoreSalesDataSet(ExecutionEnvironment env) throws ConfigurationException {
        return env.readCsvFile(store_sales_path)
                .fieldDelimiter("|")
                .includeFields(q20_store_sales_mask)
                .ignoreInvalidLines()
                .tupleType(Store.class);
    }

    private static DataSet<Store> getStoreReturnsDataSet(ExecutionEnvironment env) throws ConfigurationException {
        return env.readCsvFile(store_returns_path)
                .fieldDelimiter("|")
                .includeFields(q20_store_returns_mask)
                .ignoreInvalidLines()
                .tupleType(Store.class);
    }

    public static double round(double value, int places) {
        if (places < 0)
            throw new IllegalArgumentException();

        BigDecimal bd = new BigDecimal(value);
        bd = bd.setScale(places, RoundingMode.HALF_UP);
        return bd.doubleValue();
    }

}


