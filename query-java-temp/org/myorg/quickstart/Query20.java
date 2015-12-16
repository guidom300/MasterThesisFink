package org.myorg.quickstart;

import org.apache.flink.api.common.functions.CrossFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by gm on 05/11/15.
 */
public class Query20 {
    public static final String store_sales_mask = "00110000010000000000100";
    public static final String store_returns_mask = "00110000010100000000";

    public static final String store_sales_path = "hdfs://localhost:9000/Users/gm/bigbench/data-generator/output/store_sales.dat";
    public static final String store_returns_path= "/Users/gm/bigbench/data-generator/output/store_returns.dat";

    public static void main(String[] args) throws Exception {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // get input data

        //store_sales-> ss_customer_sk (Long), ss_store_sk (LOng), ss_promo_sk (LOng), ss_ext_sales_price (Double)
        DataSet<StoreSales> store_sales = getStoreSalesDataSet(env);
        store_sales.print();
        //store_returns -> sr_item_sk (Long),  sr_customer_sk (Long), sr_ticket_number (Long), sr_return_amt (Double)
        DataSet<StoreReturns> store_returns = getStoreReturnsDataSet(env);


        DataSet<Tuple4<Long, Integer, Integer, Double>> orders =
                store_sales
                .groupBy(1)
                .reduceGroup(new GroupReduce());



        // execute program


    }


    // *************************************************************************
    //     DATA TRASFORMATIONS
    // *************************************************************************

    public static class GroupReduce
            implements GroupReduceFunction<StoreSales, Tuple4<Long, Integer, Integer, Double>> {
        @Override
        public void reduce(Iterable<StoreSales> in, Collector<Tuple4<Long, Integer, Integer, Double>> out) throws Exception {
            Set<Long> uniqTicketNumber = new HashSet<Long>();
            Long customer = null;
            Integer items_counter = 0;
            Double net_paid_sum = 0.0;

            for (Tuple4<Long, Long, Long, Double> t : in) {
                customer = t.f1;
                uniqTicketNumber.add(t.f2);
                net_paid_sum += t.f3;
                items_counter++;
            }
            out.collect(new Tuple4<>(customer, uniqTicketNumber.size(), items_counter, net_paid_sum));
        }
    }


    // *************************************************************************
    //     DATA TYPES
    // *************************************************************************

    public static class StoreSales extends Tuple4<Long, Long, Long, Double> {

        public StoreSales() { }

        public StoreSales(Long ss_item_sk, Long ss_customer_sk, Long ss_ticket_number, Double ss_net_paid) {
            this.f0 = ss_item_sk;
            this.f1 = ss_customer_sk;
            this.f2 = ss_ticket_number;
            this.f3 = ss_net_paid;
        }

        public Long getItem() { return this.f0; }
        public Long getCustomer() { return this.f1; }
        public Long getTicket() { return this.f2; }
        public Double getNetPaid() { return this.f3; }
    }

    public static class StoreReturns extends Tuple4<Long, Long, Long, Double> {

        public StoreReturns() { }

        public StoreReturns(Long sr_item_sk, Long sr_customer_sk, Long sr_ticket_number, Double sr_return_amt) {
            this.f0 = sr_item_sk;
            this.f1 = sr_customer_sk;
            this.f2 = sr_ticket_number;
            this.f3 = sr_return_amt;
        }

        public Long getItem() { return this.f0; }
        public Long getCustomer() { return this.f1; }
        public Long getTicket() { return this.f2; }
        public Double getReturnAmt() { return this.f3; }
    }


    // *************************************************************************
    //     UTIL METHODS
    // *************************************************************************

    private static DataSet<StoreSales> getStoreSalesDataSet(ExecutionEnvironment env) {
        return env.readCsvFile(store_sales_path)
                .fieldDelimiter("|")
                .includeFields(store_sales_mask)
                .ignoreInvalidLines()
                .tupleType(StoreSales.class);
    }

    private static DataSet<StoreReturns> getStoreReturnsDataSet(ExecutionEnvironment env) {
        return env.readCsvFile(store_returns_path)
                .fieldDelimiter("|")
                .includeFields(store_returns_mask)
                .ignoreInvalidLines()
                .tupleType(StoreReturns.class);
    }

}

