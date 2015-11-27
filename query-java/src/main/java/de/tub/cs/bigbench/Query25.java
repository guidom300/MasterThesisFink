package de.tub.cs.bigbench;

import org.apache.flink.api.common.functions.CrossFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * Created by gm on 12/11/15.
 */
public class Query25 {
    public static final String store_sales_mask = "10010000010000000000100";
    public static final String date_dim_mask = "1010000000000000000000000000";
    public static final String web_sales_mask = "1000100000000000010000000000010000";

    public static final String store_sales_path = "/Users/gm/bigbench/data-generator/output/store_sales.dat";
    public static final String date_dim_path= "/Users/gm/bigbench/data-generator/output/date_dim.dat";
    public static final String web_sales_path = "/Users/gm/bigbench/data-generator/output/web_sales.dat";

    // Conf
    public static final String q25_date = "2002-01-02";

    public static void main(String[] args) throws Exception {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // get input data
        //store_sales -> ss_sold_date_sk (Long), ss_customer_sk (Long), ss_ticket_number (Long), ss_sold_date_sk, ss_net_paid (Double)
        DataSet<Sales> store_sales = getStoreSalesDataSet(env);

        //date_dim -> d_date_sk, d.d_date
        DataSet<DateDim> date_dim = getDateDimDataSet(env);

        //web_sales -> ws.ws_sold_date_sk (Long), ws_bill_customer_sk (Long), ws_order_number (Long), ws_net_paid (Double)
        DataSet<Sales> web_sales = getWebSalesDataSet(env);

        DataSet<Tuple4<Long, Integer, Integer, Integer>> result =
                store_sales
                        .join(date_dim) //JOIN date_dim dd ON ss.ss_sold_date_sk = dd.d_date_sk
                        .where(0)
                        .equalTo(0)
                        .with(new StoreSalesJoinDateDim())
                        .groupBy(0, 1, 2)
                        .aggregate(Aggregations.SUM, 3)
                        .union(
                                web_sales
                                        .join(date_dim) //JOIN date_dim dd ON ss.ss_sold_date_sk = dd.d_date_sk
                                        .where(0)
                                        .equalTo(0)
                                        .with(new StoreSalesJoinDateDim())
                                        .groupBy(0, 1, 2)
                                        .aggregate(Aggregations.SUM, 3)
                        )
                        .groupBy(0)
                        .reduceGroup(new Reducer())
                        .sortPartition(0, Order.ASCENDING).setParallelism(1);

        result.writeAsCsv("/Users/gm/bigbench/data-generator/output/q25_result_flink.dat", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

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

            return format.parse(dd.getDate()).after(format.parse(q25_date));
        }
    }

    public static class StoreSalesJoinDateDim implements JoinFunction<Sales, DateDim, Sales>{
        @Override
        public Sales join(Sales s, DateDim dd) throws Exception {
            return new Sales(s.getCustomer(), s.getNumber(), s.getDate(), s.getNetPaid());
        }
    }

    public static class Reducer implements GroupReduceFunction<Sales, Tuple4<Long, Integer, Integer, Integer>> {
        @Override
        public void reduce(Iterable<Sales> in, Collector<Tuple4<Long, Integer, Integer, Integer>> out) throws Exception {
            Long cid = null;
            Integer count_oid = 0;
            Long max_dateid = (long)0;
            Double sum_amount = 0.0;

            for (Sales curr : in) {
                cid = curr.f0;
                count_oid++;
                if(curr.f2 > max_dateid)
                    max_dateid = curr.f2;
                sum_amount += curr.f3;
            }
            out.collect(new Tuple4<>(cid, ((37621 - max_dateid) < 60)? 1 : 0, count_oid, (int) Math.round(sum_amount)));
        }
    }

    // *************************************************************************
    //     DATA TYPES
    // *************************************************************************

    public static class Sales extends Tuple4<Long, Long, Long, Double> {

        public Sales() { }

        public Sales(Long sold_date_sk, Long customer_sk, Long number, Double net_paid) {
            this.f0 = sold_date_sk;
            this.f1 = customer_sk;
            this.f2 = number;
            this.f3 = net_paid;
        }

        public Long getDate() { return this.f0; }
        public Long getCustomer() { return this.f1; }
        public Long getNumber() { return this.f2; }
        public Double getNetPaid() { return this.f3; }
    }

    public static class DateDim extends Tuple2<Long, String> {

        public Long getDateKey() { return this.f0; }
        public String getDate() { return this.f1; }
    }



    // *************************************************************************
    //     UTIL METHODS
    // *************************************************************************

    private static DataSet<Sales> getStoreSalesDataSet(ExecutionEnvironment env) {
        return env.readCsvFile(store_sales_path)
                .fieldDelimiter("|")
                .includeFields(store_sales_mask)
                .ignoreInvalidLines()
                .tupleType(Sales.class);
    }

    private static DataSet<DateDim> getDateDimDataSet(ExecutionEnvironment env) {
        return env.readCsvFile(date_dim_path)
                .fieldDelimiter("|")
                .includeFields(date_dim_mask)
                .tupleType(DateDim.class)
                .filter(new FilterDateDim());
    }

    private static DataSet<Sales> getWebSalesDataSet(ExecutionEnvironment env) {
        return env.readCsvFile(web_sales_path)
                .fieldDelimiter("|")
                .includeFields(web_sales_mask)
                .ignoreInvalidLines()
                .tupleType(Sales.class);
    }
}
