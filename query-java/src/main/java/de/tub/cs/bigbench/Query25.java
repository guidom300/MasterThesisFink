package de.tub.cs.bigbench;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.flink.api.common.functions.CrossFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.operators.base.JoinOperatorBase;
import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by gm on 12/11/15.
 */
public class Query25 {
    public static String q25_store_sales_mask;
    public static String q25_date_dim_mask;
    public static String q25_web_sales_mask;

    public static String input_path;
    public static String output_path;
    public static String store_sales_path;
    public static String date_dim_path;
    public static String web_sales_path;

    // Conf
    public static final String q25_date = "2002-01-02";

    public static void main(String[] args) throws Exception {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        PropertiesConfiguration config = new PropertiesConfiguration("config.properties");

        if(parseParameters(args) == 1)
            return;

        q25_store_sales_mask = config.getString("q25_store_sales_mask");
        q25_date_dim_mask = config.getString("q25_date_dim_mask");
        q25_web_sales_mask = config.getString("q25_web_sales_mask");;

        store_sales_path = input_path + "/store_sales/store_sales.dat";
        date_dim_path= input_path + "/date_dim/date_dim.dat";
        web_sales_path = input_path + "/web_sales/web_sales.dat";

        // get input data
        //store_sales -> ss_sold_date_sk (Long), ss_customer_sk (Long), ss_ticket_number (Long), ss_net_paid (Double)
        DataSet<Sales> store_sales = getStoreSalesDataSet(env);

        //date_dim -> d_date_sk, d.d_date
        DataSet<DateDim> date_dim = getDateDimDataSet(env);

        //web_sales -> ws.ws_sold_date_sk (Long), ws_bill_customer_sk (Long), ws_order_number (Long), ws_net_paid (Double)
        DataSet<Sales> web_sales = getWebSalesDataSet(env);

        DataSet<Tuple4<Long, Integer, Long, Double>> result_temp_1 =
            store_sales
                .join(date_dim, JoinHint.REPARTITION_HASH_SECOND)
                .where(0)
                .equalTo(0)
                .with(new StoreSalesJoinDateDim())
                .groupBy(0)
                .reduceGroup(new GroupComputation());

        DataSet<Tuple4<Long, Integer, Long, Double>> result_temp_2 =
            web_sales
                .join(date_dim, JoinHint.REPARTITION_HASH_SECOND)
                .where(0)
                .equalTo(0)
                .with(new StoreSalesJoinDateDim())
                .groupBy(0)
                .reduceGroup(new GroupComputation());

        DataSet<Tuple4<Long, Integer, Long, Double>> result_temp =
            result_temp_1
            .union(result_temp_2);

        DataSet<Tuple4<Long, Double, Double, Double>> result =
            result_temp
                .groupBy(0)
                .reduceGroup(new Reducer())
                .sortPartition(0, Order.ASCENDING).setParallelism(1);

        result.writeAsCsv(output_path, "\n", ",", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute("Query25");
    }


    // *************************************************************************
    //     DATA TRASFORMATIONS
    // *************************************************************************

    //Filter DateDim between startDate && endDate
    public static class FilterDateDim implements FilterFunction<DateDim> {

        private DateFormat format = new SimpleDateFormat("yyyy-MM-dd");

        @Override
        public boolean filter(DateDim dd) throws Exception {

            return format.parse(dd.f1).after(format.parse(q25_date));
        }
    }

    @FunctionAnnotation.ForwardedFieldsFirst("f1->f0; f2->f1; f0->f2; f3")
    public static class StoreSalesJoinDateDim implements JoinFunction<Sales, DateDim, Tuple4<Long, Long, Long, Double>> {

        private Tuple4<Long, Long, Long, Double> out = new Tuple4<>();

        @Override
        public Tuple4<Long, Long, Long, Double> join(Sales s, DateDim dd) throws Exception {
            out.f0 = s.f1; out.f1 = s.f2; out.f2 = s.f0; out.f3 = s.f3;
            return out;
        }
    }

    @FunctionAnnotation.ForwardedFields("f0")
    public static class GroupComputation implements GroupReduceFunction<Tuple4<Long, Long, Long, Double>,
            Tuple4<Long, Integer, Long, Double>> {

        private Set<Long> uniqTickets = new HashSet<>();
        private Tuple4<Long, Integer, Long, Double> tuple = new Tuple4<>();

        @Override
        public void reduce(Iterable<Tuple4<Long, Long, Long, Double>> in,
                           Collector<Tuple4<Long, Integer, Long, Double>> out) throws Exception {
            Long cid = null;
            uniqTickets.clear();
            Long max_dateid = (long)0;
            Double sum_amount = 0.0;

            for (Tuple4<Long, Long, Long, Double> curr : in) {
                cid = curr.f0;
                uniqTickets.add(curr.f1);
                if(curr.f2 > max_dateid)
                    max_dateid = curr.f2;
                sum_amount += curr.f3;
            }
            tuple.f0 = cid; tuple.f1 = uniqTickets.size(); tuple.f2 = max_dateid; tuple.f3 = sum_amount;
            out.collect(tuple);
        }
    }

    @FunctionAnnotation.ForwardedFields("f0")
    public static class Reducer implements GroupReduceFunction<Tuple4<Long, Integer, Long, Double>,
            Tuple4<Long, Double, Double, Double>> {

        private Tuple4<Long, Double, Double, Double> tuple = new Tuple4<>();

        @Override
        public void reduce(Iterable<Tuple4<Long, Integer, Long, Double>> in,
                           Collector<Tuple4<Long, Double, Double, Double>> out) throws Exception {
            Long cid = null;
            Long max_most_recent_date = (long)0;
            Double sum_frequency = 0.0;
            Double sum_amount = 0.0;

            for (Tuple4<Long, Integer, Long, Double> curr : in) {
                cid = curr.f0;
                if(curr.f2 > max_most_recent_date)
                    max_most_recent_date = curr.f2;
                sum_frequency += curr.f1;
                sum_amount += curr.f3;
            }
            tuple.f0 = cid; tuple.f1 = ((37621 - max_most_recent_date) < 60)? 1.0 : 0.0;
            tuple.f2 = sum_frequency; tuple.f3 = sum_amount;
            out.collect(tuple);
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
                .includeFields(q25_store_sales_mask)
                .ignoreInvalidLines()
                .tupleType(Sales.class);
    }

    private static DataSet<DateDim> getDateDimDataSet(ExecutionEnvironment env) {
        return env.readCsvFile(date_dim_path)
                .fieldDelimiter("|")
                .includeFields(q25_date_dim_mask)
                .tupleType(DateDim.class)
                .filter(new FilterDateDim());
    }

    private static DataSet<Sales> getWebSalesDataSet(ExecutionEnvironment env) {
        return env.readCsvFile(web_sales_path)
                .fieldDelimiter("|")
                .includeFields(q25_web_sales_mask)
                .ignoreInvalidLines()
                .tupleType(Sales.class);
    }
}
