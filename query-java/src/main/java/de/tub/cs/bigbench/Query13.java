package de.tub.cs.bigbench;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.operators.base.JoinOperatorBase;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

import static org.apache.flink.api.common.operators.base.JoinOperatorBase.*;
import static org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint.*;

/**
 * Created by gm on 26/11/15.
 */
public class Query13 {
    //Config
    public static final Integer q13_YEAR = 2001;
    public static final Integer q13_LIMIT = 100;

    //Mapping
    public static String q13_store_sales_mask;
    public static String q13_date_dim_mask;
    public static String q13_web_sales_mask;
    public static String q13_customers_mask;

    public static String input_path;
    public static String output_path;
    public static String store_sales_path;
    public static String date_dim_path;
    public static String web_sales_path;
    public static String customers_path;

    public static void main(String[] args) throws Exception {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        PropertiesConfiguration config = new PropertiesConfiguration("config.properties");

        if(parseParameters(args) == 1)
            return;

        q13_store_sales_mask = config.getString("q13_store_sales_mask");
        q13_date_dim_mask = config.getString("q13_date_dim_mask");
        q13_web_sales_mask = config.getString("q13_web_sales_mask");
        q13_customers_mask = config.getString("q13_customers_mask");

        store_sales_path = input_path + "/store_sales/store_sales.dat";
        date_dim_path = input_path + "/date_dim/date_dim.dat";
        web_sales_path = input_path + "/web_sales/web_sales.dat";
        customers_path = input_path + "/customer/customer.dat";

        // store_sales -> ss.ss_sold_date_sk (Long), ss.ss_customer_sk Long), ss_net_paid (Double)
        DataSet<Sales> store_sales = getStoreSalesDataSet(env);

        // date_dim -> d_date_sk (Long), d.d_year (Integer)
        DataSet<DateDim> date_dim = getDateDimDataSet(env);

        // web_sales -> ws.ws_sold_date_sk (Long), ws.ws_bill_customer_sk (Long), ws_net_paid (Double)
        DataSet<Sales> web_sales = getWebSalesDataSet(env);

        //customers -> c_customer_sk (Long), c_first_name (String), c_last_name (String)
        DataSet<Customer> customers = getCustomersDataSet(env);

        DataSet<TempTable> store =
            store_sales
                .join(date_dim, BROADCAST_HASH_SECOND)
                .where(0)
                .equalTo(0)
                .with(new JoinHelper())
                .groupBy(0)
                .reduceGroup(new groupReducerHelper());

        DataSet<TempTable> web =
            web_sales
                .join(date_dim, BROADCAST_HASH_SECOND)
                .where(0)
                .equalTo(0)
                .with(new JoinHelper())
                .groupBy(0)
                .reduceGroup(new groupReducerHelper());

        DataSet<Tuple5<Long, String, String, Double, Double>> results =
            store
                .join(web)
                .where(0)
                .equalTo(0)
                .with(new SSJoinWS())
                .filter(new FilterTotals())
                .join(customers, BROADCAST_HASH_SECOND)
                .where(0)
                .equalTo(0)
                .with(new WebStoreJoinCustomer())
                .sortPartition(4, Order.DESCENDING).setParallelism(1)
                .sortPartition(0, Order.ASCENDING).setParallelism(1)
                .sortPartition(1, Order.ASCENDING).setParallelism(1)
                .sortPartition(2, Order.ASCENDING).setParallelism(1)
                .first(q13_LIMIT);

        results.writeAsCsv(output_path, FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute("Query13");
    }

    // *************************************************************************
    //     DATA TRASFORMATIONS
    // *************************************************************************

    public static class FilterYear implements FilterFunction<DateDim> {
        @Override
        public boolean filter(DateDim dd) throws Exception {
            return dd.f1.equals(q13_YEAR) || dd.f1.equals(q13_YEAR + 1);
        }
    }

    @FunctionAnnotation.ForwardedFieldsFirst("f1->f0; f2")
    @FunctionAnnotation.ForwardedFieldsSecond("f1")
    public static class JoinHelper
            implements JoinFunction<Sales, DateDim, Tuple3<Long, Integer, Double>> {

        private Tuple3<Long, Integer, Double> out = new Tuple3<>();

        @Override
        public Tuple3<Long, Integer, Double> join(Sales s, DateDim dd) throws Exception {
            out.f0 = s.f1; out.f1 = dd.f1; out.f2 = s.f2;
            return out;
        }
    }

    @FunctionAnnotation.ForwardedFields("f0")
    public static class groupReducerHelper
            implements GroupReduceFunction<Tuple3<Long, Integer, Double>, TempTable> {

        private TempTable tmp = new TempTable();

        @Override
        public void reduce(Iterable<Tuple3<Long, Integer, Double>> in, Collector<TempTable> out) {

            Long key = null;
            Double s_year1 = 0.0;
            Double s_year2 = 0.0;

            //ss_ext_list_price-ss_ext_wholesale_cost-ss_ext_discount_amt)+ss_ext_sales_price)/2
            for (Tuple3<Long, Integer, Double> curr : in) {
                key = curr.f0;
                if (curr.f1.equals(q13_YEAR))
                    s_year1 += curr.f2;
                else if (curr.f1.equals(q13_YEAR + 1))
                    s_year2 += curr.f2;
            }

            //HAVING first_year_total > 0
            if(s_year1 > 0.0) {
                tmp.f0 = key; tmp.f1 = s_year1; tmp.f2 = s_year2;
                out.collect(tmp);
            }
        }
    }

    @FunctionAnnotation.ForwardedFieldsFirst("f0; f1; f2")
    @FunctionAnnotation.ForwardedFieldsSecond("f1->f3; f2->f4")
    public static class SSJoinWS
            implements JoinFunction<TempTable, TempTable, Tuple5<Long, Double, Double, Double, Double>> {

        private Tuple5<Long, Double, Double, Double, Double> out = new Tuple5<>();

        @Override
        public Tuple5<Long, Double, Double, Double, Double> join(TempTable ss, TempTable ws) throws Exception {
            out.f0 = ss.f0; out.f1 = ss.f1; out.f2 = ss.f2; out.f3 = ws.f1; out.f4 = ws.f2;
            return out;
        }
    }

    // Filter Year
    public static class FilterTotals implements FilterFunction<Tuple5<Long, Double, Double, Double, Double>> {

        @Override
        public boolean filter(Tuple5<Long, Double, Double, Double, Double> row) throws Exception {
            return (row.f4 / row.f3) > (row.f2 / row.f1);
        }
    }

    @FunctionAnnotation.ForwardedFieldsSecond("f0; f1; f2")
    public static class WebStoreJoinCustomer
            implements JoinFunction<Tuple5<Long, Double, Double, Double, Double>, Customer, Tuple5<Long, String, String, Double, Double>> {

        private Tuple5<Long, String, String, Double, Double> out = new Tuple5<>();

        @Override
        public Tuple5<Long, String, String, Double, Double> join(Tuple5<Long, Double, Double, Double, Double> ws, Customer c) throws Exception {
            out.f0 = c.f0; out.f1 = c.f1; out.f2 = c.f2; out.f3 = ws.f2 / ws.f1; out.f4 = ws.f4 / ws.f3;
            return out;
        }
    }

    // *************************************************************************
    //     DATA TYPES
    // *************************************************************************

    public static class Sales extends Tuple3<Long, Long, Double> {

        public Long getDate() { return this.f0; }
        public Long getCustomer() { return this.f1; }
        public Double getNedPaid() { return this.f2; }
    }

    public static class DateDim extends Tuple2<Long, Integer> {

        public Long getDateKey() { return this.f0; }
        public Integer getYear() { return this.f1; }
    }

    public static class Customer extends Tuple3<Long, String, String> {

        public Long getCustomer() { return this.f0; }
        public String getFirstName() { return this.f1; }
        public String getLastName() { return this.f2; }
    }

    public static class TempTable extends Tuple3<Long, Double, Double> {

        public TempTable() { }

        public TempTable(Long customer_sk, Double first_year_total, Double second_year_total) {
            this.f0 = customer_sk;
            this.f1 = first_year_total;
            this.f2 = second_year_total;
        }

        public Long getCustomer() { return this.f0; }
        public Double getFirstYearTotal() { return this.f1; }
        public Double getSecondYearTotal() { return this.f2; }
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

    private static DataSet<Sales> getStoreSalesDataSet(ExecutionEnvironment env) {
        return env.readCsvFile(store_sales_path)
                .fieldDelimiter("|")
                .includeFields(q13_store_sales_mask)
                .ignoreInvalidLines()
                .tupleType(Sales.class);
    }

    private static DataSet<DateDim> getDateDimDataSet(ExecutionEnvironment env) {
        return env.readCsvFile(date_dim_path)
                .fieldDelimiter("|")
                .includeFields(q13_date_dim_mask)
                .tupleType(DateDim.class)
                .filter(new FilterYear());
    }

    private static DataSet<Sales> getWebSalesDataSet(ExecutionEnvironment env) {
        return env.readCsvFile(web_sales_path)
                .fieldDelimiter("|")
                .includeFields(q13_web_sales_mask)
                .ignoreInvalidLines()
                .tupleType(Sales.class);
    }

    private static DataSet<Customer> getCustomersDataSet(ExecutionEnvironment env) {
        return env.readCsvFile(customers_path)
                .fieldDelimiter("|")
                .includeFields(q13_customers_mask)
                .ignoreInvalidLines()
                .tupleType(Customer.class);
    }
}
