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
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

import static org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint.*;
import static org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint.BROADCAST_HASH_SECOND;

/**
 * Created by gm on 25/11/15.
 */
public class Query06 {
    //Config
    public static final Integer q06_YEAR = 2001;
    public static final Integer q06_LIMIT = 100;

    //Mapping
    public static String q06_store_sales_mask;
    public static String q06_date_dim_mask;
    public static String q06_web_sales_mask;
    public static String q06_customers_mask;

    public static String input_path;
    public static String output_path;

    public static String store_sales_path;
    public static String date_dim_path;
    public static String web_sales_path;
    public static String customers_path;;

    public static void main(String[] args) throws Exception {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        PropertiesConfiguration config = new PropertiesConfiguration("config.properties");

        if(parseParameters(args) == 1)
            return;

        q06_store_sales_mask = config.getString("q06_store_sales_mask");
        q06_date_dim_mask = config.getString("q06_date_dim_mask");
        q06_web_sales_mask = config.getString("q06_web_sales_mask");
        q06_customers_mask = config.getString("q06_customers_mask");

        store_sales_path = input_path + "/store_sales/store_sales.dat";
        date_dim_path = input_path + "/date_dim/date_dim.dat";
        web_sales_path = input_path + "/web_sales/web_sales.dat";
        customers_path = input_path + "/customer/customer.dat";

        // store_sales -> ss_sold_date_sk (Long), ss_customer_sk (Long),
        // ss_ext_discount_amt (Double), ss_ext_sales_price (Double), ss_ext_wholesale_cost (Double), ss_ext_list_price (Double)
        DataSet<Sales> store_sales = getStoreSalesDataSet(env);

        // date_dim -> d_date_sk (Long), d_year (Integer)
        DataSet<DateDim> date_dim = getDateDimDataSet(env);

        // web_sales -> ws_sold_date_sk (Long), ws_bill_customer_sk (Long),
        // ws_ext_discount_amt (Double), ws_ext_sales_price (Double) , ws_ext_wholesale_cost (Double), ws_ext_list_price (Double)
        DataSet<Sales> web_sales = getWebSalesDataSet(env);

        //customer -> c_customer_sk (Long), c_first_name (String), c_last_name (String),
        // c_preferred_cust_flag (String), c_birth_country (String),c_login (String), c_email_address (String)
        DataSet<Customer> customers = getCustomersDataSet(env);


        DataSet<Tuple3<Long, Double, Double>> customer_store_sales =
                store_sales
                        .join(date_dim, BROADCAST_HASH_SECOND)
                        .where(0)
                        .equalTo(0)
                        .with(new JoinHelper())
                        .groupBy(0)
                        .reduceGroup(new getCustomerSales());

        DataSet<Tuple3<Long, Double, Double>> customer_web_sales =
                web_sales
                        .join(date_dim, BROADCAST_HASH_SECOND)
                        .where(0)
                        .equalTo(0)
                        .with(new JoinHelper())
                        .groupBy(0)
                        .reduceGroup(new getCustomerSales());


        //Result -> c_customer_sk Long, c_first_name STRING, c_last_name STRING, c_preferred_cust_flag STRING,
        // c_birth_country STRING, c_login STRING, c_email_address STRING

        DataSet<Tuple8<Double, Long, String, String, String, String, String, String>> results =
                customer_store_sales
                        .join(customer_web_sales, BROADCAST_HASH_FIRST)
                        .where(0)
                        .equalTo(0)
                        .with(new SSJoinWS())
                        .filter(new FilterTotals())
                        .join(customers, BROADCAST_HASH_FIRST)
                        .where(0)
                        .equalTo(0)
                        .with(new WebStoreJoinCustomer())
                        .sortPartition(0, Order.DESCENDING).setParallelism(1)
                        .sortPartition(1, Order.ASCENDING).setParallelism(1)
                        .sortPartition(2, Order.ASCENDING).setParallelism(1)
                        .sortPartition(3, Order.ASCENDING).setParallelism(1)
                        .sortPartition(4, Order.ASCENDING).setParallelism(1)
                        .sortPartition(5, Order.ASCENDING).setParallelism(1)
                        .sortPartition(6, Order.ASCENDING).setParallelism(1)
                        .sortPartition(7, Order.ASCENDING).setParallelism(1)
                        .first(q06_LIMIT);

        results.writeAsCsv(output_path, "\n", ",", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute("Query06");

    }

    // *************************************************************************
    //     DATA TRASFORMATIONS
    // *************************************************************************

    // Filter Year
    public static class FilterYear implements FilterFunction<DateDim> {
        @Override
        public boolean filter(DateDim dd) throws Exception {
            return dd.f1.equals(q06_YEAR) || dd.f1.equals(q06_YEAR + 1);
        }
    }

    @FunctionAnnotation.ForwardedFieldsFirst("f1->f0; f2; f3; f4; f5")
    @FunctionAnnotation.ForwardedFieldsSecond("f1")
    public static class JoinHelper
            implements JoinFunction<Sales, DateDim, Tuple6<Long, Integer, Double, Double, Double, Double>> {

        private Tuple6<Long, Integer, Double, Double, Double, Double> out = new Tuple6<>();

        @Override
        public Tuple6<Long, Integer, Double, Double, Double, Double> join(Sales s, DateDim dd) throws Exception {
            out.f0 = s.f1; out.f1 = dd.f1; out.f2 = s.f2; out.f3 = s.f3; out.f4 = s.f4; out.f5 = s.f5;
            return out;
        }
    }

    // GroupReduceFunction that computes two sums.
    public static class getCustomerSales
            implements GroupReduceFunction<Tuple6<Long, Integer, Double, Double, Double, Double>, Tuple3<Long, Double, Double>> {

        private Tuple3<Long, Double, Double> tuple = new Tuple3<>();

        @Override
        public void reduce(Iterable<Tuple6<Long, Integer, Double, Double, Double, Double>> in, Collector<Tuple3<Long, Double, Double>> out) {

            Long key = null;
            Double s_year1 = 0.0;
            Double s_year2 = 0.0;

            //ss_ext_list_price-ss_ext_wholesale_cost-ss_ext_discount_amt)+ss_ext_sales_price)/2
            for (Tuple6<Long, Integer, Double, Double, Double, Double> curr : in) {
                key = curr.f0;
                if (curr.f1.equals(q06_YEAR))
                    s_year1 += ((curr.f5 - curr.f4 - curr.f2) + curr.f3) / 2;
                else if (curr.f1.equals(q06_YEAR + 1))
                    s_year2 += ((curr.f5 - curr.f4 - curr.f2) + curr.f3) / 2;
            }

            //HAVING first_year_total > 0
            if(s_year1 > 0) {
                tuple.f0 = key;
                tuple.f1 = s_year1;
                tuple.f2 = s_year2;
                out.collect(tuple);
            }
        }
    }

    @FunctionAnnotation.ForwardedFieldsFirst("f0; f1; f2")
    @FunctionAnnotation.ForwardedFieldsSecond("f1->f3; f2->f4")
    public static class SSJoinWS
            implements JoinFunction<Tuple3<Long, Double, Double>, Tuple3<Long, Double, Double>, Tuple5<Long, Double, Double, Double, Double>> {

        private Tuple5<Long, Double, Double, Double, Double> out = new Tuple5<>();
        @Override
        public Tuple5<Long, Double, Double, Double, Double> join(Tuple3<Long, Double, Double> ss, Tuple3<Long, Double, Double> ws) throws Exception {
            out.f0 = ss.f0; out.f1 = ss.f1; out.f2 = ss.f2; out.f3 =ws.f1; out.f4 = ws.f2;
            return out;
        }
    }

    // Filter Year
    public static class FilterTotals implements FilterFunction<Tuple5<Long, Double, Double, Double, Double>> {
        //(web.second_year_total / web.first_year_total)  >  (store.second_year_total / store.first_year_total)
        @Override
        public boolean filter(Tuple5<Long, Double, Double, Double, Double> row) throws Exception {
            return (row.f4 / row.f3) > (row.f2 / row.f1);
        }
    }

    @FunctionAnnotation.ForwardedFieldsSecond("f0->f1; f1->f2; f2->f3; f3->f4; f4->f5; f5->f6; f6->f7")
    public static class WebStoreJoinCustomer
            implements JoinFunction<Tuple5<Long, Double, Double, Double, Double>, Customer, Tuple8<Double, Long, String, String, String, String, String, String>> {

        private Tuple8<Double, Long, String, String, String, String, String, String> out = new Tuple8<>();

        @Override
        public Tuple8<Double, Long, String, String, String, String, String, String> join(Tuple5<Long, Double, Double, Double, Double> ws, Customer c) throws Exception {
            out.f0 = ws.f4 / ws. f3; out.f1 = c.f0; out.f2 = c.f1; out.f3 = c.f2;  out.f4 = c.f3; out.f5 = c.f4; out.f6 = c.f5; out.f7 = c.f6;
            return out;
        }
    }

    // *************************************************************************
    //     DATA TYPES
    // *************************************************************************

    public static class Sales extends Tuple6<Long, Long, Double, Double, Double, Double> {

        public Long getDate() { return this.f0; }
        public Long getCustomer() { return this.f1; }
        public Double getDiscount() { return this.f2; }
        public Double getSellerPrice() { return this.f3; }
        public Double getWhosale() { return this.f4; }
        public Double getListPrice() { return this.f5; }
    }

    public static class DateDim extends Tuple2<Long, Integer> {

        public Long getDateKey() { return this.f0; }
        public Integer getYear() { return this.f1; }
    }

    public static class Customer extends Tuple7<Long, String, String, String, String, String, String> {

        public Long getCustomer() { return this.f0; }
        public String getFirstName() { return this.f1; }
        public String getLastName() { return this.f2; }
        public String getPreferedFlag() { return this.f3; }
        public String getBirthCountry() { return this.f4; }
        public String getLogin() { return this.f5; }
        public String getEmail() { return this.f6; }
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
                .includeFields(q06_store_sales_mask)
                .ignoreInvalidLines()
                .tupleType(Sales.class);
    }

    private static DataSet<DateDim> getDateDimDataSet(ExecutionEnvironment env) {
        return env.readCsvFile(date_dim_path)
                .fieldDelimiter("|")
                .includeFields(q06_date_dim_mask)
                .tupleType(DateDim.class)
                .filter(new FilterYear());
    }

    private static DataSet<Sales> getWebSalesDataSet(ExecutionEnvironment env) {
        return env.readCsvFile(web_sales_path)
                .fieldDelimiter("|")
                .includeFields(q06_web_sales_mask)
                .ignoreInvalidLines()
                .tupleType(Sales.class);
    }

    private static DataSet<Customer> getCustomersDataSet(ExecutionEnvironment env) {
        return env.readCsvFile(customers_path)
                .fieldDelimiter("|")
                .includeFields(q06_customers_mask)
                .ignoreInvalidLines()
                .tupleType(Customer.class);
    }
}
