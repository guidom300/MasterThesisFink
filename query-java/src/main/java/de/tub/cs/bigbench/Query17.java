package de.tub.cs.bigbench;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.flink.api.common.functions.CrossFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.operators.base.JoinOperatorBase;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.core.fs.FileSystem;

import java.util.Arrays;

import static org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint.*;

/**
 * Created by gm on 26/10/15.
 */
public class Query17 {
    public static String q17_store_sales_mask;
    public static String q17_date_dim_mask;
    public static String q17_items_mask;
    public static String q17_store_mask;
    public static String q17_promotion_mask;
    public static String q17_customer_mask;
    public static String q17_customer_address_mask;

    public static String input_path;
    public static String output_path;

    public static String store_sales_path;
    public static String date_dim_path;
    public static String items_path;
    public static String store_path;
    public static String promotion_path;
    public static String customer_path;
    public static String customer_address_path;

    // Conf
    public static final Integer q17_year = 2001;
    public static final Integer q17_month = 12;
    public static final Double q17_gmt_offset = -5.0;
    public static final String[] q17_i_category_IN = new String[]{"Books", "Music"};

    public static void main(String[] args) throws Exception {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        PropertiesConfiguration config = new PropertiesConfiguration("config.properties");

        if(parseParameters(args) == 1)
            return;

        q17_store_sales_mask = config.getString("q17_store_sales_mask");
        q17_date_dim_mask = config.getString("q17_date_dim_mask");
        q17_items_mask = config.getString("q17_items_mask");
        q17_store_mask = config.getString("q17_store_mask");
        q17_promotion_mask = config.getString("q17_promotion_mask");
        q17_customer_mask = config.getString("q17_customer_mask");
        q17_customer_address_mask = config.getString("q17_customer_address_mask");

        store_sales_path = input_path + "/store_sales/store_sales.dat";
        date_dim_path = input_path + "/date_dim/date_dim.dat";
        items_path = input_path + "/item/item.dat";
        store_path = input_path + "/store/store.dat";
        promotion_path = input_path + "/promotion/promotion.dat";
        customer_path = input_path + "/customer/customer.dat";
        customer_address_path = input_path + "/customer_address/customer_address.dat";

        // get input data
        //store_sales-> ss_sold_date_sk (Long), ss_item_sk (Long), ss_customer_sk (Long), ss_store_sk (LOng), ss_promo_sk (LOng), ss_ext_sales_price (Double)
        DataSet<StoreSales> store_sales = getStoreSalesDataSet(env);
        //date_dim -> d_date_sk, d.d_date
        DataSet<DateDim> date_dim = getDateDimDataSet(env);
        //items -> i_item_sk (Long) i_category_id (Integer)
        DataSet<Item> items = getItemDataSet(env);
        //store -> s_store_sk (Long), s_gmt_offset (Double)
        DataSet<Store> store = getStoreDataSet(env);
        //promotion -> p_promo_sk, p_channel_dmail, p_channel_email, p_channel_tv
        DataSet<Promotion> promotion = getPromotionDataSet(env);
        //customer -> c_customer_sk,c_current_addr_sk
        DataSet<Customer> customers = getCustomerDataSet(env);
        //customer_address-> ca_address_sk, ca_gmt_offset
        DataSet<CustomerAddress> customers_address = getCustomerAddressDataSet(env);


        DataSet<Promotion> promotion_q1 =
                promotion
                        .filter(new FilterPromotion());

        DataSet<Customer> cs = customers
                .join(customers_address, BROADCAST_HASH_SECOND)
                .where(1)
                .equalTo(0)
                .with(new CustomersJoinCustomerAddress());


        DataSet<StoreSales> ss =
                store_sales
                        .join(date_dim, BROADCAST_HASH_SECOND)
                        .where(0)
                        .equalTo(0)
                        .with(new StoreSalesJoinDateDim())
                        .join(items, BROADCAST_HASH_SECOND)
                        .where(1)
                        .equalTo(0)
                        .with(new StoreSalesJoinItem())
                        .join(store, BROADCAST_HASH_SECOND) //ROWS 12(SF1)  120(SF100)
                        .where(3)
                        .equalTo(0)
                        .with(new StoreSalesJoinStore())
                        .join(cs)
                        .where(2)
                        .equalTo(0)
                        .with(new StoreSalesJoinCs());

        DataSet<Tuple1<Double>> promotional_sales =
                ss
                    .join(promotion_q1, BROADCAST_HASH_SECOND)  //LESS THEN ROWS 300(SF1)  3707(SF100)
                    .where(4)
                    .equalTo(0)
                    .with(new StoreSalesJoinPromotionQ1())
                    .aggregate(Aggregations.SUM, 5)
                    .project(5);

        DataSet<Tuple1<Double>> all_sales =
                ss
                    .join(promotion, BROADCAST_HASH_SECOND)    //ROWS 300(SF1)  3707(SF100)
                    .where(4)
                    .equalTo(0)
                    .with(new StoreSalesJoinPromotionQ1())
                    .aggregate(Aggregations.SUM, 5)
                    .project(5);

        DataSet<Tuple3<Double, Double, Double>> result =
            promotional_sales
                .cross(all_sales)
                .with(new ComputeRatio());

        result.writeAsCsv(output_path, "\n", ",", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        // execute program
        env.execute("Query17");
    }


    // *************************************************************************
    //     DATA TRASFORMATIONS
    // *************************************************************************

    //Filter DateDim by year and month
    public static class FilterDateDim implements FilterFunction<DateDim> {

        @Override
        public boolean filter(DateDim dd) throws Exception {
            return dd.getYear().equals(q17_year) && dd.getMonth().equals(q17_month) ;
        }
    }

    public static class FilterCategory implements FilterFunction<Item>{

        @Override
        public boolean filter(Item i) throws Exception {
            return Arrays.asList(q17_i_category_IN).contains(i.getCategory());
        }
    }

    public static class FilterOffset implements FilterFunction<Store>{

        @Override
        public boolean filter(Store s) throws Exception {
            return s.getOffset().equals(q17_gmt_offset);
        }
    }

    public static class FilterPromotion implements FilterFunction<Promotion>{

        @Override
        public boolean filter(Promotion p) throws Exception {
            return p.getDMail().equals("Y") || p.getEMail().equals("Y") || p.getTv().equals("Y");
        }
    }

    public static class FilterCustomerAddress implements FilterFunction<CustomerAddress>{

        @Override
        public boolean filter(CustomerAddress ca) throws Exception {
            return ca.getOffset().equals(q17_gmt_offset);
        }
    }

    @FunctionAnnotation.ForwardedFieldsFirst("f0; f1")
    public static class CustomersJoinCustomerAddress implements JoinFunction<Customer, CustomerAddress, Customer> {
        @Override
        public Customer join(Customer c, CustomerAddress ca) throws Exception {
            return new Customer(c.f0, c.f1);
        }
    }

    @FunctionAnnotation.ForwardedFieldsFirst("f0; f1; f2; f3; f4; f5")
    public static class StoreSalesJoinDateDim
            implements JoinFunction<StoreSales, DateDim, StoreSales>{

        @Override
        public StoreSales join(StoreSales ss, DateDim dd) throws Exception {
            return new StoreSales(ss.f0, ss.f1, ss.f2, ss.f3, ss.f4, ss.f5);
        }
    }

    @FunctionAnnotation.ForwardedFieldsFirst("f0; f1; f2; f3; f4; f5")
    public static class StoreSalesJoinItem
            implements JoinFunction<StoreSales, Item, StoreSales>{

        @Override
        public StoreSales join(StoreSales ss, Item i) throws Exception {
            return new StoreSales(ss.f0, ss.f1, ss.f2, ss.f3, ss.f4, ss.f5);
        }
    }

    @FunctionAnnotation.ForwardedFieldsFirst("f0; f1; f2; f3; f4; f5")
    public static class StoreSalesJoinStore
            implements JoinFunction<StoreSales, Store, StoreSales>{

        @Override
        public StoreSales join(StoreSales ss, Store s) throws Exception {
            return new StoreSales(ss.f0, ss.f1, ss.f2, ss.f3, ss.f4, ss.f5);
        }
    }

    @FunctionAnnotation.ForwardedFieldsFirst("f0; f1; f2; f3; f4; f5")
    public static class StoreSalesJoinPromotionQ1
            implements JoinFunction<StoreSales, Promotion, StoreSales>{
        @Override
        public StoreSales join(StoreSales ss, Promotion p) throws Exception {
            return new StoreSales(ss.f0, ss.f1, ss.f2, ss.f3, ss.f4, ss.f5);
        }
    }

    @FunctionAnnotation.ForwardedFieldsFirst("f0; f1; f2; f3; f4; f5")
    public static class StoreSalesJoinCs
            implements JoinFunction<StoreSales, Customer, StoreSales>{
        @Override
        public StoreSales join(StoreSales ss, Customer c) throws Exception {
            return new StoreSales(ss.f0, ss.f1, ss.f2, ss.f3, ss.f4, ss.f5);
        }
    }

    public static class ComputeRatio
            implements CrossFunction<Tuple1<Double>, Tuple1<Double>, Tuple3<Double, Double, Double>> {

        @Override
        public Tuple3<Double, Double, Double> cross(Tuple1<Double> ps, Tuple1<Double> as) throws Exception {
            return new Tuple3<>(ps.f0, as.f0, (ps.f0 / as.f0) * 100);
        }
    }

    // *************************************************************************
    //     DATA TYPES
    // *************************************************************************

    public static class StoreSales extends Tuple6<Long, Long, Long, Long, Long, Double> {

        public StoreSales() { }

        public StoreSales(Long ss_sold_date_sk, Long ss_item_sk, Long ss_customer_sk, Long ss_store_sk, Long ss_promo_sk, Double ss_ext_sales_price) {
            this.f0 = ss_sold_date_sk;
            this.f1 = ss_item_sk;
            this.f2 = ss_customer_sk;
            this.f3 = ss_store_sk;
            this.f4 = ss_promo_sk;
            this.f5 = ss_ext_sales_price;
        }

        public Long getDate() { return this.f0; }
        public Long getItem() { return this.f1; }
        public Long getCustomer() { return this.f2; }
        public Long getStore() { return this.f3; }
        public Long getPromo() { return this.f4; }
        public Double getPrice() { return this.f5; }
    }

    public static class DateDim extends Tuple3<Long, Integer, Integer> {

        public Long getDateKey() { return this.f0; }
        public Integer getYear() { return this.f1; }
        public Integer getMonth() { return this.f2; }
    }

    public static class Item extends Tuple2<Long, String> {
        public Long getItem() { return this.f0; }
        public String getCategory() { return this.f1; }
    }

    public static class Store extends Tuple2<Long, Double> {
        public Long getStore() { return this.f0; }
        public Double getOffset() { return this.f1; }
    }

    public static class Promotion extends Tuple4<Long, String, String, String> {
        public Long getPromoKey() { return this.f0; }
        public String getDMail() { return this.f1; }
        public String getEMail() { return this.f2; }
        public String getTv() { return this.f3; }
    }

    public static class Customer extends Tuple2<Long, Long> {
        public Customer() { }

        public Customer(Long c_customer_sk, Long c_current_addr_sk) {
            this.f0 = c_customer_sk;
            this.f1 = c_current_addr_sk;
        }

        public Long getCustomer() { return this.f0; }
        public Long getAddress() { return this.f1; }
    }

    public static class CustomerAddress extends Tuple2<Long, Double> {
        public Long getAddress() { return this.f0; }
        public Double getOffset() { return this.f1; }
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
                .includeFields(q17_store_sales_mask)
                .ignoreInvalidLines()
                .tupleType(StoreSales.class);
    }

    private static DataSet<DateDim> getDateDimDataSet(ExecutionEnvironment env) {
        return env.readCsvFile(date_dim_path)
                .fieldDelimiter("|")
                .includeFields(q17_date_dim_mask)
                .tupleType(DateDim.class)
                .filter(new FilterDateDim());
    }

    private static DataSet<Item> getItemDataSet(ExecutionEnvironment env) {
        return env.readCsvFile(items_path)
                .fieldDelimiter("|")
                .includeFields(q17_items_mask)
                .ignoreInvalidLines()
                .tupleType(Item.class)
                .filter(new FilterCategory());
    }

    private static DataSet<Store> getStoreDataSet(ExecutionEnvironment env) {
        return env.readCsvFile(store_path)
                .fieldDelimiter("|")
                .includeFields(q17_store_mask)
                .ignoreInvalidLines()
                .tupleType(Store.class)
                .filter(new FilterOffset());
    }

    private static DataSet<Promotion> getPromotionDataSet(ExecutionEnvironment env) {
        return env.readCsvFile(promotion_path)
                .fieldDelimiter("|")
                .includeFields(q17_promotion_mask)
                .ignoreInvalidLines()
                .tupleType(Promotion.class);
    }

    private static DataSet<Customer> getCustomerDataSet(ExecutionEnvironment env) {
        return env.readCsvFile(customer_path)
                .fieldDelimiter("|")
                .includeFields(q17_customer_mask)
                .ignoreInvalidLines()
                .tupleType(Customer.class);
    }

    private static DataSet<CustomerAddress> getCustomerAddressDataSet(ExecutionEnvironment env) {
        return env.readCsvFile(customer_address_path)
                .fieldDelimiter("|")
                .includeFields(q17_customer_address_mask)
                .ignoreInvalidLines()
                .tupleType(CustomerAddress.class)
                .filter(new FilterCustomerAddress());
    }
}
