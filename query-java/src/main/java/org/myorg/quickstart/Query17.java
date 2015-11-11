package org.myorg.quickstart;

import org.apache.flink.api.common.functions.CrossFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.tuple.*;

import java.util.Arrays;

/**
 * Created by gm on 26/10/15.
 */
public class Query17 {
    public static final String store_sales_mask = "10110001100000010000000";
    public static final String date_dim_mask = "1000001010000000000000000000";
    public static final String items_mask = "1000000000001000000000";
    public static final String store_mask = "10000000000000000000000000010";
    public static final String promotion_mask = "1000000011010000000";
    public static final String customer_mask = "100010000000000000";
    public static final String customer_address_mask = "1000000000010";

    public static final String store_sales_path = "/Users/gm/bigbench/data-generator/output/store_sales.dat";
    public static final String date_dim_path= "/Users/gm/bigbench/data-generator/output/date_dim.dat";
    public static final String items_path = "/Users/gm/bigbench/data-generator/output/item.dat";
    public static final String store_path = "/Users/gm/bigbench/data-generator/output/store.dat";
    public static final String promotion_path = "/Users/gm/bigbench/data-generator/output/promotion.dat";
    public static final String customer_path = "/Users/gm/bigbench/data-generator/output/customer.dat";
    public static final String customer_address_path = "/Users/gm/bigbench/data-generator/output/customer_address.dat";

    // Conf
    public static final Integer q17_year = 2001;
    public static final Integer q17_month = 12;
    public static final Double q17_gmt_offset = -5.0;
    public static final String[] q17_i_category_IN = new String[]{"Books", "Music"};

    public static void main(String[] args) throws Exception {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

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
                .join(customers_address)
                .where(1)
                .equalTo(0)
                .with(new CustomersJoinCustomerAddress());


        DataSet<Tuple1<Double>> promotional_sales =
        store_sales
                .join(date_dim) //JOIN date_dim dd ON ss.ss_sold_date_sk = dd.d_date_sk
                .where(0)
                .equalTo(0)
                .with(new StoreSalesJoinDateDim())
                .join(items)
                .where(1)
                .equalTo(0)
                .with(new StoreSalesJoinItem())
                .join(store)
                .where(3)
                .equalTo(0)
                .with(new StoreSalesJoinStore())
                .join(promotion_q1)
                .where(4)
                .equalTo(0)
                .with(new StoreSalesJoinPromotionQ1())
                .join(cs)
                .where(2)
                .equalTo(0)
                .with(new StoreSalesJoinCs())
                .aggregate(Aggregations.SUM, 5)
                .project(5);

        DataSet<Tuple1<Double>> all_sales =
        store_sales
                .join(date_dim) //JOIN date_dim dd ON ss.ss_sold_date_sk = dd.d_date_sk
                .where(0)
                .equalTo(0)
                .with(new StoreSalesJoinDateDim())
                .join(items)
                .where(1)
                .equalTo(0)
                .with(new StoreSalesJoinItem())
                .join(store)
                .where(3)
                .equalTo(0)
                .with(new StoreSalesJoinStore())
                .join(promotion)
                .where(4)
                .equalTo(0)
                .with(new StoreSalesJoinPromotionQ1())
                .join(cs)
                .where(2)
                .equalTo(0)
                .with(new StoreSalesJoinCs())
                .aggregate(Aggregations.SUM, 5)
                .project(5);

        //Tuple3<Double, Double, Double> result =
                promotional_sales
                .cross(all_sales)
                .with(new ComputeRatio())
                .print();


        // execute program


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

    public static class CustomersJoinCustomerAddress implements JoinFunction<Customer, CustomerAddress, Customer> {
        @Override
        public Customer join(Customer c, CustomerAddress ca) throws Exception {
            return new Customer(c.getCustomer(), c.getAddress());
        }
    }

    public static class StoreSalesJoinDateDim
            implements JoinFunction<StoreSales, DateDim, StoreSales>{

        @Override
        public StoreSales join(StoreSales ss, DateDim dd) throws Exception {
            return new StoreSales(ss.getDate(), ss.getItem(), ss.getCustomer(), ss.getStore(), ss.getPromo(), ss.getPrice());
        }
    }


    public static class StoreSalesJoinItem
            implements JoinFunction<StoreSales, Item, StoreSales>{

        @Override
        public StoreSales join(StoreSales ss, Item i) throws Exception {
            return new StoreSales(ss.getDate(), ss.getItem(), ss.getCustomer(), ss.getStore(), ss.getPromo(), ss.getPrice());
        }
    }

    public static class StoreSalesJoinStore
            implements JoinFunction<StoreSales, Store, StoreSales>{

        @Override
        public StoreSales join(StoreSales ss, Store s) throws Exception {
            return new StoreSales(ss.getDate(), ss.getItem(), ss.getCustomer(), ss.getStore(), ss.getPromo(), ss.getPrice());
        }
    }

    public static class StoreSalesJoinPromotionQ1
            implements JoinFunction<StoreSales, Promotion, StoreSales>{
        @Override
        public StoreSales join(StoreSales ss, Promotion p) throws Exception {
            return new StoreSales(ss.getDate(), ss.getItem(), ss.getCustomer(), ss.getStore(), ss.getPromo(), ss.getPrice());
        }
    }

    public static class StoreSalesJoinCs
            implements JoinFunction<StoreSales, Customer, StoreSales>{
        @Override
        public StoreSales join(StoreSales ss, Customer c) throws Exception {
            return new StoreSales(ss.getDate(), ss.getItem(), ss.getCustomer(), ss.getStore(), ss.getPromo(), ss.getPrice());
        }
    }

    public static class ComputeRatio
            implements CrossFunction<Tuple1<Double>, Tuple1<Double>, Tuple3<Double, Double, Double>> {

        @Override
        public Tuple3<Double, Double, Double> cross(Tuple1<Double> ps, Tuple1<Double> as) {
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

    private static DataSet<StoreSales> getStoreSalesDataSet(ExecutionEnvironment env) {
        return env.readCsvFile(store_sales_path)
                .fieldDelimiter("|")
                .includeFields(store_sales_mask)
                .ignoreInvalidLines()
                .tupleType(StoreSales.class);
    }

    private static DataSet<DateDim> getDateDimDataSet(ExecutionEnvironment env) {
        return env.readCsvFile(date_dim_path)
                .fieldDelimiter("|")
                .includeFields(date_dim_mask)
                .tupleType(DateDim.class)
                .filter(new FilterDateDim());
    }

    private static DataSet<Item> getItemDataSet(ExecutionEnvironment env) {
        return env.readCsvFile(items_path)
                .fieldDelimiter("|")
                .includeFields(items_mask)
                .ignoreInvalidLines()
                .tupleType(Item.class)
                .filter(new FilterCategory());
    }

    private static DataSet<Store> getStoreDataSet(ExecutionEnvironment env) {
        return env.readCsvFile(store_path)
                .fieldDelimiter("|")
                .includeFields(store_mask)
                .ignoreInvalidLines()
                .tupleType(Store.class)
                .filter(new FilterOffset());
    }

    private static DataSet<Promotion> getPromotionDataSet(ExecutionEnvironment env) {
        return env.readCsvFile(promotion_path)
                .fieldDelimiter("|")
                .includeFields(promotion_mask)
                .ignoreInvalidLines()
                .tupleType(Promotion.class);
    }

    private static DataSet<Customer> getCustomerDataSet(ExecutionEnvironment env) {
        return env.readCsvFile(customer_path)
                .fieldDelimiter("|")
                .includeFields(customer_mask)
                .ignoreInvalidLines()
                .tupleType(Customer.class);
    }

    private static DataSet<CustomerAddress> getCustomerAddressDataSet(ExecutionEnvironment env) {
        return env.readCsvFile(customer_address_path)
                .fieldDelimiter("|")
                .includeFields(customer_address_mask)
                .ignoreInvalidLines()
                .tupleType(CustomerAddress.class)
                .filter(new FilterCustomerAddress());
    }

}
