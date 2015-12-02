package de.tub.cs.bigbench;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.operators.base.JoinOperatorBase;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Arrays;

import static org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint.*;

/**
 * Created by gm on 27/11/15.
 */
public class Query05 {
    //Config
    public static final String[] q05_cd_education_status_IN = new String[]{"Advanced Degree", "College", "4 yr Degree", "2 yr Degree"};
    public static final String q05_cd_gender = "M";
    public static final String q05_i_category = "Books";

    //Mapping
    public static String input_path;
    public static String output_path;
    public static String items_path;
    public static String customers_path;
    public static String customers_demographics_path;
    public static String web_clickstreams_path;

    public static String q05_items_mask;
    public static String q05_customers_mask;
    public static String q05_customers_demographics_mask;
    public static String q05_web_clickstreams_mask;


    public static void main(String[] args) throws Exception {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        PropertiesConfiguration config = new PropertiesConfiguration("config.properties");

        if(parseParameters(args) == 1)
            return;

        items_path = input_path + "/item/item.dat";
        customers_path = input_path + "/customer/customer.dat";
        customers_demographics_path = input_path + "/customer_demographics/customer_demographics.dat";
        web_clickstreams_path = input_path + "/web_clickstreams/web_clickstreams.dat";

        q05_items_mask = config.getString("q05_items_mask");
        q05_customers_mask = config.getString("q05_customers_mask");
        q05_customers_demographics_mask = config.getString("q05_customers_demographics_mask");
        q05_web_clickstreams_mask = config.getString("q05_web_clickstreams_mask");

        // item -> i_item_sk (Long), i_category_id (Integer), i_category (String)
        DataSet<Item> items = getItemDataSet(env);

        // customers -> c_customer_sk (Long), c_current_cdemo_sk (Long)
        DataSet<Customer> customers = getCustomerDataSet(env);

        //customers_demographics -> cd_demo_sk (Long), cd_gender (String), cd_education_status (String)
        DataSet<CustomerDemographics> customers_demographics = getCustomerDemographicsDataSet(env);

        // web_clickstreams -> wcs_item_sk (Long), wcs_user_sk (Long)
        DataSet<WebClickStream> web_clickstreams = getWebClickStreamDataSet(env);

        DataSet<Tuple3<Long, String, String>> cd =
                customers
                        .join(customers_demographics, REPARTITION_HASH_FIRST)
                        .where(1)
                        .equalTo(0)
                        .with(new CTJoinCDT());

        DataSet<TempTable> q05_user_clicks_in_cat =
                items
                        .joinWithHuge(web_clickstreams)
                        .where(0)
                        .equalTo(0)
                        .with(new ITJoinWS())
                        .groupBy(0)
                        .reduceGroup(new ReduceClicks());

        DataSet<Tuple10<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> result =
                q05_user_clicks_in_cat
                        .join(cd)
                        .where(0)
                        .equalTo(0)
                        .with(new TTJoinCD());

        result.writeAsCsv(output_path, "\n", ",", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute("Query 05");

    }

    // *************************************************************************
    //     DATA TRASFORMATIONS
    // *************************************************************************

    @FunctionAnnotation.ForwardedFieldsFirst("f0")
    @FunctionAnnotation.ForwardedFieldsSecond("f1; f2")
    public static class CTJoinCDT
            implements JoinFunction<Customer, CustomerDemographics, Tuple3<Long, String, String>> {

        @Override
        public Tuple3<Long, String, String> join(Customer customer, CustomerDemographics customerDemographics) throws Exception {
            return new Tuple3<>(customer.f0, customerDemographics.f1, customerDemographics.f2);
        }
    }

    @FunctionAnnotation.ForwardedFieldsFirst("f1; f2")
    @FunctionAnnotation.ForwardedFieldsSecond("f1->f0")
    public static class ITJoinWS
            implements JoinFunction<Item, WebClickStream, Tuple3<Long, Integer, String>> {
        @Override
        public Tuple3<Long, Integer, String> join(Item it, WebClickStream wcs) throws Exception {
            return new Tuple3<>(wcs.f1, it.f1, it.f2);
        }
    }

    public static class ReduceClicks implements GroupReduceFunction<Tuple3<Long, Integer, String>,
            TempTable> {
        @Override
        public void reduce(Iterable<Tuple3<Long, Integer, String>> in, Collector<TempTable> out) throws Exception {
            Long wcs_user_sk = null;
            Integer[] integers = new Integer[8];
            Arrays.fill(integers, 0);
            //    List<Integer> clicks = Arrays.asList(integers);

            for (Tuple3<Long, Integer, String> curr : in) {
                wcs_user_sk = curr.f0;
                if(curr.f2.equals(q05_i_category))
                    integers[0]++;
                //    clicks.set(0, clicks.get(0) + 1);
                if(curr.f1 < 8)
                    integers[curr.f1]++;
                //    clicks.set(curr.f1, clicks.get(curr.f1) + 1);
            }
            out.collect(new TempTable(wcs_user_sk, integers));
        }
    }

    public static class TTJoinCD
            implements JoinFunction<TempTable, Tuple3<Long, String, String>,
            Tuple10<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> {
        @Override
        public Tuple10<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>
        join(TempTable tt, Tuple3<Long, String, String> cd) throws Exception {
            return new Tuple10<>(tt.getClicks()[0],(Arrays.asList(q05_cd_education_status_IN).contains(cd.f2))? 1 : 0,
                    (cd.f1.equals(q05_cd_gender))? 1 : 0, tt.getClicks()[1], tt.getClicks()[2], tt.getClicks()[3],
                    tt.getClicks()[4], tt.getClicks()[5], tt.getClicks()[6], tt.getClicks()[7]);
        }
    }

    // *************************************************************************
    //     DATA TYPES
    // *************************************************************************
    public static class Item extends Tuple3<Long, Integer, String> {

        public Long getItem() { return this.f0; }
        public Integer getCategoryID() { return this.f1; }
        public String getCategory() { return this.f2; }
    }

    public static class Customer extends Tuple2<Long, Long> {

        public Customer() { }

        public Customer(Long c_customer_sk, Long c_current_cdemo_sk) {
            this.f0 = c_customer_sk;
            this.f1 = c_current_cdemo_sk;
        }

        public Long getCustomer() { return this.f0; }
        public Long getCustoemrDemo() { return this.f1; }
    }

    public static class CustomerDemographics extends Tuple3<Long, String, String>{

        public CustomerDemographics() {

        }

        public CustomerDemographics(Long cd_demo_sk, String cd_gender, String cd_education_status) {
            this.f0 = cd_demo_sk;
            this.f1 = cd_gender;
            this.f2 = cd_education_status;
        }

        public Long getCustoemrDemo() { return this.f0; }
        public String getGender() { return this.f1; }
        public String getEducationStatus() { return this.f2; }
    }

    public static class WebClickStream extends Tuple2<Long, Long> {

        public WebClickStream() { }

        public WebClickStream(Long wcs_item_sk, Long wcs_user_sk) {
            this.f0 = wcs_item_sk;
            this.f1 = wcs_user_sk;
        }

        public Long getItem() { return this.f0; }
        public Long getUser() { return this.f1; }
    }

    public static class TempTable extends Tuple2<Long, Integer[]> {

        public TempTable() { }

        public TempTable(Long wcs_user_sk, Integer[] clicks) {
            this.f0 = wcs_user_sk;
            this.f1 = clicks;
        }

        public Long getItem() { return this.f0; }
        public Integer[] getClicks() { return this.f1; }
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

    private static DataSet<Item> getItemDataSet(ExecutionEnvironment env) {
        return env.readCsvFile(items_path)
                .lineDelimiter("\n")
                .fieldDelimiter('|')
                .includeFields(q05_items_mask)
                .ignoreInvalidLines()
                .tupleType(Item.class);
    }

    private static DataSet<Customer> getCustomerDataSet(ExecutionEnvironment env) {
        return env.readCsvFile(customers_path)
                .lineDelimiter("\n")
                .fieldDelimiter('|')
                .includeFields(q05_customers_mask)
                .ignoreInvalidLines()
                .tupleType(Customer.class);
    }

    private static DataSet<CustomerDemographics> getCustomerDemographicsDataSet(ExecutionEnvironment env) {
        return env.readCsvFile(customers_demographics_path)
                .lineDelimiter("\n")
                .fieldDelimiter('|')
                .includeFields(q05_customers_demographics_mask)
                .ignoreInvalidLines()
                .tupleType(CustomerDemographics.class);
    }

    private static DataSet<WebClickStream> getWebClickStreamDataSet(ExecutionEnvironment env) {
        return env.readCsvFile(web_clickstreams_path)
                .lineDelimiter("\n")
                .fieldDelimiter('|')
                .includeFields(q05_web_clickstreams_mask)
                .ignoreInvalidLines()
                .tupleType(WebClickStream.class);
    }
}
