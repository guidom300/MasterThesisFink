package org.apache.flink.examples.java;

import com.sun.tools.javac.util.List;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.table.Row;
import org.apache.flink.api.table.Table;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.table.TableEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;


import java.util.*;
import java.util.stream.Collector;

/**
 * Created by gm on 15/10/15.
 *
 * SELECT pid1 AS item1 , pid2 AS item2 , COUNT (*) AS cnt
 *  FROM basket_generator (ON
 *      (SELECT s.ss_ticket_number AS oid, s.ss_item_sk AS pid
 *          FROM store_sales100 s
 *       INNER JOIN item100 i ON s.ss_item_sk = i_item_sk
 *       WHERE i.i_category_id in (1,4,6) and s.ss_store_sk = 10
 *       PARTITION BY oid
 *       basket_size (2)
 *       basket_item(’pid’)
 *       item_set_max (500)
 *       )
 *   GROUP BY 1,2
 * HAVING COUNT(pid1) > 49
 *  ORDER BY 1,3,2;

 */
public class Query1 {

    public static class Sale {
        public Long ss_ticket_number;
        public Long ss_item_sk;

        // Public constructor to make it a Flink POJO
        public Sale() {

        }

        public Sale(Long ss_ticket_number, Long ss_item_sk) {
            this.ss_ticket_number = ss_ticket_number;
            this.ss_item_sk = ss_item_sk;
        }

        @Override
        public String toString() {
            return ss_ticket_number + " " + ss_item_sk;
        }

    }


    public static class DistinctReduce implements GroupReduceFunction<Sale, SortedSet<Long>> {

        @Override
        public void reduce(Iterable<Sale> in, org.apache.flink.util.Collector<SortedSet<Long>> out) throws Exception {

            SortedSet<Long> uniqItems = new TreeSet<Long>();
            Long key = null;

            // add all i_item_sk of the group to the set
            for (Sale t : in) {
                key = t.ss_ticket_number;
                uniqItems.add(t.ss_item_sk);
            }

            // emit all unique i_item_sk.
            out.collect(uniqItems);

        }
    }

    public static class PointAssigner
            implements FlatJoinFunction<Tuple2<Integer, Long>, Tuple2<Integer, Long>, Tuple3<Long, Long, Integer>> {

        @Override
        public void join(Tuple2<Integer, Long> item_a, Tuple2<Integer, Long> item_b, org.apache.flink.util.Collector<Tuple3<Long, Long, Integer>> out) {

            if (item_a.f1 < item_b.f1) {
                out.collect(new Tuple3<>(item_a.f1, item_b.f1, 1));
            }
        }
    }

    public static class FilterCounts implements FilterFunction<Tuple3<Long, Long, Integer>> {
        @Override
        public boolean filter(Tuple3<Long, Long, Integer> t) {
            return t.f2 > 50;
        }
    }

    public static class MakePairs implements FlatMapFunction<SortedSet<Long>, Tuple3<Long, Long, Integer>>
    {
        @Override
        public void flatMap(SortedSet<Long> longs, org.apache.flink.util.Collector<Tuple3<Long, Long, Integer>> out) throws Exception {
            for (Long item_a : longs) {
                for (Long item_b : longs) {
                    if(item_a < item_b)
                    {
                        out.collect(new Tuple3<>(item_a, item_b, 1));
                    }
                }
            }
        }
    }



    public static void main(String[] args) throws Exception {



        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
/*
        //  items : 0 -> i_item_sk, 1 -> i_category_id
        DataSet<Tuple2<Long, Integer>> items =
        env.readCsvFile("/Users/gm/bigbench/data-generator/output/item.dat")
                .lineDelimiter("\n")
                .fieldDelimiter('|')
                .includeFields("1000000000010000000000")  // take the first and the fourth field
                .types(Long.class, Integer.class);

        //sotre_sales 0 -> ss_item_sk, 1 .> ss_store_sk, 2 -> ss_ticket_number
        DataSet<Tuple3<Long, Long, Long>> store_sales =
                env.readCsvFile("/Users/gm/bigbench/data-generator/output/store_sales.dat")
                .lineDelimiter("\n")
                .fieldDelimiter('|')
                        .includeFields("00100001010000000000000")  // take the first and the fourth field
                        .types(Long.class, Long.class, Long.class);
        */

        //ExecutionEnvironment env = ExecutionEnvironment.createCollectionsEnvironment();
        TableEnvironment tableEnv = new TableEnvironment();

        Table items = tableEnv.fromDataSet(env.readCsvFile("/Users/gm/bigbench/data-generator/output/item.dat")
                .lineDelimiter("\n")
                .fieldDelimiter('|')
                .includeFields("1000000000010000000000")  // take the first and the fourth field
                .types(Long.class, Integer.class)).as("i_item_sk, i_category_id");

        Table store_sales = tableEnv.fromDataSet(env.readCsvFile("/Users/gm/bigbench/data-generator/output/store_sales.dat")
                .lineDelimiter("\n")
                .fieldDelimiter('|')
                .includeFields("00100001010000000000000")  // take the first and the fourth field
                .types(Long.class, Long.class, Long.class)).as("ss_item_sk, ss_store_sk, ss_ticket_number");

        Table items_category =
                items
                        .filter("i_category_id = 1 || i_category_id = 2 || i_category_id = 3")
                        .select("i_item_sk, i_category_id");

        Table store_sales_sk =
                store_sales
                        .filter("ss_store_sk = 10 || ss_store_sk = 20 || ss_store_sk = 30"
                                + "|| ss_store_sk = 40 || ss_store_sk = 50")
                        .select("ss_item_sk, ss_store_sk,ss_ticket_number");

        // joined: ss_item_sk: Long, ss_store_sk: Long, ss_ticket_number: Long, i_item_sk: Long, i_category_id: Intege
        Table joined = store_sales_sk.
                join(items_category)
                .where("ss_item_sk = i_item_sk")
                .select("ss_ticket_number, ss_item_sk");

        DataSet<Sale> salesNumber = tableEnv.toDataSet(joined, Sale.class);


        DataSet<SortedSet<Long>> soldItemsPerTicket = salesNumber
                .groupBy("ss_ticket_number")            // group DataSet by the first tuple field
                .reduceGroup(new DistinctReduce());

        DataSet<Tuple3<Long, Long, Integer>>
                pairs = soldItemsPerTicket
                .flatMap(new MakePairs())
                .groupBy(0,1)
                .aggregate(Aggregations.SUM, 2)
                .filter(new FilterCounts())
                .sortPartition(2, Order.DESCENDING).setParallelism(1)
                .first(100);

        pairs.writeAsCsv("/tmp/pairs.csv");

        env.execute("Query1");






 /*               Table mailsPerSenderMonth = tEnv
                        // to table
                        .fromDataSet(monthSender).as("month, sender")
                                // filter out bot email addresses
                        .filter("sender !== 'jira@apache.org' && " +
                                "sender !== 'no-reply@apache.org' && " +
                                "sender !== 'git@git.apache.org'")
                                // count emails per month and email address
                        .groupBy("month, sender").select("month, sender, month.count as cnt");

        Table membersOTMonth = mailsPerSenderMonth
                // find max number of emails sent by an address per month
                .groupBy("month").select("month as m, cnt.max as max")
                        // find email address that sent the most emails in each month
                .join(mailsPerSenderMonth).where("month = m && cnt = max").select("month, sender");
    */


    }
}

