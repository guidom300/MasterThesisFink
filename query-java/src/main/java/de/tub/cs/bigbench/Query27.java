package de.tub.cs.bigbench;

import com.sun.tools.hat.internal.model.JavaObject;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;

import io.bigdatabenchmark.v1.queries.q27.CompanyUDF;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

/**
 * Created by gm on 13/11/15.
 */
public class Query27 {

    public static final String product_reviews_mask = "10001001";

    public static final String product_reviews_path = "/Users/gm/bigbench/data-generator/output/product_reviews.dat";

    // Conf
    public static final Long q27_pr_item_sk = (long)10002;

    public static void main(String[] args) throws Exception {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //product_reviews -> pr_review_sk (Long), pr_item_sk (Long), pr_review_content (String)
        DataSet<ProductReview> product_reviews = getProductReviewDataSet(env);

        CompanyUDF C = new CompanyUDF();

        PrimitiveObjectInspector stringOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        PrimitiveObjectInspector longOI = PrimitiveObjectInspectorFactory.javaLongObjectInspector;
        PrimitiveObjectInspector longOI2 = PrimitiveObjectInspectorFactory.javaLongObjectInspector;

        StandardStructObjectInspector resultInspector = (StandardStructObjectInspector) C.initialize(new ObjectInspector[]{longOI, longOI2, stringOI});

        //Object result = C.process(new Object[]{(long)1, (long)2, "a"});
        //ObjectInspector listOI = ObjectInspectorFactory.getStandardListObjectInspector(stringOI);


        // execute program


    }

    // *************************************************************************
    //     DATA TRASFORMATIONS
    // *************************************************************************

    public static class FilterReviewsItem implements FilterFunction<ProductReview> {
        @Override
        public boolean filter(ProductReview pr) throws Exception {
            return pr.getItem().equals(q27_pr_item_sk);
        }
    }

    // *************************************************************************
    //     DATA TYPES
    // *************************************************************************

    public static class ProductReview extends Tuple3<Long, Long, String> {

        public ProductReview() { }

        public ProductReview(Long pr_review_sk, Long pr_item_sk, String pr_review_content) {
            this.f0 = pr_review_sk;
            this.f1 = pr_item_sk;
            this.f2 = pr_review_content;
        }

        public Long getReview() { return this.f0; }
        public Long getItem() { return this.f1; }
        public String getContent() { return this.f2; }
    }

    // *************************************************************************
    //     UTIL METHODS
    // *************************************************************************

    private static DataSet<ProductReview> getProductReviewDataSet(ExecutionEnvironment env) {
        return env.readCsvFile(product_reviews_path)
                .fieldDelimiter("|")
                .includeFields(product_reviews_mask)
                .ignoreInvalidLines()
                .tupleType(ProductReview.class)
                .filter(new FilterReviewsItem());
    }
}
