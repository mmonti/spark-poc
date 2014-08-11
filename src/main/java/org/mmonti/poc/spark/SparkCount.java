package org.mmonti.poc.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

/**
 * Created by mmonti on 8/11/14.
 */
public class SparkCount {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf(true).setAppName("SparkCount").setMaster("local");
        JavaSparkContext context = new JavaSparkContext(conf);

        JavaRDD<String> rdd = context.textFile("./src/main/resources/info.txt", 2);
        JavaRDD<Integer> lengths = rdd.map(new Function<String, Integer>() {
            @Override
            public Integer call(String string) throws Exception {
                return string.length();
            }
        });
        int total = lengths.reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer+integer2;
            }
        });
        System.out.println("Total count="+total);
    }
}
