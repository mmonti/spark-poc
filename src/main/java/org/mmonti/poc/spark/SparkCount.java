package org.mmonti.poc.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.mmonti.poc.spark.actions.SumLength;
import org.mmonti.poc.spark.transformations.GetLength;

/**
 * Created by mmonti on 8/11/14.
 */
public class SparkCount {

    private static final String APP_NAME = "SparkCount";
    private static final String MASTER = "local";

    private final SparkConf configuration;
    private final JavaSparkContext context;

    public SparkCount() {
        this.configuration = new SparkConf(true);
        this.configuration.setAppName(APP_NAME);
        this.configuration.setMaster(MASTER);

        this.context = new JavaSparkContext(this.configuration);
    }

    public int count(String file) {
        final JavaRDD<String> rdd = this.context.textFile(file);
        return rdd.map(new GetLength()).reduce(new SumLength());
    }

    public static void main(String[] args) {
        final SparkCount counter = new SparkCount();
        int result = counter.count("./src/main/resources/info.txt");
        System.out.println("Total count in file = ["+result+"]");
    }
}
