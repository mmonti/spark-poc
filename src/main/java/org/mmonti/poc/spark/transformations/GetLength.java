package org.mmonti.poc.spark.transformations;

import org.apache.spark.api.java.function.Function;

/**
 * Created by mmonti on 8/11/14.
 */
public class GetLength implements Function<String, Integer> {

    @Override
    public Integer call(final String first) throws Exception {
        return first.length();
    }

}
