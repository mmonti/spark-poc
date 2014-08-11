package org.mmonti.poc.spark.actions;

import org.apache.spark.api.java.function.Function2;

/**
 * Created by mmonti on 8/11/14.
 */
public class SumLength implements Function2<Integer, Integer, Integer> {

    @Override
    public Integer call(final Integer first, final Integer second) throws Exception {
        return first + second;
    }

}
