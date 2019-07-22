package org.datacogn.flink.wordcount;

import java.util.HashMap;
import java.util.Map;

/**
 * User:shijingui
 * Date:2019-07-22
 */
public class Acc {

    public Map<String, Integer> count = new HashMap<>();


    public Map<String, Integer> getCount() {
        return count;
    }

    public void setCount(Map<String, Integer> count) {
        this.count = count;
    }
}
