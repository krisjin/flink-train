package org.datacogn.flink.test;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * User:shijingui
 * Date:2019-07-22
 */
public class Con {


    public static void main(String[] args) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date d = new Date(1563890650000l);
        System.out.println(sdf.format(d));
    }
}
