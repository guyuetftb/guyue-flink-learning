package com.jerome.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * This is Description
 *
 * @author KONG BIN
 * @date 2019/12/12
 */
public class MyPropertiesUtil {

    public Properties getPropertiesFromFile(String fileName) throws IOException {

        Properties prop = new Properties();
        InputStream in = this.getClass().getResourceAsStream(fileName);
        prop.load(in);
        return prop;
    }

    public static void main(String[] args) throws IOException {
        MyPropertiesUtil myPropertiesUtil = new MyPropertiesUtil();
        Properties prop = myPropertiesUtil.getPropertiesFromFile("/kafka.properties");
        String abc = prop.getProperty("abc");
        System.out.println(abc);

    }


}
