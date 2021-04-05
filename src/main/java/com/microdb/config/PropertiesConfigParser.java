package com.microdb.config;

import com.microdb.exception.ParseException;

import java.io.InputStream;
import java.util.Properties;

/**
 * Properties 解析器
 *
 * @author zhangjw
 * @version 1.0
 */
public class PropertiesConfigParser {

    /**
     * 解析配置
     *
     * @param filePath 配置文件路径
     */
    public DBConfig parse(String filePath) {
        try {
            InputStream in = getClass().getClassLoader().getResourceAsStream(filePath);
            Properties props = new Properties();
            props.load(in);
            int pageSize = Integer.parseInt(props.getProperty("page_size", "4096"));
            int bufferPoolCapacity = Integer.parseInt(props.getProperty("buffer_pool_capacity", "100"));
            // load 文本
            // String text = load(filePath);
            // 文本parse，创建config
            return new DBConfig(pageSize, bufferPoolCapacity);
        } catch (Exception e) {
            throw new ParseException("配置解析失败", e);
        }

    }

}
