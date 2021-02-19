package com.microdb.model;

import com.microdb.model.dbfile.DbTableFile;
import com.microdb.model.field.FieldType;
import com.microdb.model.page.Page;

import java.io.File;
import java.util.Arrays;
import java.util.List;

/**
 * 访问客户地段helper，用于调试
 *
 * @author zhangjw
 * @version 1.0
 */
public class ClientHelper {
    public static void main(String[] args) {

        DataBase dataBase = new DataBase();
        // 创建数据库文件
        // 路径修改
        DbTableFile dbTableFile = new DbTableFile(new File("/Users/zhangjianwei/IdeaProjects/cs/mine/micro-DB/db_file.txt"));
        List<TableDesc.Attribute> attributes = Arrays.asList(new TableDesc.Attribute("f1", FieldType.INT));
        TableDesc tableDesc = new TableDesc(attributes);
        // tableDesc
        dataBase.addTable(dbTableFile, "t_person", tableDesc);
        Page page = new Page(0, new byte[]{});
        // 数据写入file
        dbTableFile.writePageToDisk(page);
    }

    /**
     * file readPage 返回page
     *
     *
     *
     *
     */




}
