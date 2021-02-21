import com.microdb.model.DataBase;
import com.microdb.model.DbTable;
import com.microdb.model.TableDesc;
import com.microdb.model.Tuple;
import com.microdb.model.dbfile.DbTableFile;
import com.microdb.model.field.FieldType;
import com.microdb.model.field.IntField;
import com.microdb.model.page.Page;
import com.microdb.model.page.PageID;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

/**
 * test
 *
 * @author zhangjw
 * @version 1.0
 */
public class DataBaseTest {

    public DataBase dataBase;

    @Before
    public void initDataBase() {
        DataBase dataBase = DataBase.getInstance();
        // 创建数据库文件
        String fileName = UUID.randomUUID().toString();
        DbTableFile dbTableFile = new DbTableFile(new File(fileName));
        List<TableDesc.Attribute> attributes = Arrays.asList(new TableDesc.Attribute("f1", FieldType.INT));
        TableDesc tableDesc = new TableDesc(attributes);
        // tableDesc
        dataBase.addTable(dbTableFile, "t_person", tableDesc);

        this.dataBase = dataBase;
    }
    //
    // @Test
    // public void writePageToDisk() {
    //     DataBase dataBase = new DataBase();
    //     // 创建数据库文件
    //     // 路径修改
    //     DbTableFile dbTableFile = new DbTableFile(new File("db_file.txt"));
    //     List<TableDesc.Attribute> attributes = Arrays.asList(new TableDesc.Attribute("f1", FieldType.INT));
    //     TableDesc tableDesc = new TableDesc(attributes);
    //     // tableDesc
    //     dataBase.addTable(dbTableFile, "t_person", tableDesc);
    //     Page page = new Page(0, new byte[]{});
    //     // 数据写入file
    //     dbTableFile.writePageToDisk(page);
    // }

    @Test
    public void insertRow() throws IOException {
        DbTable tablePerson = this.dataBase.getDbTableByName("t_person");
        Tuple tuple = new Tuple(tablePerson.getTableDesc());
        tablePerson.insertRow(tuple);
    }

    /**
     * 一页4096字节，表t_person有一个int型字段，占用4字节，slot状态占用1字节
     * 4096/(4+1) = 819(向下取整)，级每页可容纳819行
     */
    @Test
    public void testCalculateMaxSlotNum() throws IOException {
        DbTable tablePerson = this.dataBase.getDbTableByName("t_person");
        Page page = new Page(new PageID(tablePerson.getTableId(), 0), Page.createEmptyPageData());
        int i = page.calculateMaxSlotNum(tablePerson.getTableDesc());
        System.out.println(i);
        Assert.assertEquals(819, i);
    }
    /**
     * t_person 表只有一个int类型字段
     */
    @Test
    public void insertRowTest() throws IOException {
        DbTable tablePerson = this.dataBase.getDbTableByName("t_person");
        Tuple tuple = new Tuple(tablePerson.getTableDesc());

        // 第1页
        for (int i = 0; i < 819; i++) {
            tuple.setField(0, new IntField(i));
            tablePerson.insertRow(tuple);
            int existPageCount = tablePerson.getDbTableFile().getExistPageCount();
            Assert.assertEquals(1, existPageCount);
        }

        // 第2页
        for (int i = 0; i < 819; i++) {
            tuple.setField(0, new IntField(i));
            tablePerson.insertRow(tuple);
            int existPageCount = tablePerson.getDbTableFile().getExistPageCount();
            Assert.assertEquals(2, existPageCount);
        }

        tuple.setField(0, new IntField(1000));
        tablePerson.insertRow(tuple);
        int existPageCount = tablePerson.getDbTableFile().getExistPageCount();
        Assert.assertEquals(3, existPageCount);

    }
}