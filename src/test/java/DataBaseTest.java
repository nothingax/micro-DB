import com.microdb.bufferpool.BufferPool;
import com.microdb.connection.Connection;
import com.microdb.model.DataBase;
import com.microdb.model.table.DbTable;
import com.microdb.model.row.Row;
import com.microdb.model.table.TableDesc;
import com.microdb.model.table.tablefile.HeapTableFile;
import com.microdb.model.table.tablefile.TableFile;
import com.microdb.model.field.FieldType;
import com.microdb.model.field.IntField;
import com.microdb.model.page.heap.HeapPage;
import com.microdb.model.page.heap.HeapPageID;
import com.microdb.transaction.Lock;
import com.microdb.transaction.Transaction;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * test
 *
 * @author zhangjw
 * @version 1.0
 */
public class DataBaseTest {

    public DataBase dataBase;
    private BufferPool bufferPool;

    @Before
    public void initDataBase() {
        DataBase dataBase = DataBase.getInstance();
        // 创建数据库文件
        String fileName = "person";
        List<TableDesc.Attribute> attributes = Arrays.asList(new TableDesc.Attribute("f1", FieldType.INT));
        TableDesc tableDesc = new TableDesc(attributes);
        File file = new File(fileName);
        file.deleteOnExit();
        TableFile tableFile = new HeapTableFile(dataBase,file, tableDesc);

        // tableDesc
        dataBase.addTable(tableFile, "t_person", tableDesc);

        this.dataBase = dataBase;
        this.bufferPool = dataBase.getBufferPool();

    }
    @Test
    public void insertRow() throws IOException {
        Transaction transaction = new Transaction(Lock.LockType.XLock);
        transaction.start();
        Connection.passingTransaction(transaction);

        DbTable tablePerson = this.dataBase.getDbTableByName("t_person");
        Row row = new Row(tablePerson.getTableDesc());
        row.setField(0, new IntField(0));
        bufferPool.insertRow(row, "t_person");

        transaction.commit();
    }

    /**
     * 一页4096字节，表t_person有一个int型字段，占用4字节，slot状态占用1字节
     * 4096/(4+1) = 819(向下取整)，即每页可容纳819行
     */
    @Test
    public void testCalculateMaxSlotNum() throws IOException {
        DbTable tablePerson = this.dataBase.getDbTableByName("t_person");
        HeapPage heapPage = new HeapPage(dataBase,new HeapPageID(tablePerson.getTableId(), 0), HeapPage.createEmptyPageData(dataBase.getDBConfig().getPageSizeInByte()));
        int i = heapPage.calculateMaxSlotNum(tablePerson.getTableDesc());
        System.out.println(i);
        Assert.assertEquals(819, i);
    }

    /**
     * t_person 表只有一个int类型字段
     */
    @Test
    public void insertRowTest() throws IOException {
        DbTable tablePerson = this.dataBase.getDbTableByName("t_person");
        Row row = new Row(tablePerson.getTableDesc());

        Transaction transaction = new Transaction(Lock.LockType.XLock);
        transaction.start();
        Connection.passingTransaction(transaction);

        // 第1页
        for (int i = 0; i < 819; i++) {
            row.setField(0, new IntField(i));
            tablePerson.insertRow(row);
            int existPageCount = tablePerson.getTableFile().getExistPageCount();
            Assert.assertEquals(1, existPageCount);
        }

        // 第2页
        for (int i = 0; i < 819; i++) {
            row.setField(0, new IntField(i));
            tablePerson.insertRow(row);
            int existPageCount = tablePerson.getTableFile().getExistPageCount();
            Assert.assertEquals(2, existPageCount);
        }

        row.setField(0, new IntField(1000));
        tablePerson.insertRow(row);
        int existPageCount = tablePerson.getTableFile().getExistPageCount();
        Assert.assertEquals(3, existPageCount);

        transaction.commit();
    }
}
