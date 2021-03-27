package transaction;

import com.microdb.bufferpool.BufferPool;
import com.microdb.connection.Connection;
import com.microdb.model.DataBase;
import com.microdb.model.Row;
import com.microdb.model.TableDesc;
import com.microdb.model.dbfile.HeapTableFile;
import com.microdb.model.dbfile.TableFile;
import com.microdb.model.field.FieldType;
import com.microdb.model.field.IntField;
import com.microdb.operator.Delete;
import com.microdb.operator.SeqScan;
import com.microdb.transaction.Transaction;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

/**
 * 锁
 *
 * @author zhangjw
 * @version 1.0
 */
public class LockTest {
    public DataBase dataBase;

    private TableDesc personTableDesc;
    BufferPool bufferPool;

    @Before
    public void initDataBase() {
        DataBase dataBase = DataBase.getInstance();
        bufferPool = DataBase.getBufferPool();
        // 创建数据库文件
        String fileName = UUID.randomUUID().toString();

        // 表中有两个int类型字段，person_id 字段为索引字段
        List<TableDesc.Attribute> attributes = Arrays.asList(new TableDesc.Attribute("person_id", FieldType.INT),
                new TableDesc.Attribute("person_age", FieldType.INT));

        File file = new File(fileName);
        file.deleteOnExit();
        TableDesc tableDesc = new TableDesc(attributes);
        TableFile tableFile = new HeapTableFile(file, tableDesc);
        dataBase.addTable(tableFile, "t_person");
        this.dataBase = dataBase;

        personTableDesc = tableDesc;
    }

    @Test
    public void testInsertAndDeleteWithTransaction() throws IOException {
        Transaction transaction = new Transaction();
        Connection.passingTransaction(transaction.getTransactionId());
        TableFile tableFile = dataBase.getDbTableByName("t_person").getTableFile();

        int num = 5;
        long l1 = System.currentTimeMillis();
        for (int i = 1; i <= num; i++) {
            Row row = new Row(personTableDesc);
            row.setField(0, new IntField(i));
            row.setField(1, new IntField(18));
            DataBase.getBufferPool().insertRow(row, "t_person");
        }

        System.out.println("开始打印表数据====");

        SeqScan scan = new SeqScan(tableFile.getTableId());

        scan.open();
        while (scan.hasNext()) {
            System.out.println(scan.next());
        }

        // 通过Delete操作符删除，执行非常快速
        //
        Delete delete = new Delete(scan);
        delete.loopDelete();

        scan.open();
        Assert.assertFalse(scan.hasNext());
        System.out.println("耗时ms:" + (System.currentTimeMillis() - l1));

    }

    @Test
    public void testTransactionCommit() throws IOException {
        Transaction transaction = new Transaction();
        Connection.passingTransaction(transaction.getTransactionId());
        TableFile tableFile = dataBase.getDbTableByName("t_person").getTableFile();
        int num = 30000;
        long l1 = System.currentTimeMillis();
        for (int i = 1; i <= num; i++) {
            Row row = new Row(personTableDesc);
            row.setField(0, new IntField(i));
            row.setField(1, new IntField(18));
            DataBase.getBufferPool().insertRow(row, "t_person");
        }

        System.out.println("开始打印表数据====");
        SeqScan scan = new SeqScan(tableFile.getTableId());
        scan.open();
        while (scan.hasNext()) {
            System.out.println(scan.next());
        }
        System.out.println("耗时ms:" + (System.currentTimeMillis() - l1));


        // 添加不经过缓存的读取文件 用于测试

        scan.open();
        Assert.assertFalse(scan.hasNext());


    }


}
