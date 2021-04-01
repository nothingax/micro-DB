package logging;

import com.microdb.bufferpool.BufferPool;
import com.microdb.connection.Connection;
import com.microdb.model.DataBase;
import com.microdb.model.Row;
import com.microdb.model.TableDesc;
import com.microdb.model.dbfile.HeapTableFile;
import com.microdb.model.dbfile.TableFile;
import com.microdb.model.field.FieldType;
import com.microdb.model.field.IntField;
import com.microdb.operator.SeqScan;
import com.microdb.transaction.Lock;
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
 * RedoLogTest
 *
 * @author zhangjw
 * @version 1.0
 */
public class RedoLogTest {

    public DataBase dataBase;
    private TableDesc personTableDesc;
    private BufferPool bufferPool;
    private int tableId;
    private TableFile tableFile;

    @Before
    public void initDataBase() throws IOException {
        DataBase dataBase = DataBase.getInstance();
        bufferPool = DataBase.getBufferPool();
        // 创建数据库文件
        String fileName = UUID.randomUUID().toString();

        // 表中有两个int类型字段，person_id 字段为索引字段
        List<TableDesc.Attribute> attributes = Arrays.asList(new TableDesc.Attribute("person_id", FieldType.INT),
                new TableDesc.Attribute("person_age", FieldType.INT));

        File file = new File(fileName);
        // file.deleteOnExit();
        TableDesc tableDesc = new TableDesc(attributes);
        TableFile tableFile = new HeapTableFile(file, tableDesc);
        dataBase.addTable(tableFile, "t_person");
        this.tableId = tableFile.getTableId();
        personTableDesc = tableDesc;
        this.dataBase = dataBase;
        this.tableFile = tableFile;
    }


    /**
     * 测试崩溃恢复
     * 写入数据并提交事务后，脏页未刷盘，数据库崩溃。
     * 数据库重启后，会从redo log日志中恢复数据到数据磁盘
     */
    @Test
    public void testCrashRecover() throws IOException {
        // 表中初始化数据
        int num = 10;
        Transaction transaction = new Transaction(Lock.LockType.XLock);
        transaction.start();
        Connection.passingTransaction(transaction);
        for (int i = 1; i <= num; i++) {
            Row row = new Row(personTableDesc);
            row.setField(0, new IntField(i));
            row.setField(1, new IntField(18));
            DataBase.getBufferPool().insertRow(row, "t_person");
        }
        transaction.commit();

        // 模拟数据库崩溃后重启
        DataBase.resetWithFile(tableFile, "t_person");

        // 从redo 日志中恢复数据
        DataBase.getRedoLogFile().recover();

        // 验证数据已写入磁盘
        Transaction t2 = new Transaction(Lock.LockType.SLock);
        Connection.passingTransaction(t2);
        SeqScan scan = new SeqScan(this.tableId);
        scan.open();
        Assert.assertTrue(scan.hasNext());
        t2.commit();
    }

}
