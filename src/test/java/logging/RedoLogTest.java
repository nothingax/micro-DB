package logging;

import com.microdb.bufferpool.BufferPool;
import com.microdb.connection.Connection;
import com.microdb.model.DataBase;
import com.microdb.model.row.Row;
import com.microdb.model.table.TableDesc;
import com.microdb.model.table.tablefile.HeapTableFile;
import com.microdb.model.table.tablefile.TableFile;
import com.microdb.model.field.FieldType;
import com.microdb.model.field.IntField;
import com.microdb.operator.SeqScan;
import com.microdb.transaction.Lock;
import com.microdb.transaction.Transaction;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

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

    public void initDataBase() throws IOException {
        File redoFile = new File("redo");
        redoFile.delete();

        DataBase.reset();
        DataBase dataBase = DataBase.getInstance();
        bufferPool = DataBase.getBufferPool();
        // 创建数据库文件
        String fileName = "person";

        // 表中有两个int类型字段，person_id 字段为索引字段
        List<TableDesc.Attribute> attributes = Arrays.asList(new TableDesc.Attribute("person_id", FieldType.INT));

        File file = new File(fileName);
        file.deleteOnExit();
        TableDesc tableDesc = new TableDesc(attributes);
        TableFile tableFile = new HeapTableFile(file, tableDesc);
        dataBase.addTable(tableFile, "t_person");
        this.tableId = tableFile.getTableId();
        personTableDesc = tableDesc;
        this.dataBase = dataBase;
        this.tableFile = tableFile;
    }

    @After
    public void after() {
        (new File("redo")).delete();
    }

    /**
     * 提交后 崩溃恢复
     * 写入数据并提交事务后，脏页未刷盘，数据库崩溃。
     * 数据库重启后，会从redo log日志中恢复数据到数据磁盘
     */
    @Test
    public void testCommitCrashRecover() throws IOException {
        initDataBase();
        // 表中初始化数据
        Transaction transaction = new Transaction(Lock.LockType.XLock);
        transaction.start();
        Connection.passingTransaction(transaction);

        // 插入一行
        Row row = new Row(personTableDesc);
        row.setField(0, new IntField(10));
        DataBase.getBufferPool().insertRow(row, "t_person");
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

    /**
     * 未提交 崩溃恢复
     * 未提交事务的数据被刷盘，数据库崩溃恢复后，该部分数据不应存在磁盘中
     */
    @Test
    public void testUnCommitCrashRecover() throws IOException {
        initDataBase();

        // 表中初始化数据
        Transaction transaction = new Transaction(Lock.LockType.XLock);
        transaction.start();
        Connection.passingTransaction(transaction);
        // 插入一行
        Row row = new Row(personTableDesc);
        row.setField(0, new IntField(10));
        DataBase.getBufferPool().insertRow(row, "t_person");

        // transaction.commit();
        // 模拟事务提交前，数据被刷盘,刷盘前记录redo日志
        bufferPool.flushAllPage();
        // 模拟数据库崩溃后重启
        DataBase.resetWithFile(tableFile, "t_person");
        // 从redo 日志中恢复数据
        DataBase.getRedoLogFile().recover();

        // 验证数据已写入磁盘
        Transaction t2 = new Transaction(Lock.LockType.SLock);
        Connection.passingTransaction(t2);
        SeqScan scan = new SeqScan(this.tableId);
        scan.open();
        Assert.assertFalse(scan.hasNext());
        t2.commit();
    }

}
