package integrated.bptree;

import base.TestBase;
import com.microdb.connection.Connection;
import com.microdb.model.DataBase;
import com.microdb.model.field.FieldType;
import com.microdb.model.field.IntField;
import com.microdb.model.row.Row;
import com.microdb.model.table.DbTable;
import com.microdb.model.table.TableDesc;
import com.microdb.model.table.tablefile.BPTreeTableFile;
import com.microdb.operator.Delete;
import com.microdb.operator.PredicateEnum;
import com.microdb.operator.bptree.BPTreeScan;
import com.microdb.operator.bptree.IndexPredicate;
import com.microdb.transaction.Lock;
import com.microdb.transaction.Transaction;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.assertFalse;

/**
 * BPTreeTest
 *
 * @author zhangjw
 * @version 1.0
 */
public class BPTreeTest extends TestBase {
    public DataBase dataBase;

    private TableDesc personTableDesc;

    @Before
    public void initDataBase() {
        DataBase dataBase = DataBase.getInstance();
        // 创建数据库文件
        String fileName = UUID.randomUUID().toString();

        // 表中有两个int类型字段，person_id 字段为索引字段
        List<TableDesc.Attribute> attributes = Arrays.asList(new TableDesc.Attribute("person_id", FieldType.INT),
                new TableDesc.Attribute("person_age", FieldType.INT));

        File file = new File(fileName);
        file.deleteOnExit();
        TableDesc tableDesc = new TableDesc(attributes);
        BPTreeTableFile bpTreeTableFile = new BPTreeTableFile(file, tableDesc, 0);
        dataBase.addTable(bpTreeTableFile, "t_person");
        this.dataBase = dataBase;

        personTableDesc = tableDesc;
    }

    /**
     * t_person表 Leaf页可存储453行记录，Internal页可以存储455个索引
     */
    @Test
    public void testInsert() throws IOException {
        Transaction transaction = new Transaction(Lock.LockType.XLock);
        transaction.start();
        Connection.passingTransaction(transaction);

        DbTable t_person = dataBase.getDbTableByName("t_person");

        long l1 = System.currentTimeMillis();
        int num = 453;
        // 第一页
        for (int i = 0; i < num; i++) {
            Row row = new Row(personTableDesc);
            row.setField(0, new IntField(i));
            row.setField(1, new IntField(18));
            t_person.insertRow(row);
        }
        System.out.println("插入" + num + "条记录,耗时(ms)" + (System.currentTimeMillis() - l1));

        // 第2页，触发页分裂
        // 测试 leaf页分裂逻辑
        for (int i = 0; i < 1; i++) {
            Row row = new Row(personTableDesc);
            row.setField(0, new IntField(i));
            row.setField(1, new IntField(18));
            t_person.insertRow(row);
        }

        //
        for (int i = 0; i < num; i++) {
            Row row = new Row(personTableDesc);
            row.setField(0, new IntField(i));
            row.setField(1, new IntField(18));
            t_person.insertRow(row);
        }

        transaction.commit();
    }

    /**
     * BPTreeScan
     * 带索引条件的扫描
     */
    @Test
    public void BPTreeScan() throws IOException {
        Transaction transaction = new Transaction(Lock.LockType.XLock);
        transaction.start();
        Connection.passingTransaction(transaction);

        DbTable person = dataBase.getDbTableByName("t_person");
        long l1 = System.currentTimeMillis();
        int num = 12;
        // 完成内部页分裂
        for (int i = 1; i < 14; i++) {
            Row row = new Row(personTableDesc);
            row.setField(0, new IntField(i));
            row.setField(1, new IntField(18));
            person.insertRow(row);
        }

        // 带索引的扫描
        IndexPredicate predicate =
                new IndexPredicate(PredicateEnum.GREATER_THAN_OR_EQ, new IntField(5));
        BPTreeScan scan = new BPTreeScan(person.getTableId(), predicate);
        scan.open();
        while (scan.hasNext()) {
            Row next = scan.next();
            System.out.println(next);
        }

        // 不带索引的扫描
        BPTreeScan scanNoIndex = new BPTreeScan(person.getTableId(),null);
        scanNoIndex.open();
        while (scanNoIndex.hasNext()) {
            Row next = scanNoIndex.next();
            System.out.println(next);
        }

        transaction.commit();
    }

    /**
     * BPTreeScan
     * 带索引条件的扫描
     */
    @Test
    public void BPTreeDelete() throws IOException {
        Transaction transaction = new Transaction(Lock.LockType.XLock);
        transaction.start();
        Connection.passingTransaction(transaction);

        DbTable person = dataBase.getDbTableByName("t_person");
        long l1 = System.currentTimeMillis();
        int num = 12;
        // 完成内部页分裂
        // FIXME 3000 以内数据能跑通测试，页重分布还是有问题
        for (int i = 1; i < 1000; i++) {
            Row row = new Row(personTableDesc);
            row.setField(0, new IntField(i));
            row.setField(1, new IntField(18));
            person.insertRow(row);
        }

        BPTreeScan scan = new BPTreeScan(person.getTableId(), null);
        scan.open();
        Row next = scan.next();

        // 全部删
        Delete delete = new Delete(scan);
        delete.loopDelete();

        scan.open();
        assertFalse(scan.hasNext());

        transaction.commit();

    }

}
