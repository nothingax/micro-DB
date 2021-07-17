package integrated.bptree;

import base.TestBase;
import com.microdb.connection.Connection;
import com.microdb.model.DataBase;
import com.microdb.model.field.FieldType;
import com.microdb.model.field.IntField;
import com.microdb.model.page.bptree.*;
import com.microdb.model.row.Row;
import com.microdb.model.table.TableDesc;
import com.microdb.model.table.tablefile.BPTreeTableFile;
import com.microdb.operator.Delete;
import com.microdb.operator.bptree.BPTreeScan;
import com.microdb.transaction.Lock;
import com.microdb.transaction.Transaction;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.assertFalse;

/**
 * BPTreeInternalPageTest
 *
 * @author zhangjw
 * @version 1.0
 */
public class BPTreeInternalPageTest extends TestBase {
    public DataBase dataBase;
    private TableDesc personTableDesc;
    private int keyIndex;

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
        this.keyIndex = 0;
        BPTreeTableFile bpTreeTableFile = new BPTreeTableFile(file, tableDesc, keyIndex);
        dataBase.addTable(bpTreeTableFile, "t_person");
        this.dataBase = dataBase;

        personTableDesc = tableDesc;
    }

    /**
     * t_person表 Leaf页可存储453行记录，Internal页可以存储455个索引
     * <p>
     * 需要>455个leaf页来触发Internal页分裂
     *
     * TODO pageSize多大时，执行时间过长， 后续更更改为参数可配置，
     */
    @Test
    public void testInternalPageSplit() throws IOException {
    }

    /**
     *
     */
    @Test
    public void insertEntryAndSplit() throws IOException {

    }

    /**
     * 删除测试
     * 表中只有一页internalPage的情况
     */
    @Test
    public void deleteRowAndEntryOneInternalPage() throws IOException {
        Transaction transaction = new Transaction(Lock.LockType.XLock);
        transaction.start();
        Connection.passingTransaction(transaction);

        BPTreeTableFile tableFile = (BPTreeTableFile) dataBase.getDbTableByName("t_person").getTableFile();
        int tableId = tableFile.getTableId();
        for (int i = 1; i <= 10; i++) {
            Row row = new Row(personTableDesc);
            row.setField(0, new IntField(i));
            row.setField(1, new IntField(18));
            tableFile.insertRow(row);
        }

        BPTreeScan scan = new BPTreeScan(tableFile.getTableId(), null);

        System.out.println("开始打印====");
        // 删除并打印
        for (int i = 1; i <= 10; i++) {
            deleteOne(tableFile, scan);
            printTree(tableFile, tableId);
        }


        scan.open();
        assertFalse(scan.hasNext());

        transaction.commit();
    }

    /**
     * 删除测试
     * 表中超过1页internalPage，但internal page 均不满的情况
     */
    @Test
    public void deleteRowAndEntryForSeveralInternalPage() throws IOException {
        Transaction transaction = new Transaction(Lock.LockType.XLock);
        transaction.start();
        Connection.passingTransaction(transaction);

        BPTreeTableFile tableFile = (BPTreeTableFile) dataBase.getDbTableByName("t_person").getTableFile();
        int tableId = tableFile.getTableId();
        int num = 200;
        for (int i = 1; i <= num; i++) {
            Row row = new Row(personTableDesc);
            row.setField(0, new IntField(i));
            row.setField(1, new IntField(18));
            DataBase.getBufferPool().insertRow(row, "t_person");
        }

        System.out.println("开始打印表数据====");
        printTree(tableFile, tableId);

        BPTreeScan scan = new BPTreeScan(tableFile.getTableId(), null);

        System.out.println("开始打印====");
        // 删除并打印
        for (int i = 1; i <= num; i++) {
            deleteOne(tableFile, scan);
            printTree(tableFile, tableId);
        }

        scan.open();
        assertFalse(scan.hasNext());

        transaction.commit();
    }

    /**
     * 删除测试
     * 大量数据
     */
    @Test
    public void deleteRowAndEntryMassData() throws IOException {
        Transaction transaction = new Transaction(Lock.LockType.XLock);
        transaction.start();
        Connection.passingTransaction(transaction);

        BPTreeTableFile tableFile = (BPTreeTableFile) dataBase.getDbTableByName("t_person").getTableFile();
        int tableId = tableFile.getTableId();
        int num = 30000;
        long l1 = System.currentTimeMillis();
        for (int i = 1; i <= num; i++) {
            Row row = new Row(personTableDesc);
            row.setField(0, new IntField(i));
            row.setField(1, new IntField(18));
            DataBase.getBufferPool().insertRow(row, "t_person");
        }

        System.out.println("开始打印表数据====");
        printTree(tableFile, tableId);

        BPTreeScan scan = new BPTreeScan(tableFile.getTableId(), null);

        scan.open();
        while (scan.hasNext()) {
            System.out.println(scan.next());

        }

        // 通过Delete操作符删除，执行非常快速
        Delete delete = new Delete(scan);
        delete.loopDelete();

        // 外部迭代删除，删除非常慢
        // System.out.println("开始打印====");
        // // 删除并打印
        // for (int i = 1; i <= num; i++) {
        //     deleteOne(tableFile, scan);
        //     printTree(tableFile, tableId);
        // }

        scan.open();
        assertFalse(scan.hasNext());
        System.out.println("耗时ms:" + (System.currentTimeMillis() - l1));

        transaction.commit();
    }

    private void deleteOne(BPTreeTableFile tableFile, BPTreeScan scan) throws IOException {
        scan.open();
        Row next = scan.next();
        // tableFile.deleteRow(scan.next());
        DataBase.getBufferPool().deleteRow(next);
        scan.close();
    }

    private void printTree(BPTreeTableFile tableFile, int tableId) {
        BPTreePageID rootPtrPageID = BPTreeRootPtrPage.getRootPtrPageID(tableId);
        BPTreeRootPtrPage rootPtrPage = (BPTreeRootPtrPage) DataBase.getBufferPool().getPage(rootPtrPageID);
        BPTreePage rootNodePage = (BPTreePage) DataBase.getBufferPool().getPage(rootPtrPage.getRootNodePageID());
        System.out.println("打印rootNodePage======");
        if (rootNodePage instanceof BPTreeInternalPage) {
            printBPTreePage(rootNodePage);
            Iterator<BPTreeEntry> iterator = ((BPTreeInternalPage) rootNodePage).getIterator();
            System.out.println("打印内部元素=====");
            while (iterator.hasNext()) {
                BPTreeEntry next = iterator.next();

                System.out.println("left=====:");
                BPTreePage bpTreePage = (BPTreePage) DataBase.getBufferPool().getPage(next.getLeftChildPageID());
                printBPTreePage(bpTreePage);

                System.out.println("right=====:");
                BPTreePage bpTreePage1 = (BPTreePage) DataBase.getBufferPool().getPage(next.getRightChildPageID());
                printBPTreePage(bpTreePage1);
            }
        } else if (rootNodePage instanceof BPTreeLeafPage) {
            printBPTreePage(rootNodePage);
        }

    }

    private void printBPTreePage(BPTreePage bpTreePage) {
        if (bpTreePage instanceof BPTreeInternalPage) {
            printInternalPage((BPTreeInternalPage) bpTreePage);
        } else if (bpTreePage instanceof BPTreeLeafPage) {
            printLeafPage((BPTreeLeafPage) bpTreePage);
        }
    }

    private void printLeafByInternalPage(BPTreeTableFile tableFile, BPTreeInternalPage bpTreePage) {
        Iterator<BPTreeEntry> iterator1 = bpTreePage.getIterator();
        while (iterator1.hasNext()) {
            BPTreeEntry next1 = iterator1.next();
            printLeafPage((BPTreeLeafPage) DataBase.getBufferPool().getPage(next1.getLeftChildPageID()));
            printLeafPage((BPTreeLeafPage) DataBase.getBufferPool().getPage(next1.getRightChildPageID()));
        }
    }

    private void printLeafPage(BPTreeLeafPage bpTreePage) {
        Iterator<Row> rowIterator = bpTreePage.getRowIterator();
        while (rowIterator.hasNext()) {
            Row next1 = rowIterator.next();
            System.out.println(next1);
        }
    }

    public void printInternalPage(BPTreeInternalPage page) {
        Iterator<BPTreeEntry> iterator = page.getIterator();
        while (iterator.hasNext()) {
            BPTreeEntry next = iterator.next();
            System.out.println(next);
        }
    }

    public int getMaxEntryNum(TableDesc tableDesc, int keyFieldIndex) {
        int slotStatusSizeInByte = 1;
        int pageNoSizeInByte = FieldType.INT.getSizeInByte();
        int keySizeInByte = tableDesc.getFieldTypes().get(keyFieldIndex).getSizeInByte();

        // 索引值+子节点指针（每个entry两个子页面）+slot状态位
        int perEntrySizeInByte = keySizeInByte + pageNoSizeInByte * 2 + slotStatusSizeInByte;

        // // 每页一个父指针，m个entry有m+1个子节点指针，一个字节表示子page类型、一个额外的header使用1字节。
        // int extra = 2 * FieldType.INT.getSizeInByte() + 1 + 1;

        // 每页一个父pageNo指针 一个字节表示子page类型
        int extra = FieldType.INT.getSizeInByte() + 1;

        return (DataBase.getDBConfig().getPageSizeInByte() - extra) / perEntrySizeInByte;
    }

    /**
     * 返回该页容纳的行数
     * 每行数据使用1 byte存储行的使用状态
     * page需要额外3个指针维护左右兄弟page，父page
     */
    public int calculateMaxSlotNum(TableDesc tableDesc) {
        // slot状态位占用空间=1byte
        int slotStatusSizeInByte = 1;
        int sizePerRowInBytes = tableDesc.getRowMaxSizeInBytes() + slotStatusSizeInByte;

        int POINTER_SIZE_IN_BYTE = 4;
        int pointerSizeInBytes = 3 * POINTER_SIZE_IN_BYTE;
        return (DataBase.getDBConfig().getPageSizeInByte() - pointerSizeInBytes) / sizePerRowInBytes;
    }

}
