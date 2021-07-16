package unit.bptree;

import com.microdb.model.DataBase;
import com.microdb.model.field.FieldType;
import com.microdb.model.field.IntField;
import com.microdb.model.page.bptree.BPTreeEntry;
import com.microdb.model.page.bptree.BPTreeInternalPage;
import com.microdb.model.page.bptree.BPTreePageID;
import com.microdb.model.page.bptree.BPTreePageType;
import com.microdb.model.table.TableDesc;
import com.microdb.model.table.tablefile.BPTreeTableFile;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.*;

import static org.junit.Assert.fail;

/**
 * BPTreeInternalPageTest
 *
 * @author zhangjw
 * @version 1.0
 */
public class BPTreeInternalPageTest {
    public DataBase dataBase;

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
    }

    public static final int[][] EXAMPLE_VALUES = new int[][]{
            {2, 6350, 4},
            {4, 9086, 5},
            {5, 17197, 7},
            {7, 22064, 9},
            {9, 22189, 10},
            {10, 28617, 11},
            {11, 31933, 13},
            {13, 33549, 14},
            {14, 34784, 15},
            {15, 42878, 17},
            {17, 45569, 19},
            {19, 56462, 20},
            {20, 62778, 21},
            {15, 42812, 16},
            {2, 3596, 3},
            {6, 17876, 7},
            {1, 1468, 2},
            {11, 29402, 12},
            {18, 51440, 19},
            {7, 19209, 8}
    };

    /**
     * 添加entry
     */
    @Test
    public void insertEntry() throws Exception {
        byte[] data = BPTreeInternalPage.createEmptyPageData();
        BPTreePageID pageID =
                new BPTreePageID(DataBase.getInstance().getDbTableByName("t_person").getTableId(), 1, BPTreePageType.INTERNAL);
        BPTreeInternalPage page = new BPTreeInternalPage(pageID, data, 0);
        ArrayList<BPTreeEntry> entries = new ArrayList<>();

        int[][] EXAMPLE_VALUES = new int[][]{
                {2, 100, 4},
                {4, 200, 5},
                {5, 300, 7}
        };


        // 超量插入：报错
        EXAMPLE_VALUES = new int[][]{
                {2, 6350, 4},
                {4, 9086, 5},
                {5, 17197, 7},
                {7, 22064, 9},
                {9, 22189, 10},
                {10, 28617, 11},
        };
        for (int[] entry : EXAMPLE_VALUES) {
            BPTreePageID leftChild = new BPTreePageID(pageID.getTableId(), entry[0], BPTreePageType.LEAF);
            BPTreePageID rightChild = new BPTreePageID(pageID.getTableId(), entry[2], BPTreePageType.LEAF);
            BPTreeEntry e = new BPTreeEntry(new IntField(entry[1]), leftChild, rightChild);
            entries.add(e);
            page.insertEntry(e);
        }

        System.out.println(page);

    }

    @Test
    public void testIterator() throws IOException {
        byte[] data = BPTreeInternalPage.createEmptyPageData();
        BPTreePageID pageID =
                new BPTreePageID(DataBase.getInstance().getDbTableByName("t_person").getTableId(), 1, BPTreePageType.INTERNAL);
        BPTreeInternalPage page = new BPTreeInternalPage(pageID, data, 0);
        ArrayList<BPTreeEntry> entries = new ArrayList<>();

        int[][] EXAMPLE_VALUES = new int[][]{
                {2, 100, 4},
                {4, 200, 5},
                {5, 300, 7}
        };

        // 从小到大顺序插入
        for (int[] entry : EXAMPLE_VALUES) {
            BPTreePageID leftChild = new BPTreePageID(pageID.getTableId(), entry[0], BPTreePageType.LEAF);
            BPTreePageID rightChild = new BPTreePageID(pageID.getTableId(), entry[2], BPTreePageType.LEAF);
            BPTreeEntry e = new BPTreeEntry(new IntField(entry[1]), leftChild, rightChild);
            entries.add(e);
            page.insertEntry(e);
        }


        System.out.println("正向迭代器---");
        Iterator<BPTreeEntry> iterator = page.getIterator();
        while (iterator.hasNext()) {
            BPTreeEntry next = iterator.next();
            System.out.println(next);
        }

        System.out.println("===========");
        iterator = page.getIterator();
        System.out.println(iterator.next());
        System.out.println(iterator.next());
        System.out.println(iterator.next());

        try {
            iterator.next();
            fail("expected NoSuchElementException");
        } catch (NoSuchElementException e) {
        }

        System.out.println("反向迭代器---");
        Iterator<BPTreeEntry> reverseIterator = page.getReverseIterator();
        while (reverseIterator.hasNext()) {
            BPTreeEntry next = reverseIterator.next();
            System.out.println(next);
        }

        System.out.println("====================");

        reverseIterator = page.getReverseIterator();
        System.out.println(reverseIterator.next());
        System.out.println(reverseIterator.next());
        System.out.println(reverseIterator.next());

        try {
            iterator.next();
            fail("expected NoSuchElementException");
        } catch (NoSuchElementException e) {
        }
    }

    /**
     * delete Entry
     */
    @Test
    public void deleteEntry() throws Exception {

        byte[] data = BPTreeInternalPage.createEmptyPageData();
        BPTreePageID pageID =
                new BPTreePageID(DataBase.getInstance().getDbTableByName("t_person").getTableId(), 1, BPTreePageType.INTERNAL);
        BPTreeInternalPage page = new BPTreeInternalPage(pageID, data, 0);
        ArrayList<BPTreeEntry> entries = new ArrayList<>();

        int[][] EXAMPLE_VALUES = new int[][]{
                {2, 100, 4},
                {4, 200, 5},
                {5, 300, 7}
        };

        // 从小到大顺序插入
        for (int[] entry : EXAMPLE_VALUES) {
            BPTreePageID leftChild = new BPTreePageID(pageID.getTableId(), entry[0], BPTreePageType.LEAF);
            BPTreePageID rightChild = new BPTreePageID(pageID.getTableId(), entry[2], BPTreePageType.LEAF);
            BPTreeEntry e = new BPTreeEntry(new IntField(entry[1]), leftChild, rightChild);
            entries.add(e);
            page.insertEntry(e);
        }

        Iterator<BPTreeEntry> iterator = page.getIterator();
        iterator = page.getIterator();
        BPTreeEntry next1 = iterator.next();
        BPTreeEntry next2 = iterator.next();
        BPTreeEntry next3 = iterator.next();

        // page.deleteEntryFromTheLeft(next1);
        // page.deleteEntryFromTheLeft(next2);
        // page.deleteEntryFromTheLeft(next3);
        // printEntry(page);

        // page.deleteEntryFromTheRight(next1);
        page.deleteEntryFromTheRight(next2);
        // page.deleteEntryFromTheRight(next3);
        printInternalPage(page);
    }

    public void printInternalPage(BPTreeInternalPage page) {
        Iterator<BPTreeEntry> iterator = page.getIterator();
        while (iterator.hasNext()) {
            BPTreeEntry next = iterator.next();
            System.out.println(next);
        }
    }

}
