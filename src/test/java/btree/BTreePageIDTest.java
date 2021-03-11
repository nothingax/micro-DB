package btree;

import com.microdb.model.page.btree.BTreePageID;
import com.microdb.model.page.btree.BTreePageType;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * B+Tree page id test
 *
 * @author zhangjw
 * @version 1.0
 */
public class BTreePageIDTest {

    /**
     * 测试Equals 方法
     */
    @Test
    public void testEquals() {
        BTreePageID pageID1 = new BTreePageID(1, 1, BTreePageType.ROOT_PTR);
        BTreePageID pageID1Copy = new BTreePageID(1, 1, BTreePageType.ROOT_PTR);

        BTreePageID pageID2 = new BTreePageID(222, 1, BTreePageType.ROOT_PTR);
        BTreePageID pageID3 = new BTreePageID(1, 222, BTreePageType.ROOT_PTR);
        BTreePageID pageID4 = new BTreePageID(1, 1, BTreePageType.LEAF);

        assertEquals(pageID1, pageID1Copy);

        assertNotEquals(pageID1, null);
        assertNotEquals(pageID1, pageID2);
        assertNotEquals(pageID1, pageID3);
        assertNotEquals(pageID1, pageID4);

    }

}
