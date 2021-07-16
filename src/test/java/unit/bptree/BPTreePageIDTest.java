package unit.bptree;

import com.microdb.model.page.bptree.BPTreePageID;
import com.microdb.model.page.bptree.BPTreePageType;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

/**
 * B+Tree page id test
 *
 * @author zhangjw
 * @version 1.0
 */
public class BPTreePageIDTest {

    /**
     * 测试Equals 方法
     */
    @Test
    public void testEquals() {
        BPTreePageID pageID1 = new BPTreePageID(1, 1, BPTreePageType.ROOT_PTR);
        BPTreePageID pageID1Copy = new BPTreePageID(1, 1, BPTreePageType.ROOT_PTR);

        BPTreePageID pageID2 = new BPTreePageID(222, 1, BPTreePageType.ROOT_PTR);
        BPTreePageID pageID3 = new BPTreePageID(1, 222, BPTreePageType.ROOT_PTR);
        BPTreePageID pageID4 = new BPTreePageID(1, 1, BPTreePageType.LEAF);

        assertEquals(pageID1, pageID1Copy);

        assertNotEquals(pageID1, null);
        assertNotEquals(pageID1, pageID2);
        assertNotEquals(pageID1, pageID3);
        assertNotEquals(pageID1, pageID4);

    }

}
