package base;

import org.junit.After;
import org.junit.Before;

import java.io.File;

/**
 * 删除测试生成的redo undo文件
 *
 * @author zhangjianwei
 * @version 1.0
 */
public class TestBase {
    @Before
    @After
    public void after() {
        (new File("redo")).delete();
        (new File("undo")).delete();
    }
}
