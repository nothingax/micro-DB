// package com.microdb.model.page.btree;
//
// import com.microdb.model.Row;
// import com.microdb.model.TableDesc;
// import com.microdb.model.page.Page;
// import com.microdb.model.page.PageID;
//
// import java.io.IOException;
// import java.util.Iterator;
//
// /**
//  * TODO
//  *
//  * @author zhangjw
//  * @version 1.0
//  */
// public class BTreePage implements Page {
//     @Override
//     public PageID getPageID() {
//         return null;
//     }
//
//     @Override
//     public byte[] serialize() throws IOException {
//         return new byte[0];
//     }
//
//     @Override
//     public void deserialize(byte[] pageData) throws IOException {
//
//     }
//
//     @Override
//     public boolean hasEmptySlot() {
//         return false;
//     }
//
//     @Override
//     public boolean isSlotUsed(int index) {
//         return false;
//     }
//
//     @Override
//     public int calculateMaxSlotNum(TableDesc tableDesc) {
//         return 0;
//     }
//
//     @Override
//     public void insertRow(Row row) {
//
//
//     }
//
//     @Override
//     public Iterator<Row> getRowIterator() {
//         return null;
//     }
// }
