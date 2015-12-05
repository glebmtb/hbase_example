package ru.n5g.hbaseclient;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.*;

public class SimpleHBaseClientTest {

    private static final String TEST_ZOOKEEPER_SERVER = "srv-zookeeper-1";

    @Test
    @Ignore
    public void integrationTest() throws Throwable {
        String TEST_ZOOKEEPER_SERVER = "srv-zookeeper-1";
        String TABLE_NAME = "integration_test_hbase";
        String ROW1 = "row1";
        String COL1 = "col1";
        String COL2 = "col2";
        String VAL1 = "val1";
        String VAL2 = "val2";

        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", TEST_ZOOKEEPER_SERVER);
        Connection connection = ConnectionFactory.createConnection(conf);
        SimpleHBaseClient hBaseClient = new SimpleHBaseClient(connection);

        if (hBaseClient.isExistTable(TABLE_NAME)) {
            hBaseClient.deleteTable(TABLE_NAME);
            assertFalse(hBaseClient.isExistTable(TABLE_NAME));
        }
        //CREATE TABLE
        hBaseClient.createTable(TABLE_NAME);
        assertTrue(hBaseClient.isExistTable(TABLE_NAME));
        assertEquals(hBaseClient.size(TABLE_NAME), 0);
        //SET DATA
        hBaseClient.set(TABLE_NAME, ROW1, COL1, VAL1);
        hBaseClient.set(TABLE_NAME, ROW1, COL2, VAL2);
        //GET DATA
        String valueOut1 = hBaseClient.get(TABLE_NAME, ROW1, COL1);
        String valueOut2 = hBaseClient.get(TABLE_NAME, ROW1, COL2);
        assertEquals(valueOut1, VAL1);
        assertEquals(valueOut2, VAL2);
        //SIZE
        assertEquals(hBaseClient.size(TABLE_NAME), 1);
        //DELETE COLUMN
        hBaseClient.delete(TABLE_NAME, ROW1, COL1);
        valueOut1 = hBaseClient.get(TABLE_NAME, ROW1, COL1);
        valueOut2 = hBaseClient.get(TABLE_NAME, ROW1, COL2);
        assertNull(valueOut1);
        assertEquals(valueOut2, VAL2);
        //DELETE ROW
        hBaseClient.delete(TABLE_NAME, "row1");
        valueOut1 = hBaseClient.get(TABLE_NAME, ROW1, COL1);
        valueOut2 = hBaseClient.get(TABLE_NAME, ROW1, COL2);
        assertNull(valueOut1);
        assertNull(valueOut2);
        assertEquals(hBaseClient.size(TABLE_NAME), 0);
        //DELETE TABLE
        hBaseClient.deleteTable(TABLE_NAME);
        assertFalse(hBaseClient.isExistTable(TABLE_NAME));
    }

    @Test
    @Ignore
    public void testTablesName() throws Exception {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", TEST_ZOOKEEPER_SERVER);
        Connection connection = ConnectionFactory.createConnection(conf);
        SimpleHBaseClient hBaseClient = new SimpleHBaseClient(connection);
        hBaseClient.listTablesName().forEach(System.out::println);
    }
}