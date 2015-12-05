package ru.n5g.hbaseclient;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.apache.hadoop.hbase.util.Bytes.toBytes;

/**
 * @author Gleb Belyaev
 *         Created by on 05.12.15.
 */
public class SimpleHBaseClient {
    private static final Logger LOG = LoggerFactory.getLogger(SimpleHBaseClient.class);
    private final Connection connection;

    private String columnFamily = "default";

    public SimpleHBaseClient(Connection connection) {
        this.connection = connection;
    }

    public String get(String tableName, String rowId, String columnName) throws IOException {
        LOG.debug("get(tableName {}, rowId {}, columnName {}", tableName, rowId, columnName);
        try (Table table = connection.getTable(TableName.valueOf(tableName))) {
            Get theGet = new Get(toBytes(rowId));
            Result result = table.get(theGet);
            //get value by ColumnFamily and ColumnName
            byte[] inValueByte = result.getValue(toBytes(columnFamily), toBytes(columnName));
            return Bytes.toString(inValueByte);
        }
    }

    public void set(String tableName, String rowId, String columnName, String value) throws IOException {
        LOG.debug("set(tableName {}, rowID {}, columnName {}, value {}", tableName, rowId, columnName, value);
        try (Table table = connection.getTable(TableName.valueOf(tableName))) {
            Put put = new Put(toBytes(rowId));
            put.addColumn(toBytes(columnFamily), toBytes(columnName), toBytes(value));
            table.put(put);
        }
    }

    public List<String> listTablesName() throws IOException {
        LOG.debug("listTableName()");
        try (Admin admin = connection.getAdmin()) {
            return Arrays.stream(admin.listTables()).map(HTableDescriptor::getNameAsString).collect(Collectors.toList());
        }
    }

    public void delete(String tableName, String rowId) throws IOException {
        LOG.debug("delete(tableName {}, rowId {}", tableName, rowId);
        try (Table table = connection.getTable(TableName.valueOf(tableName))) {
            Delete delete = new Delete(toBytes(rowId));
            delete.addFamily(toBytes(columnFamily));
            table.delete(delete);
        }
    }

    public void delete(String tableName, String rowId, String columnName) throws IOException {
        LOG.debug("delete(tableName {}, rowId {}, columnName {})", tableName, rowId, columnName);
        try (Table table = connection.getTable(TableName.valueOf(tableName))) {
            Delete delete = new Delete(toBytes(rowId));
            delete.addColumn(toBytes(columnFamily), toBytes(columnName));
            table.delete(delete);
        }
    }

    /**
     * Very slowly.
     * Do you have an idea how do is more fast?
     */
    public long size(String tableName) throws Throwable {
        LOG.debug("size(tableName {})", tableName);
        AtomicLong atomicLong = new AtomicLong();
        try (Table table = connection.getTable(TableName.valueOf(tableName))) {
            ResultScanner rs = table.getScanner(toBytes(columnFamily));
            rs.forEach(e -> atomicLong.incrementAndGet());
        }
        return atomicLong.get();
    }

    public boolean isExistTable(String tableName) throws IOException {
        LOG.debug("isExistTable(tableName {})", tableName);
        Admin admin = connection.getAdmin();
        return admin.tableExists(TableName.valueOf(tableName));
    }

    public void createTable(String tableName) throws IOException {
        LOG.debug("createTable(tableName {})", tableName);
        Admin admin = connection.getAdmin();
        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        tableDescriptor.addFamily(new HColumnDescriptor(columnFamily));
        admin.createTable(tableDescriptor);
    }

    public void deleteTable(String tableNameString) throws IOException {
        LOG.debug("deleteTable(tableName {}", tableNameString);
        Admin admin = connection.getAdmin();
        TableName tableName = TableName.valueOf(tableNameString);
        admin.disableTable(tableName);
        admin.deleteTable(tableName);
    }

    public void setColumnFamily(String columnFamily) {
        this.columnFamily = columnFamily;
    }
}
