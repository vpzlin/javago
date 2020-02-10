package org.vpzlin.javago.utils.hbase;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;
import org.vpzlin.javago.utils.Result;
import org.vpzlin.javago.utils.ByteUtil;

import java.io.IOException;
import java.util.*;

public class HBaseUtil {
    /**
     * default parameters
     */
    // all threads use only one connection best, HBase manages connection automatically and thread safety
    private Connection connection;
    // max version number
    private int maxVersionNumber = HConstants.ALL_VERSIONS;
    // all threads use their Admin alone best
    private Admin admin;

    /**
     * transform array to String without bracket
     * @param array array of String
     * @return String
     */
    private static String transformArrayToStringWithoutBracket(String[] array){
        if(array == null){
            return null;
        }

        return Arrays.toString(array).replace("[", "").replace("]", "").replace(" ", "");
    }

    /**
     * get current HBase connection
     * @return
     */
    public Connection getConnection() {
        return connection;
    }

    /**
     * init class with connection to HBase
     * @param connection
     */
    public HBaseUtil(Connection connection){
        this.connection = connection;
    }

    /**
     * init class with connection to HBase
     * @param zookeeperServers zookeeper server ip or servers' names, separated by commas
     * @param zookeeperPort zookeeper server port
     * @param connectionPoolSize connection pool size, default value is [1]
     * @throws Exception error of initializing class
     */
    public HBaseUtil(String zookeeperServers, String zookeeperPort, int connectionPoolSize) throws Exception{
        Result connectionResult = ConnectionUtil.getHBaseConnection(zookeeperServers, zookeeperPort, connectionPoolSize);
        if(!connectionResult.isSuccess()){
            throw new Exception(String.format("Failed to init class [HBaseUtil]. %s", connectionResult.getMessage()));
        }
        else {
            this.connection = (Connection)connectionResult.getData();
            this.admin = this.connection.getAdmin();
        }
    }

    /**
     * init class with connection to HBase
     * @param zookeeperServers zookeeper server ip or servers' names, separated by commas
     * @param zookeeperPort zookeeper server port
     * @throws Exception error of initializing class
     */
    public HBaseUtil(String zookeeperServers, String zookeeperPort) throws Exception{
        Result connectionResult = ConnectionUtil.getHBaseConnection(zookeeperServers, zookeeperPort, 1);
        if(!connectionResult.isSuccess()){
            throw new Exception(String.format("Failed to init class [HBaseUtil]. %s", connectionResult.getMessage()));
        }
        else {
            this.connection = (Connection)connectionResult.getData();
            this.admin = this.connection.getAdmin();
        }
    }


    /**
     * check if HBase table exists
     * @param tableName table name
     * @return the type of Result.data is [boolean]
     */
    public Result existTable(String tableName){
        if(tableName == null || tableName.trim().length() == 0){
            return Result.getResult(false, null, "Failed to check if HBase table exists, table name can't be null or empty.");
        }
        tableName = tableName.trim();

        try {
            if(admin.tableExists(TableName.valueOf(tableName)) == true){
                return Result.getResult(true, true, String.format("HBase table [%s] exists.", tableName));
            }
            else {
                return Result.getResult(true, false, String.format("HBase table [%s] doesn't exist.", tableName));
            }
        } catch (IOException e) {
            return Result.getResult(false, null, String.format("Failed to check HBase table [%s] exists, more info = [%s].", tableName, e.getMessage()));
        }
    }

    /**
     * disable HBase table
     * @param tableName table name
     * @return
     */
    public Result disableTable(String tableName){
        if(tableName == null || tableName.trim().length() == 0){
            return Result.getResult(false, null, "Failed to disable HBase table, the table name can't be null or empty.");
        }
        tableName = tableName.trim();
        try {
            if(admin.isTableDisabled(TableName.valueOf(tableName))){
                return Result.getResult(true, null, String.format("It doesn't need to disable HBase table [%s] again, it has already been disabled.", tableName));
            }
            admin.disableTable(TableName.valueOf(tableName));
            return Result.getResult(true, null, String.format("Disabled HBase table [%s].", tableName));
        }
        catch (Exception e){
            return Result.getResult(false, null, String.format("Failed to disable HBase table [%s], more info = [%s].", tableName, e.getMessage()));
        }
    }

    /**
     * enable HBase table
     * @param tableName table name
     * @return
     */
    public Result enableTable(String tableName){
        if(tableName == null || tableName.trim().length() == 0){
            return Result.getResult(false, null, "Failed to enable HBase table, the table name can't be null or empty.");
        }
        tableName = tableName.trim();
        try {
            if(!admin.isTableDisabled(TableName.valueOf(tableName))){
                return Result.getResult(true, null, String.format("It doesn't need to enable HBase table [%s] again, it has already been enabled.", tableName));
            }
            admin.enableTable(TableName.valueOf(tableName));
            return Result.getResult(true, null, String.format("Enabled HBase table [%s].", tableName));
        }
        catch (Exception e){
            return Result.getResult(false, null, String.format("Failed to enable HBase table [%s], more info = [%s].", tableName, e.getMessage()));
        }
    }

    /**
     * drop HBase table
     * @param tableName table name
     * @param forceDisableTable force disable table if it is not disabled firstly
     * @return
     */
    public Result dropTable(String tableName, boolean forceDisableTable){
        if(tableName == null || tableName.trim().length() == 0){
            return Result.getResult(false, null, "Failed to drop HBase table, the table name can't be null or empty.");
        }
        tableName = tableName.trim();
        try {
            if (!admin.tableExists(TableName.valueOf(tableName))) {
                return Result.getResult(false, null, String.format("Failed to drop HBase table [%s], it doesn't exist.", tableName));
            }
            if(!admin.isTableDisabled(TableName.valueOf(tableName))){
                if(forceDisableTable){
                    admin.disableTable(TableName.valueOf(tableName));
                }
                else {
                    return Result.getResult(false, null, String.format("Failed to drop HBase table [%s], it must be disabled before dropping it.", tableName));
                }
            }
            admin.deleteTable(TableName.valueOf(tableName));
            return Result.getResult(true, null, String.format("Dropped HBase table [%s], it doesn't exist.", tableName));
        }
        catch (Exception e){
            return Result.getResult(false, null, String.format("Failed to drop HBase table [%s], more info = [%s].", tableName, e.getMessage()));
        }
    }

    /**
     * drop HBase table
     * @param tableName table name
     * @return
     */
    public Result dropTable(String tableName){
        return dropTable(tableName, false);
    }

    /**
     * get compression type
     * @param compressionType compression type
     * @return enum of HBase table's compress type
     */
    private Compression.Algorithm getHBaseCompression(HBaseCompressionType compressionType){
        if(compressionType == null){
            return null;
        }
        switch (compressionType){
            case GZ:
                return Compression.Algorithm.GZ;
            case LZ4:
                return Compression.Algorithm.LZ4;
            case LZO:
                return Compression.Algorithm.LZO;
            case NONE:
                return Compression.Algorithm.NONE;
            case SNAPPY:
                return Compression.Algorithm.SNAPPY;
            default:
                return null;

        }
    }

    /**
     * create new HBase table
     * @param tableName table name
     * @param familyNames family names
     * @param compressionType compression type, default value is [HBaseCompressionType.NONE]
     * @param maxVersions max version of HBase table, default value is [HConstants.ALL_VERSIONS] which is equal to [2147483647]
     * @return
     */
    public Result createTable(String tableName, String[] familyNames, HBaseCompressionType compressionType, int maxVersions){
        if(tableName == null || tableName.trim().length() == 0){
            return Result.getResult(false, null, String.format("Failed to create new HBase table, table name can't be null or empty."));
        }

        if(familyNames == null || familyNames.length == 0){
            return Result.getResult(false, null, String.format("Failed to create new HBase table [%s], family names can't be null or empty.", tableName));
        }
        if(maxVersions < 1){
            return Result.getResult(false, null, String.format("Failed to create new HBase table [%s], max version of HBase table can't be less than [1].", tableName));
        }
        try {
            if (admin.tableExists(TableName.valueOf(tableName)) == true) {
                return Result.getResult(false, null, String.format("Failed to create new HBase table [%s], it already exists.", tableName));
            }

            // add family names
            Compression.Algorithm algorithm = getHBaseCompression(compressionType);
            TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName));
            for (String columnFamilyName : familyNames) {
                ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder(
                        Bytes.toBytes(columnFamilyName));
                // assign compression type
                if(compressionType != null){
                    columnFamilyDescriptorBuilder.setCompactionCompressionType(algorithm);
                }
                columnFamilyDescriptorBuilder.setMaxVersions(maxVersions);
                tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptorBuilder.build());
            }
            admin.createTable(tableDescriptorBuilder.build());
            return Result.getResult(true, null, String.format("Created new HBase table [%s] with family names [%s].", tableName, transformArrayToStringWithoutBracket(familyNames)));
        }
        catch (Exception e){
            return Result.getResult(false, null, String.format("Failed to create new HBase table [%s] with family names [%s], more info = [%s].", tableName, transformArrayToStringWithoutBracket(familyNames), e.getMessage()));
        }
    }

    /**
     * create new HBase table
     * @param tableName table name
     * @param familyNames family names
     * @param compressionType compression type, default value is [HBaseCompressionType.NONE]
     * @return
     */
    public Result createTable(String tableName, String[] familyNames, HBaseCompressionType compressionType){
        return createTable(tableName, familyNames, compressionType, maxVersionNumber);
    }

    /**
     * create new HBase table
     * @param tableName table name
     * @param familyName family name
     * @param compressionType compression type, default value is [HBaseCompressionType.NONE]
     * @return
     */
    public Result createTable(String tableName, String familyName, HBaseCompressionType compressionType){
        String[] familyNames = {familyName};
        return createTable(tableName, familyNames, compressionType, maxVersionNumber);
    }

    /**
     * create new HBase table
     * @param tableName table name
     * @param familyNames family names
     * @return
     */
    public Result createTable(String tableName, String[] familyNames){
        return createTable(tableName, familyNames, HBaseCompressionType.NONE, maxVersionNumber);
    }

    /**
     * create new HBase table
     * @param tableName table name
     * @param familyName family name
     * @return
     */
    public Result createTable(String tableName, String familyName){
        String[] familyNames = {familyName};
        return createTable(tableName, familyNames, HBaseCompressionType.NONE, maxVersionNumber);
    }

    /**
     * delete HBase table
     * @param tableName table name
     * @return
     */
    public Result deleteTable(String tableName){
        if(tableName == null || tableName.trim().length() == 0){
            return Result.getResult(false, null, "Failed to delete HBase table, table name can't be null or empty.");
        }

        try {
            if(admin.tableExists(TableName.valueOf(tableName)) == false){
                return Result.getResult(false, null, String.format("Failed to delete HBase table [%s], it doesn't exist.", tableName));
            }

            if(admin.isTableDisabled(TableName.valueOf(tableName)) == false){
                admin.disableTable(TableName.valueOf(tableName));
            }
            admin.deleteTable(TableName.valueOf(tableName));
            return Result.getResult(true, null, String.format("Deleted HBase table [%s].", tableName));
        }
        catch (Exception e){
            return Result.getResult(false, null, String.format("Failed to delete HBase table [%s], more info = [%s].", tableName, e.getMessage()));
        }
    }

    /**
     * add new column family names to HBase table
     * @param tableName table name
     * @param columnFamilyNames new column family names
     * @param maxVersions max version of HBase table, default value is [HConstants.ALL_VERSIONS] which is equal to [2147483647]
     * @param compressionType compression type, default value is [HBaseCompressionType.NONE], this parameter can be null
     * @return
     */
    public Result addColumnFamilies(String tableName, String[] columnFamilyNames, int maxVersions, HBaseCompressionType compressionType){
        if(tableName == null || tableName.trim().length() == 0){
            return Result.getResult(false, null, String.format("Failed to add new column family names to HBase table, table name can't be null or empty."));
        }

        if(columnFamilyNames == null || columnFamilyNames.length == 0){
            return Result.getResult(false, null, String.format("Failed to add new column family names to HBase table [%s], table name can't be null or empty.", tableName));
        }

        if(maxVersions < 1){
            return Result.getResult(false, null, String.format("Failed to add new column family names [%s] to HBase table [%s], max version number can't be less than [1].", transformArrayToStringWithoutBracket(columnFamilyNames), tableName));
        }

        /**
         * add column family names
         */
        try {
            for (String columnFamilyName : columnFamilyNames) {
                ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(columnFamilyName));
                if(compressionType != null){
                    Compression.Algorithm algorithm = getHBaseCompression(compressionType);
                    columnFamilyDescriptorBuilder.setCompactionCompressionType(algorithm);
                }
                columnFamilyDescriptorBuilder.setMaxVersions(maxVersions);

                admin.addColumnFamily(TableName.valueOf(tableName), columnFamilyDescriptorBuilder.build());
            }
            return Result.getResult(true, null, String.format("Added new column family names [%s] to HBase table [%s].", transformArrayToStringWithoutBracket(columnFamilyNames), tableName));
        }
        catch (Exception e){
            return Result.getResult(false, null, String.format("Failed to add new column family names [%s] to HBase table [%s], more info = [%s].", transformArrayToStringWithoutBracket(columnFamilyNames), tableName, e.getMessage()));
        }
    }

    /**
     * add new column family names to HBase table
     * @param tableName table name
     * @param columnFamilyNames new column family names
     * @return
     */
    public Result addColumnFamilies(String tableName, String[] columnFamilyNames){
        return addColumnFamilies(tableName, columnFamilyNames, maxVersionNumber, null);
    }

    /**
     * add new column family name to HBase table
     * @param tableName table name
     * @param columnFamilyName new column family name
     * @param maxVersions max version of HBase table, default value is [HConstants.ALL_VERSIONS] which is equal to [2147483647]
     * @param compressionType compression type, default value is [HBaseCompressionType.NONE], this parameter can be null
     * @return
     */
    public Result addColumnFamily(String tableName, String columnFamilyName, int maxVersions, HBaseCompressionType compressionType){
        String[] columnFamilyNames = {columnFamilyName};
        return addColumnFamilies(tableName, columnFamilyNames, maxVersions, compressionType);
    }

    /**
     * add new column family name to HBase table
     * @param tableName table name
     * @param columnFamilyName new column family name
     * @return
     */
    public Result addColumnFamily(String tableName, String columnFamilyName){
        String[] columnFamilyNames = {columnFamilyName};
        return addColumnFamilies(tableName, columnFamilyNames, maxVersionNumber, null);
    }

    /**
     * get column family names of HBase table
     * @param tableName table name
     * @return the type of Result.data is [List<String>]
     */
    public Result getColumnFamilyNames(String tableName){
        if(tableName == null || tableName.trim().length() == 0){
            return Result.getResult(false, null, "Failed to get column family names of HBase table, table name can't be null or empty.");
        }

        try {
            if(admin.tableExists(TableName.valueOf(tableName)) == false){
                return Result.getResult(false, null, String.format("Failed to get column family names of HBase table [%s], the table doesn't exist.", tableName));
            }
            if(admin.isTableAvailable(TableName.valueOf(tableName)) == false){
                return Result.getResult(false, null, String.format("Failed to get column family names of HBase table [%s], the table isn't available.", tableName));
            }
            Table table = connection.getTable(TableName.valueOf(tableName));
            TableDescriptor tableDescriptor = table.getDescriptor();
            Set<byte[]> columnFamilyNames = tableDescriptor.getColumnFamilyNames();
            List<String> columnFamilyNamesList = new LinkedList<>();
            for(byte[] columnFamilyName: columnFamilyNames){
                columnFamilyNamesList.add(Bytes.toString(columnFamilyName));
            }

            return Result.getResult(true, columnFamilyNamesList, String.format("Got column family names [%s] of HBase table [%s].", transformArrayToStringWithoutBracket((String[])columnFamilyNamesList.toArray()), tableName));
        }
        catch (Exception e){
            return Result.getResult(false, null, String.format("Failed to get column family names of HBase table [%s], more info = [%s].", tableName, e.getMessage()));
        }
    }

    /**
     * delete column family names of HBase table
     * @param tableName table name
     * @param columnFamilyNames column family names
     * @return
     */
    public Result deleteColumnFamilies(String tableName, String[] columnFamilyNames){
        if(tableName == null || tableName.trim().length() == 0){
            return Result.getResult(false, null, "Failed to delete column family names of HBase table, table name can't be null or empty.");
        }

        if(columnFamilyNames == null || columnFamilyNames.length == 0){
            return Result.getResult(false, null, String.format("Failed to delete column family names of HBase table [%s], column family names can't be null or empty.", tableName));
        }

        try {
            for (String columnFamilyName : columnFamilyNames) {
                admin.deleteColumnFamily(TableName.valueOf(tableName), columnFamilyName.getBytes("UTF-8"));
            }
            return Result.getResult(true, null, String.format("Deleted column family names [%s] of HBase table [%s].", transformArrayToStringWithoutBracket(columnFamilyNames), tableName));
        }
        catch (Exception e){
            return Result.getResult(false, null, String.format("Failed to delete column family names [%s] of HBase table [%s], more info = [%s].", transformArrayToStringWithoutBracket(columnFamilyNames), tableName, e.getMessage()));
        }
    }

    /**
     * delete column family name of HBase table
     * @param tableName table name
     * @param columnFamilyName column family name
     * @return
     */
    public Result deleteColumnFamily(String tableName, String columnFamilyName){
        String[] columnFamilyNames = {columnFamilyName};
        return deleteColumnFamilies(tableName, columnFamilyNames);
    }

    /**
     * update column family name of HBase table
     * @param tableName table name
     * @param columnFamilyName new column family name
     * @param maxVersions max version of column family, default value is [HConstants.ALL_VERSIONS] which is equal to [2147483647]
     * @param compressionType compression type, default value is [HBaseCompressionType.NONE], this parameter can be null
     * @return
     */
    public Result updateColumnFamily(String tableName, String columnFamilyName, int maxVersions, HBaseCompressionType compressionType){
        if(tableName == null || tableName.trim().length() == 0){
            return Result.getResult(false, null, "Failed to update column family name of HBase table, table name can't be null or empty.");
        }

        if(columnFamilyName == null || columnFamilyName.trim().length() == 0){
            return Result.getResult(false, null, "Failed to update column family name of HBase table, column family name can't be null or empty.");
        }

        try {
            ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder(columnFamilyName.getBytes());
            columnFamilyDescriptorBuilder.setMaxVersions(maxVersions);
            StringBuilder sb = new StringBuilder(String.format("max version of column family = [%s]", maxVersions));
            if(compressionType != null){
                Compression.Algorithm algorithm = getHBaseCompression(compressionType);
                columnFamilyDescriptorBuilder.setCompactionCompressionType(algorithm);
                sb.append(String.format(", compression type of column family = [%s]", compressionType.toString()));
            }

            admin.modifyColumnFamily(TableName.valueOf(tableName), columnFamilyDescriptorBuilder.build());
            return Result.getResult(true, null, String.format("Updated column family name [%s] of HBase table [%s], %s.", columnFamilyName, tableName, sb.toString()));
        }
        catch (Exception e){
            return Result.getResult(false, null, String.format("Failed to update column family name [%s] of HBase table [%s], more info = [%s].", columnFamilyName, tableName, e.getMessage()));
        }
    }

    /**
     * get HBase table names
     * @param namespace namespace's name
     * @return the type of Result.data is [List<String>]
     */
    public Result getTableNames(String namespace){
        String moreTip = "";
        try {

            TableName[] tableNames;
            // if not assigned namespace, get all HBase table names
            if(namespace == null || namespace.trim().length() == 0) {
                tableNames = admin.listTableNames();
            }
            // if assigned namespace, get HBase table names of it
            else {
                tableNames = admin.listTableNamesByNamespace(namespace);
                moreTip = String.format(" from namespace [%s]", namespace);
            }

            List<String> tableNamesList = new LinkedList<>();
            for(TableName tableName: tableNames){
                tableNamesList.add(tableName.getNameAsString());
            }
            return Result.getResult(true, tableNamesList, String.format("Got HBase table names [%s]%s.", transformArrayToStringWithoutBracket((String[])tableNamesList.toArray()), moreTip));
        }
        catch (Exception e){
            return Result.getResult(false, null, String.format("Failed to get table names%s, more info = [%s].", namespace, e.getMessage()));
        }
    }

    /**
     * update column family name of HBase table
     * @param tableName table name
     * @param columnFamilyName new column family name
     * @return
     */
    public Result updateColumnFamily(String tableName, String columnFamilyName){
        return updateColumnFamily(tableName, columnFamilyName, maxVersionNumber, null);
    }

    /**
     * add row to HBase table
     * @param tableName table name
     * @param rowkey RowKey
     * @param columnFamilyName column family name
     * @param data data map
     * @return
     */
    public Result addDataRow(String tableName, String rowkey, String columnFamilyName, Map<String, Object> data){
        if(tableName == null || tableName.trim().length() == 0){
            return Result.getResult(false, null, "Failed to add row to HBase table, table name can't be null or empty.");
        }

        if(rowkey == null || rowkey.trim().length() == 0){
            return Result.getResult(false, null, String.format("Failed to add row to HBase table [%s], rowkey can't be null or empty.", tableName));
        }

        if(columnFamilyName == null || columnFamilyName.trim().length() == 0){
            return Result.getResult(false, null, String.format("Failed to add row with rowkey [%s] to HBase table [%s], column family name can't be null or empty.", rowkey, tableName));
        }

        if(data == null || data.size() == 0){
            return Result.getResult(false, null, String.format("Failed to add row with rowkey [%s] to HBase table [%s], data map can't be null or empty.", rowkey, tableName));
        }

        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            Put put = new Put(rowkey.getBytes());
            for(Map.Entry<String, Object> entry: data.entrySet()){
                put.addColumn(columnFamilyName.getBytes(), entry.getKey().getBytes(), ByteUtil.toByteArray(entry.getValue()));
            }
            table.put(put);
            table.close();

            return Result.getResult(true, false, String.format("Added row with rowkey [%s] to HBase table [%s].", rowkey, tableName));
        }
        catch (Exception e) {
            return Result.getResult(false, false, String.format("Failed to add row with rowkey [%s] to HBase table [%s], more info = [%s].", rowkey, tableName, e.getMessage()));
        }
    }

    /**
     * get row data from HBase table
     * @param tableName table name
     * @param rowkey RowKey of each row
     * @return the type of Result.data is [Map<String, Object>]
     */
    public Result getDataRow(String tableName, String rowkey){
        if(tableName == null || tableName.trim().length() == 0){
            return Result.getResult(false, null, String.format("Failed to get row data from HBase table, table name can't be null or empty."));
        }

        if(rowkey == null || rowkey.trim().length() == 0){
            return Result.getResult(false, null, String.format("Failed to get row data from HBase table [%s], rowkey can't be null or empty.", tableName));
        }

        try {
            Map<String, Object> row = new HashMap<>();
            Table table = connection.getTable(TableName.valueOf(tableName));
            Get get = new Get(rowkey.getBytes());
            org.apache.hadoop.hbase.client.Result result = table.get(get);
            if(result != null && !result.isEmpty()){
                for(Cell cell: result.listCells()){
                    row.put(Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()),
                            Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
                }
            }
            table.close();
            return Result.getResult(true, row, String.format("Got row data from HBase table [%s] by rowkey [%s].", tableName, rowkey));
        }
        catch (Exception e){
            return Result.getResult(false, null, String.format("Failed to get row data from HBase table [%s] by rowkey [%s], more info = [%s].", tableName, rowkey, e.getMessage()));
        }
    }

    /**
     * delete row from HBase table
     * @param tableName table name
     * @param rowkey RowKey
     * @return
     */
    public Result deleteDataRow(String tableName, String rowkey){
        if(tableName == null || tableName.trim().length() == 0){
            return Result.getResult(false, null, String.format("Failed to delete row from HBase table, table name can't be null or empty."));
        }

        if(rowkey == null || rowkey.trim().length() == 0){
            return Result.getResult(false, null, String.format("Failed to delete row from HBase table [%s], rowkey can't be null or empty.", tableName));
        }
        rowkey = rowkey.trim();

        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            Delete delete = new Delete(rowkey.getBytes());
            table.delete(delete);
            table.close();
            return Result.getResult(true, null, String.format("Deleted row from HBase table [%s] by rowkey [%s].", tableName, rowkey));
        }
        catch (Exception e){
            return Result.getResult(false, null, String.format("Failed to delete row from HBase table [%s] by rowkey [%s].", tableName, rowkey));
        }
    }

    /**
     * get rows data from HBase table
     * @param tableName table name
     * @param columns columns' map </br>
     *                the key is column family name, the value is column name
     * @param rowkeyPrefixFilter rowkey prefix filter to scan
     * @return the type of Result.data is [Map<String, Object>]  </br>
     *         the key of Map is [rowkey]
     *         the value of map is [Map<String, Object>], which key is [field name] and value is [field value]
     */
    public Result getDataRows(String tableName, Map<String, String> columns, String rowkeyPrefixFilter){
        Map<String, Object> data = new HashMap<>();
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            Scan scan = new Scan();
            // if assigned fields to get
            if(columns != null && columns.size() > 0){
                for(Map.Entry<String, String> columnName: columns.entrySet()){
                    // check if fields assigned exists, column name can't be null
                    if((columnName.getValue() == null || columnName.getValue().trim().length() == 0)){
                        return Result.getResult(false, null, String.format("Failed to get rows data from HBase table [%s], the columns' map contains [null] value by key [%s].", tableName, columnName.getKey()));
                    }
                    scan.addColumn(columnName.getKey().getBytes(), columnName.getValue().getBytes());
                }
            }
            // if assigned rowkey prefix filter to scan
            if(rowkeyPrefixFilter != null && rowkeyPrefixFilter.trim().length() > 0){
                scan.setRowPrefixFilter(rowkeyPrefixFilter.trim().getBytes());
            }

            /**
             * begin to scan
             */
            ResultScanner results = table.getScanner(scan);
            for(org.apache.hadoop.hbase.client.Result result = results.next(); result != null; result = results.next()){
                String rowkey = Bytes.toString(result.getRow());
                Map<String, String> columnsAndValue = new HashMap<>();
                for(Cell cell: result.rawCells()){
                    String columnFamily = Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength());
                    String columnName = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                    String columnValue = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                    columnsAndValue.put(columnFamily + ":" + columnName, columnValue);
                }

                data.put(rowkey, columnsAndValue);
            }
            results.close();
            table.close();
            return Result.getResult(true, data, String.format("Got rows data from HBase table [%s].", tableName));
        }
        catch (Exception e){
            return Result.getResult(true, data, String.format("Failed to get rows data from HBase table [%s], more info = [%s].", tableName, e.getMessage()));
        }
    }
}
