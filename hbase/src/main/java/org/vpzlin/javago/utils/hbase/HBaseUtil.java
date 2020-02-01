package org.vpzlin.javago.utils.hbase;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;
import org.vpzlin.javago.utils.Result;

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
            if(!admin.isTableDisabled(TableName.valueOf(tableName)){
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
     * @param forceDisableTable force disable table if is not disabled firstly
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
     * 获取HBase列族的压缩格式
     * @param compressionType 压缩格式（自定义类）
     * @return 压缩格式枚举（算法类）
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
     * 新建HBase表
     * @param tableName 表名
     * @param columnFamilyName 列族名
     * @return 结果：              <br/>
     *      0:成功                 <br/>
     *      1:执行失败             <br/>
     *      2:表名为空             <br/>
     *      3:列族名为空           <br/>
     *      4:最大数据版本数异常   <br/>
     *      5:表已存在             <br/>
     */
    public int createTable(String tableName, String columnFamilyName){
        String[] columnFamilyNames = {columnFamilyName};
        return createTable(tableName, columnFamilyNames, HBaseCompressionType.NONE, maxVersionNumber);
    }

    /**
     * 新建HBase表
     * @param tableName 表名
     * @param columnFamilyNames 列族名
     * @return 结果：              <br/>
     *      0:成功                 <br/>
     *      1:执行失败             <br/>
     *      2:表名为空             <br/>
     *      3:列族名为空           <br/>
     *      4:最大数据版本数异常   <br/>
     *      5:表已存在             <br/>
     */
    public int createTable(String tableName, String[] columnFamilyNames){
        return createTable(tableName, columnFamilyNames, HBaseCompressionType.NONE, defaultMaxVersion);
    }

    /**
     * 新建HBase表
     * @param tableName 表名
     * @param columnFamilyName 列族名
     * @param compressionType 压缩格式
     * @param maxVersions 最大数据版本数
     * @return 结果：              <br/>
     *      0:成功                 <br/>
     *      1:执行失败             <br/>
     *      2:表名为空             <br/>
     *      3:列族名为空           <br/>
     *      4:最大数据版本数异常   <br/>
     *      5:表已存在             <br/>
     */
    public int createTable(String tableName, String columnFamilyName, HBaseCompressionType compressionType, int maxVersions){
        String[] columnFamilyNames = {columnFamilyName};
        return createTable(tableName, columnFamilyNames, compressionType, maxVersions);
    }

    /**
     * 新建HBase表
     * @param tableName 表名
     * @param columnFamilyNames 列族名集
     * @param compressionType 压缩格式
     * @param maxVersions 最大数据版本数
     * @return 结果：              <br/>
     *      0:成功                 <br/>
     *      1:执行失败             <br/>
     *      2:表名为空             <br/>
     *      3:列族名为空           <br/>
     *      4:最大数据版本数异常   <br/>
     *      5:表已存在             <br/>
     */
    public int createTable(String tableName, String[] columnFamilyNames, HBaseCompressionType compressionType, int maxVersions){
        if(tableName == null || tableName.trim().length() == 0){
            logger.error("新建HBase表失败！表名不能为空。");
            return 2;
        }
        tableName = tableName.trim();
        if(columnFamilyNames == null || columnFamilyNames.length == 0){
            logger.error("新建HBase表失败！列族名不能为空。");
            return 3;
        }
        if(maxVersions < 1){
            logger.error("新建HBase表失败！最大数据版本数必须大于0。");
            return 4;
        }
        try {
            if (admin.tableExists(TableName.valueOf(tableName)) == true) {
                logger.error("新建HBase表失败！表[" + tableName + "]已存在。");
                return 5;
            }

            // 添加列族
            Compression.Algorithm algorithm = getHBaseCompression(compressionType);
            TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName));
            for (String columnFamilyName : columnFamilyNames) {
                ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder(
                        Bytes.toBytes(columnFamilyName));
                // 指定了压缩算法格式
                if(compressionType != null){
                    columnFamilyDescriptorBuilder.setCompactionCompressionType(algorithm);
                }
                columnFamilyDescriptorBuilder.setMaxVersions(maxVersions);
                tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptorBuilder.build());
            }
            admin.createTable(tableDescriptorBuilder.build());
            logger.info("成功新建了HBase表[" + tableName + "]，列族名为" + Arrays.toString(columnFamilyNames) + "。");
            return 0;
        }
        catch (Exception e){
            e.printStackTrace();
            logger.error("新建了HBase表失败！表名为[" + tableName + "]，列族名为[" + Arrays.toString(columnFamilyNames) + "]。");
            return 1;
        }
    }

    /**
     * 新建或重建HBase表
     * @param tableName 表名
     * @param columnFamilyName 列族名
     * @return 结果：              <br/>
     *      0:成功                 <br/>
     *      1:执行失败             <br/>
     *      2:表名不能为空         <br/>
     *      3:列族名不能为空       <br/>
     *      4:最大数据版本数异常   <br/>
     */
    public int createOrOverwriteTable(String tableName, String columnFamilyName){
        String[] columnFamilyNames = {columnFamilyName};
        return createOrOverwriteTable(tableName, columnFamilyNames, HBaseCompressionType.NONE, defaultMaxVersion);
    }

    /**
     * 新建或重建HBase表
     * @param tableName 表名
     * @param columnFamilyNames 列族名
     * @return 结果：              <br/>
     *      0:成功                 <br/>
     *      1:执行失败             <br/>
     *      2:表名不能为空         <br/>
     *      3:列族名不能为空       <br/>
     *      4:最大数据版本数异常   <br/>
     */
    public int createOrOverwriteTable(String tableName, String[] columnFamilyNames){
        return createOrOverwriteTable(tableName, columnFamilyNames, HBaseCompressionType.NONE, defaultMaxVersion);
    }

    /**
     * 新建或重建HBase表
     * @param tableName 表名
     * @param columnFamilyName 列族名
     * @param compressionType 压缩格式
     * @param maxVersions 最大数据版本数
     * @return 结果：              <br/>
     *      0:成功                 <br/>
     *      1:执行失败             <br/>
     *      2:表名不能为空         <br/>
     *      3:列族名不能为空       <br/>
     *      4:最大数据版本数异常   <br/>
     */
    public int createOrOverwriteTable(String tableName, String columnFamilyName, HBaseCompressionType compressionType, int maxVersions){
        String[] columnFamilyNames = {columnFamilyName};
        return createOrOverwriteTable(tableName, columnFamilyNames, HBaseCompressionType.NONE, defaultMaxVersion);
    }

    /**
     * 新建或重建HBase表
     * @param tableName 表名
     * @param columnFamilyNames 列族名
     * @param compressionType 压缩格式
     * @param maxVersions 最大数据版本数
     * @return 结果：              <br/>
     *      0:成功                 <br/>
     *      1:执行失败             <br/>
     *      2:表名不能为空         <br/>
     *      3:列族名不能为空       <br/>
     *      4:最大数据版本数异常   <br/>
     */
    public int createOrOverwriteTable(String tableName, String[] columnFamilyNames, HBaseCompressionType compressionType, int maxVersions){
        if(tableName == null || tableName.trim().length() == 0){
            logger.error("新建或重建HBase表失败！表名不能为空。");
            return 2;
        }
        tableName = tableName.trim();
        if(columnFamilyNames == null || columnFamilyNames.length == 0){
            logger.error("新建或重建HBase表失败！列族名不能为空。");
            return 3;
        }
        if(maxVersions < 1){
            logger.error("新建或重建HBase表失败！最大数据版本数必须大于0。");
            return 4;
        }
        try {
            if(admin.tableExists(TableName.valueOf(tableName)) == true){
                logger.info("开始重建HBase表[" + tableName + "]，列族名为" + Arrays.toString(columnFamilyNames) + "。");
                admin.disableTable(TableName.valueOf(tableName));
                logger.info("成功禁用了HBase表[" + tableName + "]。");
                admin.deleteTable(TableName.valueOf(tableName));
                logger.info("成功删除了HBase表[" + tableName + "]。");
            }
            // 添加列族
            Compression.Algorithm algorithm = getHBaseCompression(compressionType);
            TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName));
            for (String columnFamilyName : columnFamilyNames) {
                ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder(
                        Bytes.toBytes(columnFamilyName));
                // 指定了压缩算法格式
                if(compressionType != null){
                    columnFamilyDescriptorBuilder.setCompactionCompressionType(algorithm);
                }
                columnFamilyDescriptorBuilder.setMaxVersions(maxVersions);
                tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptorBuilder.build());
            }
            admin.createTable(tableDescriptorBuilder.build());
            logger.info("成功新建了HBase表[" + tableName + "]，列族名为" + Arrays.toString(columnFamilyNames) + "。");
            return 0;
        }
        catch (Exception e){
            e.printStackTrace();
            logger.info("新建或重建HBase表失败！表名为[" + tableName + "]，列族名为" + Arrays.toString(columnFamilyNames) + "。");
            return 1;
        }
    }

    /**
     * 删除HBase表
     * @param tableName 表名
     * @return 结果：              <br/>
     *      0:成功                 <br/>
     *      1:执行失败             <br/>
     *      2:表名不能为空         <br/>
     *      3.表不存在             <br/>
     */
    public int deleteTable(String tableName){
        if(tableName == null || tableName.trim().length() == 0){
            logger.error("删除HBase表失败！表名不能为空。");
            return 2;
        }
        tableName = tableName.trim();

        try {
            logger.info("开始删除HBase表[" + tableName + "]。");
            if(admin.tableExists(TableName.valueOf(tableName)) == false){
                logger.error("删除HBase表失败！表[" + tableName + "]不存在。");
                return 3;
            }
            admin.disableTable(TableName.valueOf(tableName));
            logger.info("成功禁用了HBase表[" + tableName + "]。");
            admin.deleteTable(TableName.valueOf(tableName));
            logger.info("成功删除了HBase表[" + tableName + "]。");
            return 0;
        }
        catch (Exception e){
            e.printStackTrace();
            logger.error("删除HBase表失败！表名=[" + tableName + "]。");
            return 1;
        }
    }

    /**
     * 添加列族
     * @param tableName 表名
     * @param columnFamilyName 列族名
     * @return 结果：                <br/>
     *      0:成功                   <br/>
     *      1:执行失败               <br/>
     *      2:表名不能为空           <br/>
     *      3:列族名不能为空         <br/>
     *      4:最大数据版本数异常     <br/>
     */
    public int addColumnFamily(String tableName, String columnFamilyName){
        String[] columnFamilyNames = {columnFamilyName};
        return addColumnFamilies(tableName, columnFamilyNames, HBaseCompressionType.NONE, defaultMaxVersion);
    }

    /**
     * 添加列族
     * @param tableName 表名
     * @param columnFamilyNames 列族名
     * @return 结果：                <br/>
     *      0:成功                   <br/>
     *      1:执行失败               <br/>
     *      2:表名不能为空           <br/>
     *      3:列族名不能为空         <br/>
     *      4:最大数据版本数异常     <br/>
     */
    public int addColumnFamilies(String tableName, String[] columnFamilyNames){
        return addColumnFamilies(tableName, columnFamilyNames, HBaseCompressionType.NONE, defaultMaxVersion);
    }

    /**
     * 添加列族
     * @param tableName 表名
     * @param columnFamilyNames 列族名
     * @param compressionType 压缩格式
     * @param maxVersions 最大数据版本数
     * @return 结果：                <br/>
     *      0:成功                   <br/>
     *      1:执行失败               <br/>
     *      2:表名不能为空           <br/>
     *      3:列族名不能为空         <br/>
     *      4:最大数据版本数异常     <br/>
     */
    public int addColumnFamilies(String tableName, String[] columnFamilyNames, HBaseCompressionType compressionType, int maxVersions){
        if(tableName == null || tableName.trim().length() == 0){
            logger.error("添加列族失败！表名不能为空。");
            return 2;
        }
        tableName = tableName.trim();
        if(columnFamilyNames == null || columnFamilyNames.length == 0){
            logger.error("添加列族失败！列族名不能为空。");
            return 3;
        }
        if(maxVersions < 1){
            logger.error("添加列族失败！最大数据版数必须大于0。");
            return 4;
        }
        // 添加列族
        Compression.Algorithm algorithm = getHBaseCompression(compressionType);
        try {
            for (String columnFamilyName : columnFamilyNames) {
                ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(columnFamilyName));
                if(compressionType != null){
                    columnFamilyDescriptorBuilder.setCompactionCompressionType(algorithm);
                }
                columnFamilyDescriptorBuilder.setMaxVersions(maxVersions);

                admin.addColumnFamily(TableName.valueOf(tableName), columnFamilyDescriptorBuilder.build());
                logger.info("成功添加了表[" + tableName + "]的列族[" + columnFamilyName + "]。");
            }
            return 0;
        }
        catch (Exception e){
            e.printStackTrace();
            logger.error("添加表[" + tableName + "]的列族失败！列族名为" + Arrays.toString(columnFamilyNames) + "。");
            return 1;
        }
    }

    /**
     * 获取HBase表的列族名
     * @param tableName 表名
     * @return 列族名数组                    <br/>
     *         null: 表不存在 或 执行失败    <br/>
     */
    public List<String> getTableColumnFamilyNames(String tableName){
        if(tableName == null || tableName.trim().length() == 0){
            logger.error("获取HBase表的列族名失败！表名不能为空。");
            return null;
        }
        tableName = tableName.trim();
        try {
            if(admin.isTableAvailable(TableName.valueOf(tableName)) == false){
                logger.error("获取HBase表[" + tableName + "]的列族名失败！该表目前不可用。");
                return null;
            }
            Table table = connection.getTable(TableName.valueOf(tableName));
            TableDescriptor tableDescriptor = table.getDescriptor();
            Set<byte[]> columnFamilyNames = tableDescriptor.getColumnFamilyNames();
            List<String> columnFamilyNamesList = new LinkedList<>();
            for(byte[] columnFamilyName: columnFamilyNames){
                columnFamilyNamesList.add(Bytes.toString(columnFamilyName));
            }

            return columnFamilyNamesList;
        }
        catch (Exception e){
            e.printStackTrace();
            logger.error("获取HBase表[" + tableName + "]的列族名失败！");
            return null;
        }
    }

    /**
     * 删除列族
     * @param tableName 表名
     * @param columnFamilyName 列族名
     * @return 结果：          <br/>
     *      0:成功             <br/>
     *      1:执行失败         <br/>
     *      2:表名不能为空     <br/>
     *      3:列族名不能为空   <br/>
     */
    public int deleteColumnFamily(String tableName, String columnFamilyName){
        String[] columnFamilyNames = {columnFamilyName};
        return deleteColumnFamilies(tableName, columnFamilyNames);
    }

    /**
     * 删除列族
     * @param tableName 表名
     * @param columnFamilyNames 列族名
     * @return 结果：          <br/>
     *      0:成功             <br/>
     *      1:执行失败         <br/>
     *      2:表名不能为空     <br/>
     *      3:列族名不能为空   <br/>
     */
    public int deleteColumnFamilies(String tableName, String[] columnFamilyNames){
        if(tableName == null || tableName.trim().length() == 0){
            logger.error("删除列族失败！表名不能为空。");
            return 2;
        }
        tableName = tableName.trim();
        if(columnFamilyNames == null || columnFamilyNames.length == 0){
            logger.error("删除列族失败！列族名不能为空。");
            return 3;
        }

        try {
            for (String columnFamilyName : columnFamilyNames) {
                admin.deleteColumnFamily(TableName.valueOf(tableName), columnFamilyName.getBytes("UTF-8"));
                logger.info("成功删除了表[" + tableName + "]的列族[" + columnFamilyName + "]。");
            }
            return 0;
        }
        catch (Exception e){
            e.printStackTrace();
            logger.error("删除HBase表[" + tableName + "]的列族失败！");
            return 1;
        }
    }

    /**
     * 更新列族
     * @param tableName 表名
     * @param columnFamilyName 列族名
     * @param compressionType 压缩格式
     * @return 结果：              <br/>
     *      0:成功                 <br/>
     *      1:执行失败             <br/>
     *      2:表名不能为空         <br/>
     *      3.列族名不能为空       <br/>
     */
    public int updateColumnFamily(String tableName, String columnFamilyName, HBaseCompressionType compressionType){
        return updateColumnFamily(tableName, columnFamilyName, compressionType, 0);
    }

    /**
     * 更新列族
     * @param tableName 表名
     * @param columnFamilyName 列族名
     * @param maxVersions 最大数据版本数
     * @return 结果：              <br/>
     *      0:成功                 <br/>
     *      1:执行失败             <br/>
     *      2:表名不能为空         <br/>
     *      3:列族名不能为空       <br/>
     */
    public int updateColumnFamily(String tableName, String columnFamilyName, int maxVersions){
        return updateColumnFamily(tableName, columnFamilyName, null, maxVersions);
    }

    /**
     * 更新列族
     * @param tableName 表名
     * @param columnFamilyName 列族名
     * @param compressionType 压缩格式
     * @param maxVersions 最大数据版本数（值0表示不做更新）
     * @return 结果：              <br/>
     *      0:成功                 <br/>
     *      1:执行失败             <br/>
     *      2:表名不能为空         <br/>
     *      3.列族名不能为空       <br/>
     */
    public int updateColumnFamily(String tableName, String columnFamilyName, HBaseCompressionType compressionType, int maxVersions){
        if(tableName == null || tableName.trim().length() == 0){
            logger.error("更新列族失败！表名不能为空。");
            return 2;
        }
        tableName = tableName.trim();
        if(columnFamilyName == null || columnFamilyName.trim().length() == 0){
            logger.error("更新列族失败！列族名不能为空。");
            return 3;
        }

        // 添加列族
        Compression.Algorithm algorithm = getHBaseCompression(compressionType);
        StringBuilder sb = new StringBuilder();
        try {
            ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder(columnFamilyName.getBytes());
            if(compressionType != null){
                columnFamilyDescriptorBuilder.setCompactionCompressionType(algorithm);
                sb.append("压缩算法为" + compressionType.name() + "。");
            }
            if(maxVersions > 0) {
                columnFamilyDescriptorBuilder.setMaxVersions(maxVersions);
                sb.append("最大数据版本数为[" + maxVersions + "]。");
            }
            admin.modifyColumnFamily(TableName.valueOf(tableName), columnFamilyDescriptorBuilder.build());
            logger.info("更新表[" + tableName + "]列族[" + columnFamilyName + "]的属性成功。" + sb.toString());
            return 0;
        }
        catch (Exception e){
            e.printStackTrace();
            logger.error("更新表[" + tableName + "]列族[" + columnFamilyName + "]的属性失败！" + sb.toString());
            return 1;
        }
    }

    /**
     * 添加HBase表数据
     * @param tableName 表名
     * @param rowkey 主键
     * @param columnFamilyName 列族名
     * @param data 字段名、字段值的键值对
     * @return 结果：              <br/>
     *      0:成功                 <br/>
     *      1:执行失败             <br/>
     *      2:表名为空             <br/>
     *      3:RowKey为空           <br/>
     *      4:列族名为空           <br/>
     *      5:待插入数据为空       <br/>
     */
    public int addRow(String tableName, String rowkey, String columnFamilyName, Map<String, String> data){
        if(tableName == null || tableName.trim().length() == 0){
            logger.error("添加HBase表数据失败！表名不能为空。");
            return 2;
        }
        tableName = tableName.trim();
        if(rowkey == null || rowkey.trim().length() == 0){
            logger.error("添加HBase表数据失败！RowKey为空。");
            return 3;
        }
        rowkey = rowkey.trim();
        if(columnFamilyName == null || columnFamilyName.trim().length() == 0){
            logger.error("添加HBase表数据失败！列族名为空。");
            return 4;
        }
        columnFamilyName = columnFamilyName.trim();
        if(data == null || data.size() == 0){
            logger.error("添加HBase表数据失败！RowKey为空。");
            return 5;
        }

        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            Put put = new Put(rowkey.getBytes());
            // 添加字段值
            for(Map.Entry<String, String> entry: data.entrySet()){
                put.addColumn(columnFamilyName.getBytes(), entry.getKey().getBytes(), entry.getValue().getBytes());
            }
            table.put(put);
            table.close();
            return 0;
        }
        catch (Exception e) {
            e.printStackTrace();
            logger.error("添加HBase表数据失败！表名为[" + tableName + "]，RowKey为[" + rowkey + "]，列族名为[" + columnFamilyName + "]。");
            return 1;
        }
    }

    /**
     * 获取HBase表数据行
     * @param tableName 表名
     * @param rowkey 主键
     * @return 字段键值对的Map
     * @throws
     */
    public Map<String, String> getRow(String tableName, String rowkey) throws Exception{
        if(tableName == null || tableName.trim().length() == 0){
            throw new Exception("获取HBase表数据行失败！表名不能为空。");
        }
        tableName = tableName.trim();
        if(rowkey == null || rowkey.trim().length() == 0){
            throw new Exception("获取HBase表数据行失败！RowKey为空。");
        }
        rowkey = rowkey.trim();

        try {
            Map<String, String> row = new HashMap<>();
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
            return row;
        }
        catch (Exception e){
            e.printStackTrace();
            return null;
        }
    }

    /**
     * 删除HBase表数据行
     * @param tableName 表名
     * @param rowkey 主键
     * @return 结果：          <br/>
     *      0:正常             <br/>
     *      1:失败             <br/>
     *      2:表名为空         <br/>
     *      3:RowKey为空       <br/>
     */
    public int deleteRow(String tableName, String rowkey){
        if(tableName == null || tableName.trim().length() == 0){
            logger.error("删除HBase表数据行失败！表名不能为空");
            return 2;
        }
        tableName = tableName.trim();
        if(rowkey == null || rowkey.trim().length() == 0){
            logger.error("删除HBase表数据行失败！RowKey不能为空");
            return 3;
        }
        rowkey = rowkey.trim();

        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            Delete delete = new Delete(rowkey.getBytes());
            table.delete(delete);
            logger.debug("删除了HBase表[" + tableName + "]中RowKey为[" + rowkey + "]的数据。");
            table.close();
            return 0;
        }
        catch (Exception e){
            e.printStackTrace();
            logger.error("删除HBase表数据行失败！");
            return 1;
        }
    }

    /**
     * 获取HBase中的表名
     * @return 表名list。如果没有表则list.size()=0 ；如果执行失败，则 list = null
     */
    public List<String> getTableNames(){
        return getTableNames(null);
    }

    /**
     * 获取HBase中的表名
     * @param namespace 指定表的命名空间，null表示不指定
     * @return 表名list。如果没有表则list.size()=0 ；如果执行失败，则 list = null
     */
    public List<String> getTableNames(String namespace){
        List<String> tableNamesList = new LinkedList<>();
        try {
            TableName[] tableNames;
            // 未指定namespace，获取所有表名
            if(namespace == null || namespace.trim().length() == 0) {
                tableNames = admin.listTableNames();
            }
            // 指定namespace
            else {
                tableNames = admin.listTableNamesByNamespace(namespace);
            }

            for(TableName tableName: tableNames){
                tableNamesList.add(tableName.getNameAsString());
            }
            return tableNamesList;
        }
        catch (Exception e){
            e.printStackTrace();
            logger.error("获取HBase中的表名失败！");
            return null;
        }
    }

    /**
     * 扫描HBase表的数据
     * @param tableName 表名
     * @param columnNames 指定要获取值的列，key为列族，value
     * @param rowkeyStartWith 指定要扫描数据RowKey的开头
     * @return 执行失败返回 null，否则返回map键值对的嵌套。map.size() = 0 不是执行失败，表示未获取到数据。 <br/>
     *      Map<String, object>                    <br/>
     *          key: RowKey                        <br/>
     *          value: Map<String, String>         <br/>
     *                     key: 字段名            <br/>
     *                     value: 字段值          <br/>
     */
    public Map<String, Object> scanData(String tableName, Map<String, String> columnNames, String rowkeyStartWith){
        Map<String, Object> data = new HashMap<>();
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            Scan scan = new Scan();
            // 指定要获取值的列
            if(columnNames != null && columnNames.size() > 0){
                for(Map.Entry<String, String> columnName: columnNames.entrySet()){
                    // 字段名为空
                    if(columnName.getValue() == null || columnName.getValue().trim().length() == 0){
                        logger.error("扫描HBase表的数据失败！传入了有列族名[" + columnName.getKey() + "]但没有列名的字段。");
                        return null;
                    }
                    scan.addColumn(columnName.getKey().getBytes(), columnName.getValue().getBytes());
                }
            }
            // 指定要扫描数据RowKey的开头
            if(rowkeyStartWith != null && rowkeyStartWith.trim().length() > 0){
                scan.setRowPrefixFilter(rowkeyStartWith.trim().getBytes());
            }

            // 开始扫描
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
            return data;
        }
        catch (Exception e){
            e.printStackTrace();
            logger.error("扫描HBase表的数据失败！");
            return null;
        }
    }
}
