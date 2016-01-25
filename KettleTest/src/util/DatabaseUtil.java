package util;

import java.util.ArrayList;
import java.util.List;

import database.Database;
import database.Database.DATABASE_TYPE;
import database.Table;
import database.Table.TableColumn;
import env.Environment;

/**
 * 数据库相关的工具类
 */
public final class DatabaseUtil 
{   
    /**
     * 从源数据库复制所有表结构到目标表中，如果有重复的就删除重建。只有全部复制成功返回true
     * @param source 源表
     * @param dest   目的表
     */
    public static boolean copyScheme(Database source , Database dest)
    {
        List<Table> tables = new ArrayList<Table>();
        // 进行table结构的转换
        for (Table table : source.tables())
        {
            Table anotherTable = transformTable(source.databaseType() , dest.databaseType() , table);
            tables.add(anotherTable);
        }
        return dest.batchCreateTable(tables , true);
    }
    
    /**
     * 把源数据库中的表结构转成目标数据库中的表结构，可能存在数据库之间数据类型的转换
     * @param source 源数据库类型
     * @param dest 目的数据库类型
     * @param table 源数据库中的表结构
     */
    public static Table transformTable(DATABASE_TYPE source , DATABASE_TYPE dest , Table table)
    {
        // 如果是同一种类型
        if (source == dest)
            return table;
        else
        {
            // 实现不同数据库之间数据类型的转换
            Table ans = Table.newTable(table.getTableName());
            // 进行每一列的转换
            for (TableColumn column : table.getColumnList())
            {
                // 需要判断特殊的columnSize，如果是表示无穷大的-1，在mysql中需要用一个同样支持很长的值的数据结构来对应
                if (column.columnSize < 0)
                {
                    // the size is not used to create a column
                    ans.addColumn(TableColumn.newColumn(column.columnName , 
                                                        "MEDIUMTEXT" , 1));
                }
                else
                {
                    ans.addColumn(TableColumn.newColumn(column.columnName , 
                        transformColumnType(source , dest , column.columnType) ,
                        column.columnSize));
                }
            }
            return ans;
        }
    }
    
    /**
     * 进行列名的转换，如果转换失败返回null
     * @param source            源数据库类型
     * @param dest              目的数据库类型
     * @param sourceColumnName  源数据库字段类型
     */
    private static String transformColumnType(DATABASE_TYPE source , DATABASE_TYPE dest , String sourceColumnType)
    {
        // source is ORACLE
        if (source == DATABASE_TYPE.ORACLE)
        {
            if (dest == DATABASE_TYPE.MYSQL)
                return Environment.ORACLE_TO_MYSQL.get(sourceColumnType);
        }
        // source is SQLSERVER
        else if (source == DATABASE_TYPE.SQLSERVER)
        {
            if (dest == DATABASE_TYPE.ORACLE)
                return Environment.SQLSERVER_TO_ORACLE.get(sourceColumnType);
            else if (dest == DATABASE_TYPE.MYSQL)
                return Environment.SQLSERVER_TO_MYSQL.get(sourceColumnType);
        }
        // 如果不在范围之内
        return null;
    }
    
    /**
     * 获得给定一列对应的sql字符串，具体是否可行还需要测试。没有任何扩展性，只是针对这些数据库
     * @param 一列的信息
     */
    // TODO check the correctness
    public static String getSql(TableColumn column , DATABASE_TYPE databaseType)
    {
        // 如果长度是写死的，后面的column.columnSize就会没有意义，直接写入
        if (column.columnType.contains("(") || column.columnType.contains(")"))
            return String.format("`%s` %s" , column.columnName , column.columnType);
        else
        {
            // 特殊处理一下date
            if (column.columnType.contains("DATE"))
                return String.format("`%s` %s" , column.columnName , column.columnType);
            // 特殊处理一下blob
            else if (column.columnType.contains("BLOB"))
                return String.format("`%s` %s" , column.columnName , column.columnType);
            // 特殊处理一下timestamp
            else if (column.columnType.contains("TIMESTAMP"))
                return String.format("`%s` %s" , column.columnName , column.columnType);
            // 特殊处理一下text
            else if (column.columnType.contains("TEXT"))
                return String.format("`%s` %s" , column.columnName , column.columnType);
            // varchar在mysql里面特别处理一下
            else if (column.columnType.contains("VARCHAR") && databaseType == DATABASE_TYPE.MYSQL)
            {
                if (column.columnSize >= 255)
                    return String.format("`%s` %s" , column.columnName , "TEXT");
                else
                    return String.format("`%s` %s(%d)" , column.columnName , column.columnType , column.columnSize);
            }
            // 其它的就按照基本的格式获取
            else
                return String.format("`%s` %s(%d)" , column.columnName , column.columnType , column.columnSize);
        }
    }

}





