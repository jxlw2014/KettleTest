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
                ans.addColumn(TableColumn.newColumn(column.columnName , 
                    transformColumnType(source , dest , column.columnType) ,
                    column.columnSize));
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
        // source is oracle
        if (source == DATABASE_TYPE.ORACLE)
        {
            if (dest == DATABASE_TYPE.MYSQL)
                return Environment.ORACLE_TO_MYSQL.get(sourceColumnType);
        }
        // source is mysql
        else if (source == DATABASE_TYPE.MYSQL)
        {
            if (dest == DATABASE_TYPE.ORACLE)
                return Environment.MYSQL_TO_ORACLE.get(sourceColumnType);
        }
        // 如果不在范围之内
        return null;
    }
    
    /**
     * preview数据库中的表
     * @param 数据库
     * @param 表格
     * @param 数据的上限
     */
    public static Iterable<List<String>> previewData(Database database , Table table , int limit)
    {
        // TODO 考虑实现
        
        
        return null;
    }
    
    /**
     * 获得给定一列对应的sql字符串，实现方式很简陋，只适用于这里
     * @param 一列的信息
     */
    public static String getSql(TableColumn column)
    {
        // 一些不需要size
        if (column.columnType.contains("DATE"))
            return String.format("`%s` %s" , column.columnName , "CHAR(20)");
        else if (column.columnType.equals("TIMESTAMP"))
            return String.format("`%s` %s" , column.columnName , column.columnType);
        // 如果是varchar相关的
        else if (column.columnType.contains("VARCHAR"))
        {
            // 太长了就用text
            if (column.columnSize >= 255)
                return String.format("`%s` %s" , column.columnName , "TEXT");
            else
                return String.format("`%s` %s(%d)" , column.columnName , column.columnType , column.columnSize);
        }
        // 剩下的都需要size
        else
            return String.format("`%s` %s(%d)" , column.columnName , column.columnType , column.columnSize);
    }

}





