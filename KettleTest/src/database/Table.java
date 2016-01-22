package database;

import java.util.ArrayList;
import java.util.List;

/**
 * 数据库表的对应类，只保存一个时期的结果，不代表数据库中表的真实信息
 */
public class Table
{
    /**
     * 表中的一列
     */
    public static class TableColumn
    {
        public String columnName;
        public String columnType;
        public int columnSize;
        
        private TableColumn() { }
        
        /**
         * 得到新的一列
         */
        public static TableColumn newColumn(String columnName , String columnType , int columnSize)
        {
            TableColumn column = new TableColumn();
            column.columnName = columnName;
            column.columnType = columnType;
            column.columnSize = columnSize;
            return column;
        }
        
        @Override
        public String toString()
        {
            return String.format("%s %s(%d)" , columnName , columnType , columnSize);
        }
    }
    
    // 底层的列表
    private List<TableColumn> columnList = new ArrayList<TableColumn>();
    // 表的名称
    private String tableName;
    
    Table(String tableName) 
    {
        this.tableName = tableName;
    }
    
    /**
     * 得到一个新的table对象
     */
    public static Table newTable(String tableName)
    {
        return new Table(tableName);
    }
    
    /**
     * 返回表的名称
     */
    public String getTableName()
    {
        return tableName;
    }
    
    /**
     * 添加新的一列
     */
    public Table addColumn(String columnName , String columnType , int columnSize)
    {
        this.columnList.add(TableColumn.newColumn(columnName , columnType , columnSize));
        return this;
    }
    
    /**
     * 添加新的一列
     */
    public Table addColumn(TableColumn column)
    {
        this.columnList.add(column);
        return this;
    }
 
    /**
     * 返回底层的column列表
     */
    public List<TableColumn> getColumnList()
    {
        return this.columnList;
    }
    
    @Override
    public String toString()
    {
        boolean first = true;
        StringBuilder builder = new StringBuilder();
        builder.append("[");
        for (TableColumn column : columnList)
        {
            if (first)
                first = false;
            else
                builder.append(", ");
            builder.append(column.toString());
        }
        builder.append("]");
        return builder.toString();
    }
        
}
