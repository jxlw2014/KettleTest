package database;

import java.util.ArrayList;
import java.util.List;

/**
 * ���ݿ��Ķ�Ӧ�ֻ࣬����һ��ʱ�ڵĽ�������������ݿ��б����ʵ��Ϣ
 */
public class Table
{
    /**
     * ���е�һ��
     */
    public static class TableColumn
    {
        public String columnName;
        public String columnType;
        public int columnSize;
        
        private TableColumn() { }
        
        /**
         * �õ��µ�һ��
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
    
    // �ײ���б�
    private List<TableColumn> columnList = new ArrayList<TableColumn>();
    // �������
    private String tableName;
    
    Table(String tableName) 
    {
        this.tableName = tableName;
    }
    
    /**
     * �õ�һ���µ�table����
     */
    public static Table newTable(String tableName)
    {
        return new Table(tableName);
    }
    
    /**
     * ���ر������
     */
    public String getTableName()
    {
        return tableName;
    }
    
    /**
     * ����µ�һ��
     */
    public Table addColumn(String columnName , String columnType , int columnSize)
    {
        this.columnList.add(TableColumn.newColumn(columnName , columnType , columnSize));
        return this;
    }
    
    /**
     * ����µ�һ��
     */
    public Table addColumn(TableColumn column)
    {
        this.columnList.add(column);
        return this;
    }
 
    /**
     * ���صײ��column�б�
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
