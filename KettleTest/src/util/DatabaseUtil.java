package util;

import java.util.ArrayList;
import java.util.List;

import database.Database;
import database.Database.DATABASE_TYPE;
import database.Table;
import database.Table.TableColumn;
import env.Environment;

/**
 * ���ݿ���صĹ�����
 */
public final class DatabaseUtil 
{   
    /**
     * ��Դ���ݿ⸴�����б�ṹ��Ŀ����У�������ظ��ľ�ɾ���ؽ���ֻ��ȫ�����Ƴɹ�����true
     * @param source Դ��
     * @param dest   Ŀ�ı�
     */
    public static boolean copyScheme(Database source , Database dest)
    {
        List<Table> tables = new ArrayList<Table>();
        // ����table�ṹ��ת��
        for (Table table : source.tables())
        {
            Table anotherTable = transformTable(source.databaseType() , dest.databaseType() , table);
            tables.add(anotherTable);
        }
        return dest.batchCreateTable(tables , true);
    }
    
    /**
     * ��Դ���ݿ��еı�ṹת��Ŀ�����ݿ��еı�ṹ�����ܴ������ݿ�֮���������͵�ת��
     * @param source Դ���ݿ�����
     * @param dest Ŀ�����ݿ�����
     * @param table Դ���ݿ��еı�ṹ
     */
    public static Table transformTable(DATABASE_TYPE source , DATABASE_TYPE dest , Table table)
    {
        // �����ͬһ������
        if (source == dest)
            return table;
        else
        {
            // ʵ�ֲ�ͬ���ݿ�֮���������͵�ת��
            Table ans = Table.newTable(table.getTableName());
            // ����ÿһ�е�ת��
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
     * ����������ת�������ת��ʧ�ܷ���null
     * @param source            Դ���ݿ�����
     * @param dest              Ŀ�����ݿ�����
     * @param sourceColumnName  Դ���ݿ��ֶ�����
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
        // ������ڷ�Χ֮��
        return null;
    }
    
    /**
     * preview���ݿ��еı�
     * @param ���ݿ�
     * @param ���
     * @param ���ݵ�����
     */
    public static Iterable<List<String>> previewData(Database database , Table table , int limit)
    {
        // TODO ����ʵ��
        
        
        return null;
    }
    
    /**
     * ��ø���һ�ж�Ӧ��sql�ַ�����ʵ�ַ�ʽ�ܼ�ª��ֻ����������
     * @param һ�е���Ϣ
     */
    public static String getSql(TableColumn column)
    {
        // һЩ����Ҫsize
        if (column.columnType.contains("DATE"))
            return String.format("`%s` %s" , column.columnName , "CHAR(20)");
        else if (column.columnType.equals("TIMESTAMP"))
            return String.format("`%s` %s" , column.columnName , column.columnType);
        // �����varchar��ص�
        else if (column.columnType.contains("VARCHAR"))
        {
            // ̫���˾���text
            if (column.columnSize >= 255)
                return String.format("`%s` %s" , column.columnName , "TEXT");
            else
                return String.format("`%s` %s(%d)" , column.columnName , column.columnType , column.columnSize);
        }
        // ʣ�µĶ���Ҫsize
        else
            return String.format("`%s` %s(%d)" , column.columnName , column.columnType , column.columnSize);
    }

}





