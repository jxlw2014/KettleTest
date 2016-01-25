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
                // ��Ҫ�ж������columnSize������Ǳ�ʾ������-1����mysql����Ҫ��һ��ͬ��֧�ֺܳ���ֵ�����ݽṹ����Ӧ
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
     * ����������ת�������ת��ʧ�ܷ���null
     * @param source            Դ���ݿ�����
     * @param dest              Ŀ�����ݿ�����
     * @param sourceColumnName  Դ���ݿ��ֶ�����
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
        // ������ڷ�Χ֮��
        return null;
    }
    
    /**
     * ��ø���һ�ж�Ӧ��sql�ַ����������Ƿ���л���Ҫ���ԡ�û���κ���չ�ԣ�ֻ�������Щ���ݿ�
     * @param һ�е���Ϣ
     */
    // TODO check the correctness
    public static String getSql(TableColumn column , DATABASE_TYPE databaseType)
    {
        // ���������д���ģ������column.columnSize�ͻ�û�����壬ֱ��д��
        if (column.columnType.contains("(") || column.columnType.contains(")"))
            return String.format("`%s` %s" , column.columnName , column.columnType);
        else
        {
            // ���⴦��һ��date
            if (column.columnType.contains("DATE"))
                return String.format("`%s` %s" , column.columnName , column.columnType);
            // ���⴦��һ��blob
            else if (column.columnType.contains("BLOB"))
                return String.format("`%s` %s" , column.columnName , column.columnType);
            // ���⴦��һ��timestamp
            else if (column.columnType.contains("TIMESTAMP"))
                return String.format("`%s` %s" , column.columnName , column.columnType);
            // ���⴦��һ��text
            else if (column.columnType.contains("TEXT"))
                return String.format("`%s` %s" , column.columnName , column.columnType);
            // varchar��mysql�����ر���һ��
            else if (column.columnType.contains("VARCHAR") && databaseType == DATABASE_TYPE.MYSQL)
            {
                if (column.columnSize >= 255)
                    return String.format("`%s` %s" , column.columnName , "TEXT");
                else
                    return String.format("`%s` %s(%d)" , column.columnName , column.columnType , column.columnSize);
            }
            // �����ľͰ��ջ����ĸ�ʽ��ȡ
            else
                return String.format("`%s` %s(%d)" , column.columnName , column.columnType , column.columnSize);
        }
    }

}





