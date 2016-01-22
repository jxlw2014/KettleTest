package database;

import java.util.List;

import org.pentaho.di.core.database.DatabaseMeta;

/**
 * ͨ�õ�kettle��ص����ݿ�ʵ��
 */
public abstract class AbstractDatabase implements Database
{
    /**
     *  ���ݿ�Ԫ����
     */
    protected DatabaseMeta databaseMeta;
    /**
     *  ���ݿ������
     */
    protected String databaseName;
    /**
     *  ���ݿ������
     */
    protected DATABASE_TYPE databaseType;
    
    @Override
    public boolean containsTable(String tablename)
    {
        List<Table> tableList = tables();
        for (Table table : tableList)
        {
            if (table.getTableName().equalsIgnoreCase(tablename))
                return true;
        }
        return false;
    }
    
    @Override
    public DatabaseMeta databaseMeta() 
    {
        return databaseMeta;
    }

    @Override
    public String databaseName() 
    {
        return databaseName;
    }

    @Override
    public DATABASE_TYPE databaseType() 
    {
        return databaseType;
    }

}
