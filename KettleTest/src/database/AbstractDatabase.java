package database;

import java.util.List;

import org.pentaho.di.core.database.DatabaseMeta;

/**
 * 通用的kettle相关的数据库实现
 */
public abstract class AbstractDatabase implements Database
{
    /**
     *  数据库元数据
     */
    protected DatabaseMeta databaseMeta;
    /**
     *  数据库的名称
     */
    protected String databaseName;
    /**
     *  数据库的类型
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
