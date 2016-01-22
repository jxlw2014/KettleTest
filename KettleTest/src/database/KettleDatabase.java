package database;

import org.pentaho.di.core.database.DatabaseMeta;

/**
 * Database在kettle方面的扩展功能
 */
public interface KettleDatabase 
{
    /**
     * 获得数据库对应的databaseMeta对象
     */
    public DatabaseMeta databaseMeta();
}
