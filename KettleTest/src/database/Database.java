package database;

import java.util.List;

/**
 * 通用的数据库实现接口，涉及到与表的相关操作
 */
public interface Database extends KettleDatabase
{
    /**
     * 数据库的类型
     */
    public enum DATABASE_TYPE
    {
        ORACLE("Oracle") , MYSQL("Mysql") , SQLSERVER("MSSQL");
     
        private String codename;
        
        private DATABASE_TYPE(String codename)
        {
            this.codename = codename;
        }
        
        /**
         * 从数据库名字得到对应的对象，如果没有对应的对象返回null
         * @param name 数据库的名称
         */
        public static DATABASE_TYPE of(String name)
        {
            name = name.toUpperCase();
            for (DATABASE_TYPE type : DATABASE_TYPE.values())
            {
                // 如果名字一致
                if (type.name().toUpperCase().equals(name))
                    return type;
            }
            return null;
        }
        
        /**
         * 返回代码中使用的名称
         */
        public String toString()
        {
            return this.codename;
        }
    }
    
    /**
     * 获得数据库中对应的表的信息
     */
    public List<Table> tables();
    
    /**
     * 数据库中是否包含表tablename，以上一次获得的数据库信息为判断依据
     */
    public boolean containsTable(String tablename);

    /**
     * 获得数据库的名称
     */
    public String databaseName();
    
    /**
     * 获得数据库的类型
     */
    public DATABASE_TYPE databaseType();

    /**
     * 进行数据库表信息的更新，这里的更新是一定会重新检查一遍，不断调用可能会影响效率
     */
    public void refreshTables();
    
    /**
     * 在数据库中强行建立对应的表，如果表名已经存在就进行删除后重建。如果建立失败返回false，否则返回true
     */
    public boolean forceCreateTable(Table table);
    
    /**
     * 在数据库中建表，如果表名已经存在则返回false
     */
    public boolean createTable(Table table);
    
    /**
     * 删除一个表
     */
    public boolean dropTable(String tableName);
    
    /**
     * 批量创建表
     * @param tables 需要建立的表
     * @param isForce 对于已经存在的表名，是采用删除重建还是放弃建立
     */
    public boolean batchCreateTable(Iterable<Table> tables , boolean isForce);
    
    /**
     * 断开数据库的连接
     */
    public void disconnect();
}




