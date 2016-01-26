package database;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.pentaho.di.core.database.DatabaseMeta;

import util.DatabaseUtil;
import database.Table.TableColumn;

/**
 * SQLServer的对应实现，SQLSERVER版本为2008r2
 */
public class SQLServerDatabase extends AbstractDatabase 
{
    /**
     * Mysql数据库对应的Builder类
     */
    public static class Builder implements common.Builder<SQLServerDatabase>
    {
        private String username = null;
        private String password = null;
        private String databasename = null;
        private String ip = null;
        private int port = - 1;
        
        private Builder() { }
        
        /**
         * 得到一个新的builder实例
         */
        public static Builder newBuilder()
        {
            return new Builder();
        }
        
        public Builder setUsername(String username)
        {
            this.username = username;
            return this;
        }
        
        public Builder setPassword(String password)
        {
            this.password = password;
            return this;
        }
        
        public Builder setDatabasename(String databasename)
        {
            this.databasename = databasename;
            return this;
        }
        
        public Builder setIp(String ip)
        {
            this.ip = ip;
            return this;
        }
        
        public Builder setPort(int port)
        {
            this.port = port;
            return this;
        }

        /**
         * 建立数据库对象，如果建立失败返回null
         */
        @Override
        public synchronized SQLServerDatabase build() 
        {
            // 先进行预先判断
            if (previousCheck())
            {
                SQLServerDatabase instance = new SQLServerDatabase();
                try
                {
                    instance.init(username , password , databasename , ip , port);
                } catch (Exception e)
                {
                    System.out.println("数据库初始化失败," + e.getMessage());
                    instance = null;
                }
                // 返回结果
                return instance;
            }
            else
                return null;
        }
        
        // 预先的判断
        private boolean previousCheck()
        {
            if (this.username == null)
            {
                System.out.println("username没有设置");
                return false;
            }
            else if (this.password == null)
            {
                System.out.println("password没有设置");
                return false;
            }
            else if (this.databasename == null)
            {
                System.out.println("databasename没有设置");
                return false;
            }
            else if (this.ip == null)
            {
                System.out.println("数据库服务器ip没有设置");
                return false;
            }
            else if (this.port < 0)
            {
                System.out.println("数据库服务器端口没有设置");
                return false;
            }
            else
                return true;
        }
    }
    
    /**
     * 获得列的数字到名称的映射
     */
    private static final Map<Integer , String> typenameMap = new HashMap<Integer , String>();
    
    static
    {
        // 初始化typenameMap，硬编码即可，因为这个东西是不变的
        addToMap(34 , "image");
        addToMap(35 , "text");
        addToMap(36 , "uniqueidentifier");
        addToMap(48 , "tinyint");
        addToMap(52 , "smallint");
        addToMap(56 , "int");
        addToMap(58 , "smalldatetime");
        addToMap(59 , "real");
        addToMap(60 , "money");
        addToMap(61 , "datetime");
        addToMap(62 , "float");
        addToMap(98 , "sql_variant");
        addToMap(99 , "ntext");
        addToMap(104 , "bit");
        addToMap(106 , "decimal");
        addToMap(108 , "numeric");
        addToMap(122 , "smallmoney");
        addToMap(127 , "bigint");
        addToMap(165 , "varbinary");
        addToMap(167 , "varchar");
        addToMap(173 , "binary");
        addToMap(175 , "char");
        addToMap(189 , "timestamp");
        addToMap(231 , "nvarchar");
        addToMap(239 , "nchar");
        addToMap(241 , "xml");
        addToMap(231 , "sysname");
    }
    
    // 为了好看一点
    private static void addToMap(Integer key , String value)
    {
        typenameMap.put(key , value);
    }
    
    // 对象计数器
    private static long count = 1;
    
    /**
     * 对应的数据库连接
     */
    private Connection conn = null;
    /**
     * 对应的语句
     */
    private Statement statement = null;
    /*
     * 对象的唯一id
     */
    private final long id;
    /**
     * tableList
     */
    private List<Table> tableList = new ArrayList<Table>();
    
    private SQLServerDatabase()
    {
        this.id = count;
        count ++;
        // 溢出
        if (count < 0)
            count = 1;
    }

    @Override
    public List<Table> tables()
    {
        return tableList;
    }
    
    @Override
    public void refreshTables() 
    {
        // 获得数据库中对应的表的信息
        this.tableList.clear();
        
        // 获得数据库中所有在username名下的表名称
        Set<String> tableNames = getTableNames();
        
        // 如果tablename获取失败
        if (tableNames == null)
            System.out.println("refresh fail...");
        else
        {
            // 对所有的表进行处理
            for (String tableName : tableNames)
            {
                Table table = Table.newTable(tableName);
                ResultSet result = null;
                // 进行table的初始化
                try
                {
                    String sql = String.format("select name , xtype , length from syscolumns where id = object_id('%s')" , tableName); 
                    result = statement.executeQuery(sql);
                    // 获得一列的信息
                    while (result.next())
                    {
                        String columnName = result.getString("name");
                        int xtype = result.getInt("xtype");
                        int length = result.getInt("length");
                        // 列名用大小，为了和其它的保持统一
                        table.addColumn(TableColumn.newColumn(columnName , typenameMap.get(xtype).toUpperCase() , length));
                    }
                    
                } catch (Exception e)
                {
                    // 清空所有的
                    tableList.clear();
                    System.out.println("refresh fail...");
                    
                } finally
                {
                    try
                    {
                        if (result != null)
                            result.close();
                    } catch (Exception e) { }
                }
                // 添加table
                tableList.add(table);
            }
        }
    }
    
    @Override
    public boolean createTable(Table table)
    {
        // 需要进行check
        return createTableImp(table , true);
    }
    
    @Override
    public boolean forceCreateTable(Table table)
    {   
        // 如果存在这个表，需要先进行删除
        if (tableIsExists(table))
        {
            String sql = String.format("drop table %s" , table.getTableName());
            try
            {
                statement.execute(sql);
            } catch (Exception e)
            {
                // 如果删除失败
                return false;
            }
        }
        
        // 这时候表是不存在的，因为已经删除了，所以不用check
        return createTableImp(table , false);
    }
    
    @Override
    public boolean batchCreateTable(Iterable<Table> tables , boolean isForce)
    {
        // 获得所有的表名
        Set<String> set = getTableNames();
        try
        {
            for (Table table : tables)
            {
                // 如果需要强制建表
                if (isForce)
                {
                    if (set.contains(table.getTableName()))
                    {
                        String sql = String.format("drop table %s" , table.getTableName());
                        this.statement.execute(sql);
                    }
                    if (!createTableImp(table , false))
                        return false;
                }
                else
                {
                    // 如果表不存在
                    if (!set.contains(table.getTableName()))
                    {
                        if (!createTableImp(table , false))
                            return false;
                    }
                }
            }
            
        } catch (SQLException e)
        {
            return false;
        }
        return true;
    }

    @Override
    public void disconnect() 
    {
        try 
        {
            if (statement != null)
                statement.close();
            if (conn != null)
                conn.close();
            // 清空数据库表的信息
            tableList.clear();
            
        } catch (SQLException e) { }
    }
    
    // 获得所有的表的名称
    private Set<String> getTableNames()
    {
        String sql = String.format("select name from sys.objects where type = 'U'");
        Set<String> ans = new HashSet<String>();
        ResultSet result = null;
        try
        {
            result = statement.executeQuery(sql);
            while (result.next())
                ans.add(result.getString(1).toUpperCase());
            
        } catch (Exception e)
        {
            ans = null;
        } finally
        {
            try
            {
                if (result != null)
                    result.close();
            } catch (Exception e) { }
        }
        
        return ans;
    }
    
    /**
     * 建立表的具体实现
     */
    private boolean createTableImp(Table table , boolean check)
    {
        // 如果需要检查而且表确实存在
        if (check && tableIsExists(table))
            return false;
        
        // 创建表
        boolean first = true;
        StringBuilder builder = new StringBuilder();
        builder.append(String.format("create table %s" , table.getTableName()));
        builder.append("(");
        for (TableColumn column : table.getColumnList())
        {
            // 获得一列对应的sql
            String str = DatabaseUtil.getSql(column , this.databaseType);
            if (first)
                first = false;
            else
                builder.append(", ");
            builder.append(str);
        }
        builder.append(")");
        
        // 建表
        try
        {
            statement.execute(builder.toString());
        } catch (Exception e) 
        {
            // for test
            System.out.println("create table " + table.getTableName() + "fail...  " + e.getMessage());
            System.out.println("the create sql is " + builder.toString());
            
            // 如果建表失败
            return false;
        }
        
        System.out.println("create table " + table.getTableName() + " success...");
        
        return true;
    }
    
    /**
     * 判断表名是否存在
     */
    private boolean tableIsExists(Table table)
    {
        // 在数据库中建表，需要判断表是否已经存在。如果已经存在就进行删除后再进行创建
        Set<String> tableNames = getTableNames();
        return tableNames.contains(table.getTableName());
    }
    
    /**
     * 初始化数据库
     */
    private void init(String username , String password , String databasename , String ip , int port) throws Exception
    {
        // 初始化数据库
        Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        this.conn = DriverManager.getConnection(String.format("jdbc:sqlserver://%s:%d;DatabaseName=%s" , ip , port , databasename) , username , password);
        this.statement = this.conn.createStatement();
        // 初始化基本信息
        this.databaseName = databasename;
        this.databaseType = DATABASE_TYPE.SQLSERVER;
        this.stringToShow = String.format("%s:%d %s(%s)" , ip , port , databasename , databaseType.toString());
        // 初始化databaseMeta
        this.databaseMeta = new DatabaseMeta(String.format("%s_%d" , this.databaseType.toString() , this.id) , this.databaseType.toString() , "jdbc" , ip , this.databaseName , Integer.toString(port) , username , password);
        // 更新表信息
        refreshTables();
    }
  
}