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
 * SQLServer�Ķ�Ӧʵ�֣�SQLSERVER�汾Ϊ2008r2
 */
public class SQLServerDatabase extends AbstractDatabase 
{
    /**
     * Mysql���ݿ��Ӧ��Builder��
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
         * �õ�һ���µ�builderʵ��
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
         * �������ݿ�����������ʧ�ܷ���null
         */
        @Override
        public synchronized SQLServerDatabase build() 
        {
            // �Ƚ���Ԥ���ж�
            if (previousCheck())
            {
                SQLServerDatabase instance = new SQLServerDatabase();
                try
                {
                    instance.init(username , password , databasename , ip , port);
                } catch (Exception e)
                {
                    System.out.println("���ݿ��ʼ��ʧ��," + e.getMessage());
                    instance = null;
                }
                // ���ؽ��
                return instance;
            }
            else
                return null;
        }
        
        // Ԥ�ȵ��ж�
        private boolean previousCheck()
        {
            if (this.username == null)
            {
                System.out.println("usernameû������");
                return false;
            }
            else if (this.password == null)
            {
                System.out.println("passwordû������");
                return false;
            }
            else if (this.databasename == null)
            {
                System.out.println("databasenameû������");
                return false;
            }
            else if (this.ip == null)
            {
                System.out.println("���ݿ������ipû������");
                return false;
            }
            else if (this.port < 0)
            {
                System.out.println("���ݿ�������˿�û������");
                return false;
            }
            else
                return true;
        }
    }
    
    /**
     * ����е����ֵ����Ƶ�ӳ��
     */
    private static final Map<Integer , String> typenameMap = new HashMap<Integer , String>();
    
    static
    {
        // ��ʼ��typenameMap��Ӳ���뼴�ɣ���Ϊ��������ǲ����
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
    
    // Ϊ�˺ÿ�һ��
    private static void addToMap(Integer key , String value)
    {
        typenameMap.put(key , value);
    }
    
    // ���������
    private static long count = 1;
    
    /**
     * ��Ӧ�����ݿ�����
     */
    private Connection conn = null;
    /**
     * ��Ӧ�����
     */
    private Statement statement = null;
    /*
     * �����Ψһid
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
        // ���
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
        // ������ݿ��ж�Ӧ�ı����Ϣ
        this.tableList.clear();
        
        // ������ݿ���������username���µı�����
        Set<String> tableNames = getTableNames();
        
        // ���tablename��ȡʧ��
        if (tableNames == null)
            System.out.println("refresh fail...");
        else
        {
            // �����еı���д���
            for (String tableName : tableNames)
            {
                Table table = Table.newTable(tableName);
                ResultSet result = null;
                // ����table�ĳ�ʼ��
                try
                {
                    String sql = String.format("select name , xtype , length from syscolumns where id = object_id('%s')" , tableName); 
                    result = statement.executeQuery(sql);
                    // ���һ�е���Ϣ
                    while (result.next())
                    {
                        String columnName = result.getString("name");
                        int xtype = result.getInt("xtype");
                        int length = result.getInt("length");
                        // �����ô�С��Ϊ�˺������ı���ͳһ
                        table.addColumn(TableColumn.newColumn(columnName , typenameMap.get(xtype).toUpperCase() , length));
                    }
                    
                } catch (Exception e)
                {
                    // ������е�
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
                // ���table
                tableList.add(table);
            }
        }
    }
    
    @Override
    public boolean createTable(Table table)
    {
        // ��Ҫ����check
        return createTableImp(table , true);
    }
    
    @Override
    public boolean forceCreateTable(Table table)
    {   
        // ��������������Ҫ�Ƚ���ɾ��
        if (tableIsExists(table))
        {
            String sql = String.format("drop table %s" , table.getTableName());
            try
            {
                statement.execute(sql);
            } catch (Exception e)
            {
                // ���ɾ��ʧ��
                return false;
            }
        }
        
        // ��ʱ����ǲ����ڵģ���Ϊ�Ѿ�ɾ���ˣ����Բ���check
        return createTableImp(table , false);
    }
    
    @Override
    public boolean batchCreateTable(Iterable<Table> tables , boolean isForce)
    {
        // ������еı���
        Set<String> set = getTableNames();
        try
        {
            for (Table table : tables)
            {
                // �����Ҫǿ�ƽ���
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
                    // ���������
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
            // ������ݿ�����Ϣ
            tableList.clear();
            
        } catch (SQLException e) { }
    }
    
    // ������еı������
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
     * ������ľ���ʵ��
     */
    private boolean createTableImp(Table table , boolean check)
    {
        // �����Ҫ�����ұ�ȷʵ����
        if (check && tableIsExists(table))
            return false;
        
        // ������
        boolean first = true;
        StringBuilder builder = new StringBuilder();
        builder.append(String.format("create table %s" , table.getTableName()));
        builder.append("(");
        for (TableColumn column : table.getColumnList())
        {
            // ���һ�ж�Ӧ��sql
            String str = DatabaseUtil.getSql(column , this.databaseType);
            if (first)
                first = false;
            else
                builder.append(", ");
            builder.append(str);
        }
        builder.append(")");
        
        // ����
        try
        {
            statement.execute(builder.toString());
        } catch (Exception e) 
        {
            // for test
            System.out.println("create table " + table.getTableName() + "fail...  " + e.getMessage());
            System.out.println("the create sql is " + builder.toString());
            
            // �������ʧ��
            return false;
        }
        
        System.out.println("create table " + table.getTableName() + " success...");
        
        return true;
    }
    
    /**
     * �жϱ����Ƿ����
     */
    private boolean tableIsExists(Table table)
    {
        // �����ݿ��н�����Ҫ�жϱ��Ƿ��Ѿ����ڡ�����Ѿ����ھͽ���ɾ�����ٽ��д���
        Set<String> tableNames = getTableNames();
        return tableNames.contains(table.getTableName());
    }
    
    /**
     * ��ʼ�����ݿ�
     */
    private void init(String username , String password , String databasename , String ip , int port) throws Exception
    {
        // ��ʼ�����ݿ�
        Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        this.conn = DriverManager.getConnection(String.format("jdbc:sqlserver://%s:%d;DatabaseName=%s" , ip , port , databasename) , username , password);
        this.statement = this.conn.createStatement();
        // ��ʼ��������Ϣ
        this.databaseName = databasename;
        this.databaseType = DATABASE_TYPE.SQLSERVER;
        this.stringToShow = String.format("%s:%d %s(%s)" , ip , port , databasename , databaseType.toString());
        // ��ʼ��databaseMeta
        this.databaseMeta = new DatabaseMeta(String.format("%s_%d" , this.databaseType.toString() , this.id) , this.databaseType.toString() , "jdbc" , ip , this.databaseName , Integer.toString(port) , username , password);
        // ���±���Ϣ
        refreshTables();
    }
  
}