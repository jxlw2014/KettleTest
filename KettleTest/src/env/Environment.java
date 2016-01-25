package env;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.pentaho.di.core.KettleEnvironment;

import database.Database.DATABASE_TYPE;

/**
 * 整个程序的全局环境类，保存一些通用的东西
 */
public class Environment
{
    /**
     * oracle到mysql的数据类型映射表
     */
    public static Map<String , String> ORACLE_TO_MYSQL = new HashMap<String , String>();
    
    /**
     * sqlserver到mysql的数据类型映射表
     */
    public static Map<String , String> SQLSERVER_TO_MYSQL = new HashMap<String , String>();

    /**
     * sqlserver到oracle的数据类型映射表
     */
    public static Map<String , String> SQLSERVER_TO_ORACLE = new HashMap<String , String>();
    
    /**
     *  所有map的集合
     */
    public static Set<Map<String , String>> set = new HashSet<Map<String , String>>();
    
    static
    {
        // 设置set
        set.add(ORACLE_TO_MYSQL);
        set.add(SQLSERVER_TO_MYSQL);
        set.add(SQLSERVER_TO_ORACLE);
    }
    
    /**
     * 运行环境的初始化
     */
    public static boolean init()
    {
        try
        {
            KettleEnvironment.init();                
            initDatabaseTypeMap();
            
        } catch (Exception e)
        {
            System.out.println("init fail...");
            System.out.println(e.getMessage());
            
            return false;
        }
        return true;
    }
    
    /**
     * 初始化数据库类型映射表
     */
    private static void initDatabaseTypeMap() throws Exception
    {
        File confDir = new File("src/databasetype");
        // 判断所有的配置文件，配置文件需要命名为oracle_xxx
        for (File file : confDir.listFiles())
        {
            BufferedReader br = null;
            try 
            {
                String filename = file.getName();
                String[] temp = filename.split("_");
                // 获得对应的数据库类型
                DATABASE_TYPE source = DATABASE_TYPE.of(temp[0]);
                DATABASE_TYPE dest = DATABASE_TYPE.of(temp[1]);
                // 读取文件
                br = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
                String line = null;
                while ((line = br.readLine()) != null)
                {
                    String[] strs = line.split(",");
                    String sourceType = strs[0];
                    String destType = strs[1];
                    // 更新映射表
                    updateTypeMap(source , dest , sourceType , destType);
                }
                // the map table created success
                System.out.println(String.format("The map table from %s to %s created success..." , source , dest));
                
            } catch (FileNotFoundException e_file) 
            {
                for (Map<String , String> map : set)
                    map.clear();
                throw new Exception(e_file.getMessage());
                    
            } catch (IOException e_io) 
            {
                for (Map<String , String> map : set)
                    map.clear();
                throw new Exception(e_io.getMessage());
                    
            } finally
            {
                try
                {
                    if (br != null)
                        br.close();
                } catch (IOException e) 
                {
                    // 关不了不影响映射表的获取，没必要使初始化失败
                }
            }
        }
    }
    
    // 具体的更新映射表
    private static void updateTypeMap(DATABASE_TYPE source , DATABASE_TYPE dest , String sourceType , String destType)
    {
        // oracle
        if (source == DATABASE_TYPE.ORACLE)
        {
            // oracle to mysql
            if (dest == DATABASE_TYPE.MYSQL)
                ORACLE_TO_MYSQL.put(sourceType , destType);
        }
        // sqlserver
        else if (source == DATABASE_TYPE.SQLSERVER)
        {
            // sqlserver to mysql
            if (dest == DATABASE_TYPE.MYSQL)
                SQLSERVER_TO_MYSQL.put(sourceType , destType);
            // sqlserver to oracle
            else if (dest == DATABASE_TYPE.ORACLE)
                SQLSERVER_TO_ORACLE.put(sourceType , destType);
        }
        // 其它的不考虑了
        else
        {
            return;
        }
    }
    
}



