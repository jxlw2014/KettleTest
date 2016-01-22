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
     * mysql到oracle的数据类型映射表
     */
    public static Map<String , String> MYSQL_TO_ORACLE = new HashMap<String , String>();
    
    /**
     *  所有map的集合
     */
    public static Set<Map<String , String>> set = new HashSet<Map<String , String>>();
    
    static
    {
        // 设置set
        set.add(ORACLE_TO_MYSQL);
        set.add(MYSQL_TO_ORACLE);
    }
    
    /**
     * 运行环境的初始化
     */
    public static void init()
    {
        try
        {
            KettleEnvironment.init();                
            initDatabaseTypeMap();
            
        } catch (Exception e)
        {
            System.out.println("init fail...");
        }
    }
    
    /**
     * 初始化数据库类型映射表
     */
    private static void initDatabaseTypeMap()
    {
        File confDir = new File("src/conf/databasetype");
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
                    String oracleType = strs[0];
                    String mysqlType = strs[1];
                    // 更新映射表
                    updateTypeMap(source , dest , oracleType , mysqlType);
                    updateTypeMap(dest , source , mysqlType , oracleType);
                }
                
            } catch (FileNotFoundException e_file) 
            {
                for (Map<String , String> map : set)
                    map.clear();
                
            } catch (IOException e_io) 
            {
                for (Map<String , String> map : set)
                    map.clear();
              
            } finally
            {
                try
                {
                    if (br != null)
                        br.close();
                } catch (IOException e) { }
            }
        }
    }
    
    // 具体的更新映射表
    private static void updateTypeMap(DATABASE_TYPE source , DATABASE_TYPE dest , String sourceType , String destType)
    {
        // oracle
        if (source == DATABASE_TYPE.ORACLE)
        {
            if (dest == DATABASE_TYPE.MYSQL)
                ORACLE_TO_MYSQL.put(sourceType , destType);
        }
        // mysql
        else if (source == DATABASE_TYPE.MYSQL)
        {
            if (dest == DATABASE_TYPE.ORACLE)
                MYSQL_TO_ORACLE.put(sourceType , destType);
        }
    }
    
}



