package env;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.pentaho.di.core.KettleEnvironment;

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
    
    // 初始化映射表
    private static void initDatabaseTypeMap()
    {
        // 实现数据类型的映射表的读取
        File file = new File("src/conf/oracle_mysql");
        if (file.exists())
        {
            BufferedReader br = null;
            try 
            {
                br = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
                String line = null;
                while ((line = br.readLine()) != null)
                {
                    String[] strs = line.split(",");
                    String oracleType = strs[0];
                    String mysqlType = strs[1];
                    // 更新映射表
                    ORACLE_TO_MYSQL.put(oracleType , mysqlType);
                    MYSQL_TO_ORACLE.put(mysqlType , oracleType);
                }
                
            } catch (FileNotFoundException e_file) 
            {
                ORACLE_TO_MYSQL.clear();
                MYSQL_TO_ORACLE.clear();
                
            } catch (IOException e_io) 
            {
                ORACLE_TO_MYSQL.clear();
                MYSQL_TO_ORACLE.clear();
              
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
    
}



