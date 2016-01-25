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
 * ���������ȫ�ֻ����࣬����һЩͨ�õĶ���
 */
public class Environment
{
    /**
     * oracle��mysql����������ӳ���
     */
    public static Map<String , String> ORACLE_TO_MYSQL = new HashMap<String , String>();
    
    /**
     * sqlserver��mysql����������ӳ���
     */
    public static Map<String , String> SQLSERVER_TO_MYSQL = new HashMap<String , String>();

    /**
     * sqlserver��oracle����������ӳ���
     */
    public static Map<String , String> SQLSERVER_TO_ORACLE = new HashMap<String , String>();
    
    /**
     *  ����map�ļ���
     */
    public static Set<Map<String , String>> set = new HashSet<Map<String , String>>();
    
    static
    {
        // ����set
        set.add(ORACLE_TO_MYSQL);
        set.add(SQLSERVER_TO_MYSQL);
        set.add(SQLSERVER_TO_ORACLE);
    }
    
    /**
     * ���л����ĳ�ʼ��
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
     * ��ʼ�����ݿ�����ӳ���
     */
    private static void initDatabaseTypeMap() throws Exception
    {
        File confDir = new File("src/databasetype");
        // �ж����е������ļ��������ļ���Ҫ����Ϊoracle_xxx
        for (File file : confDir.listFiles())
        {
            BufferedReader br = null;
            try 
            {
                String filename = file.getName();
                String[] temp = filename.split("_");
                // ��ö�Ӧ�����ݿ�����
                DATABASE_TYPE source = DATABASE_TYPE.of(temp[0]);
                DATABASE_TYPE dest = DATABASE_TYPE.of(temp[1]);
                // ��ȡ�ļ�
                br = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
                String line = null;
                while ((line = br.readLine()) != null)
                {
                    String[] strs = line.split(",");
                    String sourceType = strs[0];
                    String destType = strs[1];
                    // ����ӳ���
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
                    // �ز��˲�Ӱ��ӳ���Ļ�ȡ��û��Ҫʹ��ʼ��ʧ��
                }
            }
        }
    }
    
    // ����ĸ���ӳ���
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
        // �����Ĳ�������
        else
        {
            return;
        }
    }
    
}



