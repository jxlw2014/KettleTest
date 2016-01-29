package main;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;

import kettle.ImportResult;
import kettle.ImporterManager;
import util.KettleUtil.TableImportSetting;
import util.Pairs;
import database.Database;
import database.MysqlDatabase;
import env.Constants;
import env.Environment;



public class Main 
{
    public static void main(String[] args)
    {
        /**
         * ͬʱ���ж���⵼��Ĳ���
         */
        Environment.init();

        // mysql source1
        Database sourceDatabase1 = MysqlDatabase.Builder.newBuilder()
                                    .setDatabasename("import_sqlserver")
                                    .setIp("localhost")
                                    .setPassword("liuwei")
                                    .setUsername("root")
                                    .setPort(Constants.DEFAULT_MYSQL_PORT).build();

        // mysql source2
        Database sourceDatabase2 = MysqlDatabase.Builder.newBuilder()
                                    .setDatabasename("import_oracle")
                                    .setIp("localhost")
                                    .setUsername("root")
                                    .setPassword("liuwei")
                                    .setPort(Constants.DEFAULT_MYSQL_PORT).build();
        
        // mysql dest1
        Database destDatabase1 = MysqlDatabase.Builder.newBuilder()
                                    .setDatabasename("import1")
                                    .setIp("localhost")
                                    .setPassword("liuwei")
                                    .setUsername("root")
                                    .setPort(Constants.DEFAULT_MYSQL_PORT).build();
        
        // mysql dest2
        Database destDatabase2 = MysqlDatabase.Builder.newBuilder()
                                    .setDatabasename("import2")
                                    .setIp("localhost")
                                    .setPassword("liuwei")
                                    .setUsername("root")
                                    .setPort(Constants.DEFAULT_MYSQL_PORT).build();
        
        // ���Զ��ͬ������ͬʱ����
        ImporterManager manager = ImporterManager.newDataImportManager(TableImportSetting.DEFAULT);

        manager.buildByConnPairs(Pairs.toPairs(sourceDatabase1 , destDatabase1 , sourceDatabase2 , destDatabase2));
        
        List<Future<ImportResult>> futures = manager.executeAsync();
        List<ImportResult> results = new ArrayList<ImportResult>();
        for (Future<ImportResult> future : futures)
        {
            try
            {
                results.add(future.get());
            } catch (Exception e) { }
        }
        for (ImportResult result : results)
            System.out.println(result);
        
        // ��������ˣ��϶����Խ�����
        manager.shutdown();
    }
    
}

















