package main;

import java.util.List;
import java.util.concurrent.Future;

import kettle.DatabaseImporterManager.ImportResult;
import kettle.ImporterManager;
import util.KettleUtil.DatabaseImporterSetting;
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
        ImporterManager manager = ImporterManager.newDataImportManager(DatabaseImporterSetting.DEFAULT);

        manager.buildByConnPairs(Pairs.toPairs(sourceDatabase1 , destDatabase1 , sourceDatabase2 , destDatabase2));
        
        List<Future<ImportResult>> results = manager.executeAsync();
        for (Future<ImportResult> result : results)
        {
            try
            {
                System.out.println(result.get());
            } catch (Exception e) { }
        }
        
        // ��������ˣ��϶����Խ�����
        manager.shutdown();
    }
    
}

















