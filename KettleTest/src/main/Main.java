package main;

import java.util.List;
import java.util.concurrent.Future;

import kettle.DatabaseImporterManager.ImportResult;
import kettle.ImporterManager;
import util.KettleUtil.ImportSetting;
import util.Pairs;
import database.Database;
import database.MysqlDatabase;
import database.OracleDatabase;
import database.SQLServerDatabase;
import env.Constants;
import env.Environment;



public class Main 
{
    public static void main(String[] args)
    {
        /**
         * 同时进行多个库导入的测试
         */
        Environment.init();

        // sqlserver
        Database sourceDatabase1 = SQLServerDatabase.Builder.newBuilder()
                                    .setDatabasename("cgysd")
                                    .setIp("10.214.224.27")
                                    .setPassword("123456")
                                    .setUsername("sa")
                                    .setPort(Constants.DEFAULT_SQLSERVER_PORT).build();

        // oracle
        Database sourceDatabase2 = OracleDatabase.Builder.newBuilder()
                                    .setDatabasename("orcl")
                                    .setIp("10.214.208.194")
                                    .setUsername("datarun")
                                    .setPassword("datarun")
                                    .setPort(Constants.DEFAULT_ORACLE_PORT).build();
        
        // mysql
        Database destDatabase1 = MysqlDatabase.Builder.newBuilder()
                                    .setDatabasename("import_sqlserver")
                                    .setIp("localhost")
                                    .setPassword("liuwei")
                                    .setUsername("root")
                                    .setPort(Constants.DEFAULT_MYSQL_PORT).build();
        
        // mysql
        Database destDatabase2 = MysqlDatabase.Builder.newBuilder()
                                    .setDatabasename("import_oracle")
                                    .setIp("localhost")
                                    .setPassword("liuwei")
                                    .setUsername("root")
                                    .setPort(Constants.DEFAULT_MYSQL_PORT).build();
        
        // 测试多个导入操作同时进行
        ImporterManager manager = ImporterManager.newDataImporterMananger(ImportSetting.DEFAULT);

        manager.build(Pairs.toPairs(sourceDatabase1 , destDatabase1 , sourceDatabase2 , destDatabase2));
        
        List<Future<ImportResult>> results = manager.executeAsync();
        for (Future<ImportResult> result : results)
        {
            try
            {
                System.out.println(result.get());
            } catch (Exception e) { }
        }
    }
    
}

















