package main;

import kettle.DataImporter;
import database.Database;
import database.MysqlDatabase;
import database.SQLServerDatabase;
import env.Constants;
import env.Environment;



public class Main 
{
    public static void main(String[] args)
    {
        /**
         * syn test
         */
        Environment.init();
        
        Database sourceDatabase = SQLServerDatabase.Builder.newBuilder()
                                    .setDatabasename("cgysd")
                                    .setIp("10.214.224.27")
                                    .setPassword("123456")
                                    .setUsername("sa")
                                    .setPort(Constants.DEFAULT_SQLSERVER_PORT).build();

        Database destDatabase = MysqlDatabase.Builder.newBuilder()
                                    .setDatabasename("import")
                                    .setIp("localhost")
                                    .setPassword("liuwei")
                                    .setUsername("root")
                                    .setPort(Constants.DEFAULT_MYSQL_PORT).build();
        
        DataImporter importer = DataImporter.newImporter();
        importer.build(sourceDatabase , destDatabase);
        importer.execute();
    }
    
}

















