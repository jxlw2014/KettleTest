package main;

import kettle.TimingDataSynchronization;
import database.Database;
import database.MysqlDatabase;
import database.OracleDatabase;
import env.Constants;
import env.Environment;



public class Main 
{
    public static void main(String[] args)
    {
        /**
         * 数据导入测试
         */
        Environment.init();
        
        Database sourceDatabase = OracleDatabase.Builder.newBuilder()
                                    .setDatabasename("orcl")
                                    .setIp("10.214.208.194")
                                    .setPassword("datarun")
                                    .setUsername("datarun")
                                    .setPort(Constants.DEFAULT_ORACLE_PORT).build();

        Database destDatabase = MysqlDatabase.Builder.newBuilder()
                                    .setDatabasename("import")
                                    .setIp("localhost")
                                    .setPassword("liuwei")
                                    .setUsername("root")
                                    .setPort(Constants.DEFAULT_MYSQL_PORT).build();
        
        TimingDataSynchronization syn = TimingDataSynchronization.newInstance();
        syn.build(sourceDatabase , destDatabase);
        syn.execute();
        
//        Table sourceTable = null , destTable = null;
//        for (Table table : sourceDatabase.tables())
//        {
//            if (table.getTableName().equals("EHV_OMDS_MEAS_ICE"))
//            {
//                sourceTable = table;
//                destTable = DatabaseUtil.transformTable(sourceDatabase.databaseType() , 
//                                                        destDatabase.databaseType() , sourceTable);
//                break;
//            }
//        }
//        
//        TransMeta transMeta = new TransMeta();
//        KettleUtil.addSynchronizedComponent(transMeta , sourceDatabase , sourceTable , destDatabase , destTable , SynchronizationSetting.DEFAULT , 1);
//        destDatabase.createTable(destTable);
//        Trans trans = new Trans(transMeta);
//        
//        try
//        {
//            trans.prepareExecution(null);
//            trans.startThreads();
//            trans.waitUntilFinished();
//            
//        } catch (Exception e) { }
        
        /**
         * TODO test the sql server
         */
//        try
//        {
//            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
//            Connection conn = DriverManager.getConnection("jdbc:sqlserver://10.214.224.27:1433;database=cgysd" , "cgysd" , "cgysd"); 
//            System.out.println("conn success...");
//            
//        } catch (Exception e)
//        {
//            e.printStackTrace();
//        }
    }
    
}

















