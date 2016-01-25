package main;

import java.sql.Connection;
import java.sql.DriverManager;





public class Main 
{
    public static void main(String[] args)
    {
        /**
         * syn test
         */
        /*
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
        */
        
        /*
         * test the sqlserver 
         */
        try
        {
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
            Connection conn = DriverManager.getConnection("jdbc:sqlserver://10.214.224.27:1433;DatabaseName=cgysd" , "cgysd" , "cgysd"); 
            System.out.println("conn success...");
            
        } catch (Exception e)
        {
            e.printStackTrace();
        }
        
    }
    
}

















