package main;

import kettle.DataImporter;
import database.Database;
import database.MysqlDatabase;
import database.OracleDatabase;
import env.Environment;


public class Main 
{
    public static void main(String[] args) throws Exception
    {

        /*
        TransMeta tranMeta = new TransMeta();
        
        // input tables
        TableInputMeta inputMeta1 = new TableInputMeta();
        inputMeta1.setDatabaseMeta(databaseMeta1);
        inputMeta1.setSQL("select * from test1");
        
        StepMeta inputStepMeta1 = new StepMeta("input1" , inputMeta1);
        tranMeta.addStep(inputStepMeta1);
        
        TableInputMeta inputMeta2 = new TableInputMeta();
        inputMeta2.setDatabaseMeta(databaseMeta2);
        inputMeta2.setSQL("select * from test2");
        
        StepMeta inputStepMeta2 = new StepMeta("input2" , inputMeta2);
        tranMeta.addStep(inputStepMeta2);
        
        // merge
        MergeRowsMeta rowsMeta = new MergeRowsMeta();
        rowsMeta.setFlagField("flagfield");
        rowsMeta.setKeyFields(new String[] {"name" , "id"});
        rowsMeta.setValueFields(new String[] {"name" , "id"});
        
        // 设置旧的和新的
        rowsMeta.getStepIOMeta().setInfoSteps(new StepMeta[] {inputStepMeta2 , inputStepMeta1});
        
        StepMeta merge = new StepMeta("merge" , rowsMeta);
        tranMeta.addStep(merge);
        
        tranMeta.addTransHop(new TransHopMeta(inputStepMeta1 , merge));
        tranMeta.addTransHop(new TransHopMeta(inputStepMeta2 , merge));
        
        // Syn
        SynchronizeAfterMergeMeta synMeta = new SynchronizeAfterMergeMeta();
        synMeta.setDatabaseMeta(databaseMeta2);
        synMeta.setTableName("test2");
        
        synMeta.setKeyStream(new String[] {"name" , "id"});
        synMeta.setKeyLookup(new String[] {"name" , "id"});
        synMeta.setKeyCondition(new String[] {"=" , "="});
        synMeta.setKeyStream2(new String[] {"" , ""});
        
        synMeta.setUpdateLookup(new String[] {"name" , "id"});
        synMeta.setUpdateStream(new String[] {"name" , "id"});
        synMeta.setUpdate(new Boolean[] {true , true});
        
        synMeta.setOperationOrderField("flagfield");
        synMeta.setOrderDelete("deleted");
        synMeta.setOrderInsert("new");
        synMeta.setOrderUpdate("changed");
        
        StepMeta syn = new StepMeta("syn" , synMeta);
        tranMeta.addStep(syn);
        
        tranMeta.addTransHop(new TransHopMeta(merge , syn));
            
        Trans tran = new Trans(tranMeta);
        tran.prepareExecution(null);
        tran.startThreads();
        tran.waitUntilFinished();
        */
        
        /**
         * 数据导入测试
         */
        Environment.init();
        
        Database databaseSource = OracleDatabase.Builder.newBuilder()
                                    .setDatabasename("orcl")
                                    .setIp("10.214.208.194")
                                    .setPassword("datarun")
                                    .setUsername("datarun")
                                    .setPort(1521).build();

        Database databaseDest = MysqlDatabase.Builder.newBuilder()
                                    .setDatabasename("import")
                                    .setIp("localhost")
                                    .setPassword("liuwei")
                                    .setUsername("root")
                                    .setPort(3306).build();
        
        DataImporter importer = DataImporter.newImporter();
        importer.build(databaseSource , databaseDest);
        importer.execute();
    }
    
}
















