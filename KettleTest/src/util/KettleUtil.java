package util;

import java.util.ArrayList;
import java.util.List;

import org.pentaho.di.trans.TransHopMeta;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.steps.mergerows.MergeRowsMeta;
import org.pentaho.di.trans.steps.synchronizeaftermerge.SynchronizeAfterMergeMeta;
import org.pentaho.di.trans.steps.tableinput.TableInputMeta;
import org.pentaho.di.trans.steps.tableoutput.TableOutputMeta;

import database.Database;
import database.Table;
import database.Table.TableColumn;

/**
 * kettle相关处理的工具类
 */
public final class KettleUtil 
{
    /**
     * 表对表导入工具的参数设定，用来设置一个库中所有表import时的参数
     */
    public static class TableImportSetting
    {
        // 是否工作在batch提交下
        private boolean inBatch = true;
        // 提交的记录数目
        private int commitSize = 1000;
        
        /**
         * 默认的同步参数设置
         */
        public static final TableImportSetting DEFAULT = new TableImportSetting().setInBatch(true).setCommitSize(1000);
        
        private TableImportSetting() { }
        
        /**
         * 获得一个新的setting对象
         */
        public static TableImportSetting newSetting()
        {
            return new TableImportSetting();
        }
        
        /**
         * 设置是否采用batch的方式插入
         */
        public TableImportSetting setInBatch(boolean inBatch)
        {
            this.inBatch = inBatch;
            return this;
        }
        
        /**
         * 设置提交的记录数目
         */
        public TableImportSetting setCommitSize(int commitSize)
        {
            this.commitSize = commitSize;
            return this;
        }
    }
    
    private KettleUtil() { }
    
    /**
     * 给转换添加一次添加所有的组件，可能会导致连接过多
     * @param transMeta 转换元数据
     * @param source    源数据库
     * @param dest      目标数据库
     */
    public static void addImportComponent(TransMeta transMeta , Database source , Database dest , TableImportSetting setting)
    {
        // 操作的标号
        int next = 1;
        // 对所有的表建立导入映射
        for (Table table : source.tables())
        {
            Table destTable = DatabaseUtil.transformTable(source.databaseType() , dest.databaseType(),  table);
            addImportComponent(transMeta , source , table , dest , destTable , setting , next);
            next ++;
        }
    }
    
    /**
     * 添加同步组件，从源表同步到目标表
     * @param transMeta kettle中一个转换对应的元数据
     * @param sourceDatabase 源数据库
     * @param sourceTable 源表
     * @param destDatabase 目的数据库
     * @param destTable 目的表
     * @param index 操作的序号，用来区分不同的导入组件
     */
    public static void addImportComponent(TransMeta transMeta , Database sourceDatabase , Table sourceTable , 
                                                                     Database destDatabase , Table destTable , TableImportSetting setting , int index)
    {
        // input table
        TableInputMeta inputMeta = new TableInputMeta();
        inputMeta.setDatabaseMeta(sourceDatabase.databaseMeta());
        inputMeta.setSQL(String.format("select * from %s" , sourceTable.getTableName()));
        
        StepMeta inputStepMeta = new StepMeta(String.format("input_%s" , sourceTable.getTableName()) , inputMeta);
        transMeta.addStep(inputStepMeta);
        
        // output table
        TableOutputMeta outputMeta = new TableOutputMeta();
        outputMeta.setDatabaseMeta(destDatabase.databaseMeta());
        outputMeta.setTableName(destTable.getTableName());
        // 采用setting进行导入的设置
        outputMeta.setUseBatchUpdate(setting.inBatch);
        outputMeta.setCommitSize(setting.commitSize);
        
        StepMeta outputStepMeta = new StepMeta(String.format("output_%s" , destTable.getTableName()) , outputMeta);
        transMeta.addStep(outputStepMeta);
        
        // the hop between them
        TransHopMeta hopMeta = new TransHopMeta(inputStepMeta , outputStepMeta);
        transMeta.addTransHop(hopMeta);
        
        System.out.println("add import component to table " + sourceTable.getTableName());
    }
    
    /**
     * 为数据库中的所有表添加转换组件，可能会导致连接过多
     * @param transMeta 转换元数据
     * @param source    源数据库
     * @param dest      目的数据库
     * @param setting   同步参数设置
     */
    public static void addSynchronizedComponent(TransMeta transMeta , Database source , Database dest , TableImportSetting setting)
    {
        // 对每个表进行同步组件的设置
        int next = 1;
        for (Table table : source.tables())
        {
            Table anotherTable = DatabaseUtil.transformTable(source.databaseType() , dest.databaseType() , table);
            addSynchronizedComponent(transMeta , source , table , dest , anotherTable , setting , next);
            next ++;
        }
    }
    
    /**
     * 添加同步组件
     * @param transMeta 转换元数据
     * @param source    源数据库
     * @param sourceTable 源表
     * @param dest      目的数据库
     * @param destTable 目的表
     * @param setting   同步参数设置
     * @param index     同步序号
     */
    public static void addSynchronizedComponent(TransMeta transMeta , Database source , Table sourceTable ,
                                                Database dest , Table destTable , TableImportSetting setting , int index)
    {
        /**
         * 四个后面需要使用的数组
         * names
         * emptyStrings
         * conditions
         * booleans
         */
        List<String> list = new ArrayList<String>();
        for (TableColumn column : sourceTable.getColumnList())
            list.add(column.columnName);
        // 关键字列一般就是第一列
        String keyfield = list.get(0);
            
        String[] names = new String[list.size()];
        for (int i = 0;i < list.size();i ++)
            names[i] = list.get(i);
        String[] emptyStrings = new String[list.size()];
        for (int i = 0;i < list.size();i ++)
            emptyStrings[i] = "";
        Boolean[] booleans = new Boolean[list.size()];
        for (int i = 0;i < list.size();i ++)
            booleans[i] = true;
        String[] conditions = new String[list.size()];
        for (int i = 0;i < list.size();i ++)
            conditions[i] = "=";
        
        // input tables
        TableInputMeta inputMeta1 = new TableInputMeta();
        inputMeta1.setDatabaseMeta(source.databaseMeta());
        inputMeta1.setSQL(String.format("select * from %s order by %s" , sourceTable.getTableName() , keyfield));
        
        StepMeta inputStepMeta1 = new StepMeta(String.format("input1_%s" , sourceTable.getTableName()) , inputMeta1);
        transMeta.addStep(inputStepMeta1);
        
        TableInputMeta inputMeta2 = new TableInputMeta();
        inputMeta2.setDatabaseMeta(dest.databaseMeta());
        inputMeta2.setSQL(String.format("select * from %s order by %s" , destTable.getTableName() , keyfield));
        
        StepMeta inputStepMeta2 = new StepMeta(String.format("input2_%s" , destTable.getTableName()) , inputMeta2);
        transMeta.addStep(inputStepMeta2);
        
        // merge
        MergeRowsMeta rowsMeta = new MergeRowsMeta();
        rowsMeta.setFlagField("flagfield");
        rowsMeta.setKeyFields(new String[] {keyfield});
        rowsMeta.setValueFields(names);
        
        // 设置旧的和新的
        rowsMeta.getStepIOMeta().setInfoSteps(new StepMeta[] {inputStepMeta2 , inputStepMeta1});
        
        StepMeta merge = new StepMeta(String.format("merge_%s" , sourceTable.getTableName()) , rowsMeta);
        transMeta.addStep(merge);
        
        transMeta.addTransHop(new TransHopMeta(inputStepMeta1 , merge));
        transMeta.addTransHop(new TransHopMeta(inputStepMeta2 , merge));
        
        // TODO 先去掉filter，以后再考虑加入
        /*
        // 判断filter是否添加成功
        boolean addFilterSuccess = true;
        // filter
        FilterRowsMeta filterMeta = new FilterRowsMeta();
        StepMeta filter = null;
        try
        {
            filterMeta.setCondition(new Condition("flagfield" , Condition.FUNC_NOT_EQUAL , null , new ValueMetaAndData("flag" , "identical")));
            filter = new StepMeta(String.format("filter_%s" , sourceTable.getTableName()) , filterMeta);
            transMeta.addStep(filter);
            transMeta.addTransHop(new TransHopMeta(merge , filter));
            // 添加成功
            addFilterSuccess = true;
            
        } catch (KettleValueException e)
        {
            addFilterSuccess = false;
            System.out.println("add filter component fail, due to the filter value is not legal...");
        }*/
        
        // Syn
        SynchronizeAfterMergeMeta synMeta = new SynchronizeAfterMergeMeta();
        synMeta.setDatabaseMeta(dest.databaseMeta());
        synMeta.setTableName(destTable.getTableName());
        
        synMeta.setKeyStream(new String[] {keyfield});
        synMeta.setKeyLookup(new String[] {keyfield});
        synMeta.setKeyCondition(new String[] {"="});
        synMeta.setKeyStream2(new String[] {""});
        
        synMeta.setUpdateLookup(names);
        synMeta.setUpdateStream(names);
        synMeta.setUpdate(booleans);
        
        synMeta.setUseBatchUpdate(setting.inBatch);
        synMeta.setCommitSize(setting.commitSize);
        
        synMeta.setOperationOrderField("flagfield");
        synMeta.setOrderDelete("deleted");
        synMeta.setOrderInsert("new");
        synMeta.setOrderUpdate("changed");
        
        StepMeta syn = new StepMeta(String.format("syn_%s" , sourceTable.getTableName()) , synMeta);
        transMeta.addStep(syn);
        
        /*
        // 如果filter没有加成功，直接交给syn
        if (!addFilterSuccess)
            transMeta.addTransHop(new TransHopMeta(merge , syn));
        else
            transMeta.addTransHop(new TransHopMeta(filter , syn));*/
        
        // 从merge到syn
        transMeta.addTransHop(new TransHopMeta(merge , syn));
        
        System.out.println("add syn component to table " + sourceTable.getTableName());
    }
    
}




