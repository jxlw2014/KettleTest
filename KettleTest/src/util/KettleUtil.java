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
 * kettle��ش���Ĺ�����
 */
public final class KettleUtil 
{
    /**
     * ��Ա��빤�ߵĲ����趨����������һ���������б�importʱ�Ĳ���
     */
    public static class TableImportSetting
    {
        // �Ƿ�����batch�ύ��
        private boolean inBatch = true;
        // �ύ�ļ�¼��Ŀ
        private int commitSize = 1000;
        
        /**
         * Ĭ�ϵ�ͬ����������
         */
        public static final TableImportSetting DEFAULT = new TableImportSetting().setInBatch(true).setCommitSize(1000);
        
        private TableImportSetting() { }
        
        /**
         * ���һ���µ�setting����
         */
        public static TableImportSetting newSetting()
        {
            return new TableImportSetting();
        }
        
        /**
         * �����Ƿ����batch�ķ�ʽ����
         */
        public TableImportSetting setInBatch(boolean inBatch)
        {
            this.inBatch = inBatch;
            return this;
        }
        
        /**
         * �����ύ�ļ�¼��Ŀ
         */
        public TableImportSetting setCommitSize(int commitSize)
        {
            this.commitSize = commitSize;
            return this;
        }
    }
    
    private KettleUtil() { }
    
    /**
     * ��ת�����һ��������е���������ܻᵼ�����ӹ���
     * @param transMeta ת��Ԫ����
     * @param source    Դ���ݿ�
     * @param dest      Ŀ�����ݿ�
     */
    public static void addImportComponent(TransMeta transMeta , Database source , Database dest , TableImportSetting setting)
    {
        // �����ı��
        int next = 1;
        // �����еı�������ӳ��
        for (Table table : source.tables())
        {
            Table destTable = DatabaseUtil.transformTable(source.databaseType() , dest.databaseType(),  table);
            addImportComponent(transMeta , source , table , dest , destTable , setting , next);
            next ++;
        }
    }
    
    /**
     * ���ͬ���������Դ��ͬ����Ŀ���
     * @param transMeta kettle��һ��ת����Ӧ��Ԫ����
     * @param sourceDatabase Դ���ݿ�
     * @param sourceTable Դ��
     * @param destDatabase Ŀ�����ݿ�
     * @param destTable Ŀ�ı�
     * @param index ��������ţ��������ֲ�ͬ�ĵ������
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
        // ����setting���е��������
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
     * Ϊ���ݿ��е����б����ת����������ܻᵼ�����ӹ���
     * @param transMeta ת��Ԫ����
     * @param source    Դ���ݿ�
     * @param dest      Ŀ�����ݿ�
     * @param setting   ͬ����������
     */
    public static void addSynchronizedComponent(TransMeta transMeta , Database source , Database dest , TableImportSetting setting)
    {
        // ��ÿ�������ͬ�����������
        int next = 1;
        for (Table table : source.tables())
        {
            Table anotherTable = DatabaseUtil.transformTable(source.databaseType() , dest.databaseType() , table);
            addSynchronizedComponent(transMeta , source , table , dest , anotherTable , setting , next);
            next ++;
        }
    }
    
    /**
     * ���ͬ�����
     * @param transMeta ת��Ԫ����
     * @param source    Դ���ݿ�
     * @param sourceTable Դ��
     * @param dest      Ŀ�����ݿ�
     * @param destTable Ŀ�ı�
     * @param setting   ͬ����������
     * @param index     ͬ�����
     */
    public static void addSynchronizedComponent(TransMeta transMeta , Database source , Table sourceTable ,
                                                Database dest , Table destTable , TableImportSetting setting , int index)
    {
        /**
         * �ĸ�������Ҫʹ�õ�����
         * names
         * emptyStrings
         * conditions
         * booleans
         */
        List<String> list = new ArrayList<String>();
        for (TableColumn column : sourceTable.getColumnList())
            list.add(column.columnName);
        // �ؼ�����һ����ǵ�һ��
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
        
        // ���þɵĺ��µ�
        rowsMeta.getStepIOMeta().setInfoSteps(new StepMeta[] {inputStepMeta2 , inputStepMeta1});
        
        StepMeta merge = new StepMeta(String.format("merge_%s" , sourceTable.getTableName()) , rowsMeta);
        transMeta.addStep(merge);
        
        transMeta.addTransHop(new TransHopMeta(inputStepMeta1 , merge));
        transMeta.addTransHop(new TransHopMeta(inputStepMeta2 , merge));
        
        // TODO ��ȥ��filter���Ժ��ٿ��Ǽ���
        /*
        // �ж�filter�Ƿ���ӳɹ�
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
            // ��ӳɹ�
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
        // ���filterû�мӳɹ���ֱ�ӽ���syn
        if (!addFilterSuccess)
            transMeta.addTransHop(new TransHopMeta(merge , syn));
        else
            transMeta.addTransHop(new TransHopMeta(filter , syn));*/
        
        // ��merge��syn
        transMeta.addTransHop(new TransHopMeta(merge , syn));
        
        System.out.println("add syn component to table " + sourceTable.getTableName());
    }
    
}




