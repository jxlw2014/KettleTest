package kettle;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;

import util.DatabaseUtil;
import util.KettleUtil;
import util.KettleUtil.TableImportSetting;

import common.Pair;

import database.Database;
import database.Table;
import env.Constants;

/**
 * ��ʱ����ͬ������һ�����ݿⶨʱͬ����һ�����ݿ⡣�ٶ��������ݿ�֮���Ѿ�ִ�й�import�Ĳ������������ݿ���һ�µı�ṹ
 */
public class TimingDatabaseDataSynchronization implements DatabaseImporter , TimingImporter
{
    private TimingDatabaseDataSynchronization() { }
    
    /**
     * ����µ�ͬ��ʵ��
     */
    public static TimingDatabaseDataSynchronization newInstance()
    {
        return new TimingDatabaseDataSynchronization();
    }
    
    // syn������batch��С
    private int batchSize = Constants.DEFAULT_BATCH_SIZE;
    
    // Դ��Ŀ�����ݿ�
    private Database source;
    private Database dest;
    
    // ͬ������������
    private TableImportSetting setting = TableImportSetting.DEFAULT;
    
    // ֧�ֶ�ʱ�������̳߳�
    private ScheduledExecutorService schedule = null;
    private AtomicBoolean timingIsSet = new AtomicBoolean(false);
    
    // �Ƿ�ֹͣ�ı�־
    private AtomicBoolean isShutdown = new AtomicBoolean(false);
    
    // ���ñ���ص���Ϣ
    private AtomicInteger setFlag = new AtomicInteger(0);
    private Set<String> tables;
    
    @Override
    public void setSetting(TableImportSetting setting)
    {
        this.setting = setting;
    }

    @Override
    public void setIncludedTables(Iterable<String> tables) 
    {  
        if (this.setFlag.get() == 0)
        {
            this.setFlag.decrementAndGet();
            this.tables = new HashSet<String>();
            for (String str : tables)
                this.tables.add(str.toUpperCase());
        }
    }

    @Override
    public void setExcludedTables(Iterable<String> tables) 
    {
        if (this.setFlag.get() == 0)
        {
            this.setFlag.incrementAndGet();
            this.tables = new HashSet<String>();
            for (String str : tables)
                this.tables.add(str.toUpperCase());
        }
    }

    @Override
    public boolean build(Database source, Database dest) 
    {
        // ���ƽṹ��������У��п����Ѿ���һ���ı�ṹ�������Ͳ��ʺ�ɾ���ؽ���
        this.source = source;
        this.dest = dest;
        this.isShutdown.set(false);
        // ���Ƿ���true��ûʲô�����жϵ�
        return true;
    }
    
    @Override
    public Pair<Database , Database> getConnPair()
    {
        // ���û�гɹ�build
        if (this.source == null || this.dest == null)
            return null;
        else
            return Pair.newPair(this.source , this.dest);
    }

    /**
     * ��ʱִ��ͬ������
     */
    @Override
    public void timingExecute(long time , TimeUnit timeUnit)
    {
        // ���û������timing
        if (!this.timingIsSet.get())
        {
            this.timingIsSet.set(true);
            // һ���߳̾͹��ˣ�ֻҪ����ʱ�����Ϳ�����
            schedule = Executors.newSingleThreadScheduledExecutor();
            // ��ʱִ��ͬ������
            schedule.scheduleAtFixedRate(new Runnable()
            {
                public void run()
                {
                    execute();
                }
            } , 0 , time , timeUnit);
        }
    }
    
    @Override
    public boolean execute() 
    {
        try
        {
            System.out.println("start synchronizing...");
            
            long curTime = System.currentTimeMillis();
            
            // syn in batch
            List<Table> tableList = source.tables();
            int cur = 0 , n = tableList.size();
            while (cur < n && !isShutdown.get())
            {
                TransMeta transMeta = new TransMeta();
                Trans trans = null;
                
                int cnt = 1;
                while (cur < n && cnt <= batchSize)
                {
                    Table sourceTable = tableList.get(cur);
                    // ��������
                    if (check(sourceTable))
                    {
                        Table destTable = DatabaseUtil.transformTable(source.databaseType() , dest.databaseType() , sourceTable);
                        
                        // ����û��destTable�Ŀ��ܣ���Ϊû��ִ��һ��copySchema��������Ҫ�ж�һ��
                        if (!dest.containsTable(destTable.getTableName()))
                        {
                            dest.createTable(destTable);
                            System.out.println(String.format("Create table %s due to the table is not exists in dest database..." , sourceTable.getTableName()));
                        }
                        KettleUtil.addSynchronizedComponent(transMeta , source , sourceTable , dest , destTable , setting , cnt);
                        cnt ++;
                    }
                    cur ++;
                }
               
                // execute in batch
                trans = new Trans(transMeta);
                trans.prepareExecution(null);
                trans.startThreads();
                trans.waitUntilFinished();
                
                transMeta = null;
                trans = null;
                
                System.out.println(String.format("Syn %d table success..." , cur));
            }
            
            System.out.println(String.format("The synchronized time is %f" , (double) (System.currentTimeMillis() - curTime) / (double) 1000));                    
            System.out.println("synchronized success...");
            
        } catch (Exception e)
        {
            System.out.println("synchronized fail...");
            return false;
        }
       
        return true;
    }
    
    @Override
    public void setBatchSize(int batchSize)
    {
        this.batchSize = batchSize;
    }
    
    /**
     * ֹͣ����ͬ���߳�
     */
    public void shutdown()
    {
        isShutdown.set(true);
        this.timingIsSet.set(false);
        if (this.schedule != null)
            this.schedule.shutdown();
    }
    
    /**
     * �ж�һ�����ǲ���Ӧ�ý���ͬ��
     */
    private boolean check(Table table)
    {
        if (this.setFlag.get() == 0)
            return true;
        else
        {
            if (this.setFlag.get() < 0 && this.tables.contains(table.getTableName()))
                return true;
            else if (this.setFlag.get() > 0 && !this.tables.contains(table.getTableName()))
                return true;
            else
                return false;
        }
    }
    
}
