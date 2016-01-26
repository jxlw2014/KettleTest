package kettle;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;

import util.DatabaseUtil;
import util.KettleUtil;
import util.KettleUtil.SynchronizationSetting;
import database.Database;
import database.Table;
import env.Constants;

/**
 * ��ʱ����ͬ������һ�����ݿⶨʱͬ����һ�����ݿ⡣�ٶ��������ݿ�֮���Ѿ�ִ�й�import�Ĳ������������ݿ���һ�µı�ṹ
 */
public class TimingDataSynchronization implements DatabaseImporter , TimingImporter
{
    private TimingDataSynchronization() { }
    
    /**
     * ����µ�ͬ��ʵ��
     */
    public static TimingDataSynchronization newInstance()
    {
        return new TimingDataSynchronization();
    }
    
    // syn������batch��С
    private int batchSize = Constants.DEFAULT_BATCH_SIZE;
    
    // Դ��Ŀ�����ݿ�
    private Database source;
    private Database dest;
    
    // ͬ������������
    private SynchronizationSetting setting = SynchronizationSetting.DEFAULT;
    
    // ֧�ֶ�ʱ�������̳߳�
    private ScheduledExecutorService executor; 
    
    // �Ƿ�ֹͣ�ı�־
    private AtomicBoolean isShutdown = new AtomicBoolean(false);
    
    /**
     * ����ͬ���Ĳ���
     */
    // TODO ����ܹ��������DatabaseImporter
    public void setSetting(SynchronizationSetting setting)
    {
        this.setting = setting;
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

    /**
     * ��ʱִ��ͬ������
     */
    @Override
    public void timingExecute(long time , TimeUnit timeUnit)
    {
        // һ���߳̾͹��ˣ�ֻҪ����ʱ�����Ϳ�����
        executor = Executors.newScheduledThreadPool(1);
        // ��ʱִ��ͬ������
        executor.scheduleAtFixedRate(new Runnable()
        {
            public void run()
            {
                execute();
            }
        } , 0 , time , timeUnit);
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
                    Table destTable = DatabaseUtil.transformTable(source.databaseType() , dest.databaseType() , sourceTable);
                    
                    // ����û��destTable�Ŀ��ܣ���Ϊû��ִ��һ��copySchema��������Ҫ�ж�һ��
                    if (!dest.containsTable(destTable.getTableName()))
                    {
                        dest.createTable(destTable);
                        System.out.println(String.format("Create table %s due to the table is not exists in dest database..." , sourceTable.getTableName()));
                    }
                    KettleUtil.addSynchronizedComponent(transMeta , source , sourceTable , dest , destTable , setting , cnt);
                        
                    cur ++;
                    cnt ++;
                }
               
                // execute in batch
                trans = new Trans(transMeta);
                trans.prepareExecution(null);
                trans.startThreads();
                trans.waitUntilFinished();
                
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
        if (executor != null)
            executor.shutdown();
        isShutdown.set(true);
    }
    
}
