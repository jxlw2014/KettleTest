package kettle;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;

import util.DatabaseUtil;
import util.KettleUtil;
import util.KettleUtil.SynchronizationSetting;
import database.Database;
import database.Table;

/**
 * ��ʱ����ͬ������һ�����ݿⶨʱͬ����һ�����ݿ⡣�ٶ��������ݿ�֮���Ѿ�ִ�й�import�Ĳ������������ݿ���һ�µı�ṹ
 */
public class TimingDataSynchronization implements EntireImporter
{
    private TimingDataSynchronization() { }
    
    /**
     * ����µ�ͬ��ʵ��
     */
    public TimingDataSynchronization newInstance()
    {
        return new TimingDataSynchronization();
    }
    
    // ʱ������Ĭ��Ϊһ��
    private long time = 1;
    private TimeUnit timeUnit = TimeUnit.DAYS;
    
    // syn������batch��С
    private int batchSize = 20;
    
    // Դ��Ŀ�����ݿ�
    private Database source , dest;
    
    // ֧�ֶ�ʱ�������̳߳�
    private ScheduledExecutorService executor; 
    
    /**
     * ����ͬ��ʱ����
     * @param time      ʱ����
     * @param timeUnit  ʱ�䵥λ
     */
    public void setInterval(long time , TimeUnit timeUnit)
    {
        this.time = time;
        this.timeUnit = timeUnit;
    }

    @Override
    public void build(Database source, Database dest) 
    {
        // �������Դ���ݿ��еı���Ŀ�����ݿ����Ѿ��ж�Ӧ�ˣ����Բ����и��ƽṹ�Ĳ���
        this.source = source;
        this.dest = dest;
    }

    @Override
    public void execute() 
    {
        // һ���߳̾͹��ˣ�ֻҪ����ʱ�����Ϳ�����
        executor = Executors.newScheduledThreadPool(1);
        // ��ʱִ��ͬ������
        executor.scheduleAtFixedRate(new Runnable()
        {
            public void run()
            {
                try
                {
                    long curTime = System.currentTimeMillis();
                    
                    // syn in batch
                    List<Table> tableList = source.tables();
                    int cur = 0 , n = tableList.size();
                    while (cur < n)
                    {
                        TransMeta transMeta = new TransMeta();
                        Trans trans = null;
                        
                        int cnt = 1;
                        while (cur < n && cnt <= batchSize)
                        {
                            Table sourceTable = tableList.get(cur);
                            Table destTable = DatabaseUtil.transformTable(source.databaseType() , dest.databaseType() , sourceTable);
                            KettleUtil.addSynchronizedComponent(transMeta , source , sourceTable , dest , destTable , SynchronizationSetting.DEFAULT , cnt);
                            cur ++;
                            cnt ++;
                        }
                       
                        // execute in batch
                        trans = new Trans(transMeta);
                        trans.prepareExecution(null);
                        trans.startThreads();
                        trans.waitUntilFinished();
                    }
                    
                    System.out.println(String.format("The synchronized time is %f" , (double) (System.currentTimeMillis() - curTime) / (double) 1000));                    
                    System.out.println("synchronized success...");
                    
                } catch (Exception e)
                {
                    System.out.println("synchronized fail...");
                }
            }
        } , 0 , time , timeUnit);
    }
    
    /**
     * ֹͣ����ͬ���߳�
     */
    public void shutdown()
    {
        if (executor != null)
            executor.shutdown();
    }
    
}
