package kettle;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;

import util.KettleUtil;
import util.KettleUtil.SynchronizationSetting;
import database.Database;

/**
 * ��ʱ����ͬ������һ�����ݿⶨʱͬ����һ�����ݿ�
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
    
    // ת�����
    private TransMeta transMeta = new TransMeta();
    private Trans trans;
    
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
        // ���ת��Ԫ����
        KettleUtil.addSynchronizedComponent(transMeta , source , dest , SynchronizationSetting.DEFAULT);
        // ���ת��
        trans = new Trans(transMeta);
    }

    @Override
    public void execute() 
    {
        executor = Executors.newScheduledThreadPool(4);
        // ��ʱִ��ͬ������
        executor.scheduleAtFixedRate(new Runnable()
        {
            public void run()
            {
                try
                {
                    // TODO syn in batch action
                    long curTime = System.currentTimeMillis();
                    
                    trans.prepareExecution(null);
                    trans.startThreads();
                    trans.waitUntilFinished();
                    
                    System.out.println(String.format("The synchronized time is %f" , (double) (System.currentTimeMillis() - curTime) / (double) 1000));                    
                    System.out.println("synchronized success...");
                    
                } catch (Exception e)
                {
                    System.out.println("synchronized fail...");
                } finally
                {
                    // ���ת����Ϣ
                    trans.cleanup();
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
