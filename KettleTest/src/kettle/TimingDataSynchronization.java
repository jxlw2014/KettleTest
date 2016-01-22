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
 * 定时数据同步，从一个数据库定时同步另一个数据库
 */
public class TimingDataSynchronization implements EntireImporter
{
    private TimingDataSynchronization() { }
    
    /**
     * 获得新的同步实例
     */
    public TimingDataSynchronization newInstance()
    {
        return new TimingDataSynchronization();
    }
    
    // 时间间隔，默认为一天
    private long time = 1;
    private TimeUnit timeUnit = TimeUnit.DAYS;
    
    // 转换相关
    private TransMeta transMeta = new TransMeta();
    private Trans trans;
    
    // 支持定时操作的线程池
    private ScheduledExecutorService executor; 
    
    /**
     * 设置同步时间间隔
     * @param time      时间间隔
     * @param timeUnit  时间单位
     */
    public void setInterval(long time , TimeUnit timeUnit)
    {
        this.time = time;
        this.timeUnit = timeUnit;
    }

    @Override
    public void build(Database source, Database dest) 
    {
        // 获得转换元数据
        KettleUtil.addSynchronizedComponent(transMeta , source , dest , SynchronizationSetting.DEFAULT);
        // 获得转换
        trans = new Trans(transMeta);
    }

    @Override
    public void execute() 
    {
        executor = Executors.newScheduledThreadPool(4);
        // 定时执行同步操作
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
                    // 清除转换信息
                    trans.cleanup();
                }
            }
        } , 0 , time , timeUnit);
    }
    
    /**
     * 停止数据同步线程
     */
    public void shutdown()
    {
        if (executor != null)
            executor.shutdown();
    }
    
}
