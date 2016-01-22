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
 * 定时数据同步，从一个数据库定时同步另一个数据库。假定两个数据库之间已经执行过import的操作，两个数据库有一致的表结构
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
    
    // syn工作的batch大小
    private int batchSize = 5;
    
    // 源、目的数据库
    private Database source;
    private Database dest;
    
    // 同步参数的设置
    private SynchronizationSetting setting = SynchronizationSetting.DEFAULT;
    
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
    
    /**
     * 设置同步的参数
     */
    public void setSetting(SynchronizationSetting setting)
    {
        this.setting = setting;
    }

    @Override
    public void build(Database source, Database dest) 
    {
        // 复制结构看情况进行
        this.source = source;
        this.dest = dest;
    }

    @Override
    public void execute() 
    {
        // 一个线程就够了，只要管理定时操作就可以了
        executor = Executors.newScheduledThreadPool(1);
        // 定时执行同步操作
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
                            // 存在没有destTable的可能，因为没有执行一遍copySchema。所以需要判断一下
                            if (!dest.containsTable(destTable.getTableName()))
                                dest.createTable(destTable);
                            KettleUtil.addSynchronizedComponent(transMeta , source , sourceTable , dest , destTable , setting , cnt);
                            cur ++;
                            cnt ++;
                        }
                       
                        // execute in batch
                        trans = new Trans(transMeta);
                        trans.prepareExecution(null);
                        trans.startThreads();
                        trans.waitUntilFinished();
                        
                        transMeta = null;
                        trans = null;
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
    
    @Override
    public void setBatchSize(int batchSize)
    {
        this.batchSize = batchSize;
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
