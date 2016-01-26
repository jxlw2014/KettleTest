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
 * 定时数据同步，从一个数据库定时同步另一个数据库。假定两个数据库之间已经执行过import的操作，两个数据库有一致的表结构
 */
public class TimingDataSynchronization implements DatabaseImporter , TimingImporter
{
    private TimingDataSynchronization() { }
    
    /**
     * 获得新的同步实例
     */
    public static TimingDataSynchronization newInstance()
    {
        return new TimingDataSynchronization();
    }
    
    // syn工作的batch大小
    private int batchSize = Constants.DEFAULT_BATCH_SIZE;
    
    // 源、目的数据库
    private Database source;
    private Database dest;
    
    // 同步参数的设置
    private SynchronizationSetting setting = SynchronizationSetting.DEFAULT;
    
    // 支持定时操作的线程池
    private ScheduledExecutorService executor; 
    
    // 是否停止的标志
    private AtomicBoolean isShutdown = new AtomicBoolean(false);
    
    /**
     * 设置同步的参数
     */
    // TODO 最好能够抽象进入DatabaseImporter
    public void setSetting(SynchronizationSetting setting)
    {
        this.setting = setting;
    }

    @Override
    public boolean build(Database source, Database dest) 
    {
        // 复制结构看情况进行，有可能已经有一样的表结构，这样就不适合删除重建了
        this.source = source;
        this.dest = dest;
        this.isShutdown.set(false);
        // 就是返回true，没什么可以判断的
        return true;
    }

    /**
     * 定时执行同步任务
     */
    @Override
    public void timingExecute(long time , TimeUnit timeUnit)
    {
        // 一个线程就够了，只要管理定时操作就可以了
        executor = Executors.newScheduledThreadPool(1);
        // 定时执行同步操作
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
                    
                    // 存在没有destTable的可能，因为没有执行一遍copySchema。所以需要判断一下
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
     * 停止数据同步线程
     */
    public void shutdown()
    {
        if (executor != null)
            executor.shutdown();
        isShutdown.set(true);
    }
    
}
