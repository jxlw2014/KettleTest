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
 * 定时数据同步，从一个数据库定时同步另一个数据库。假定两个数据库之间已经执行过import的操作，两个数据库有一致的表结构
 */
public class TimingDatabaseDataSynchronization implements DatabaseImporter , TimingImporter
{
    private TimingDatabaseDataSynchronization() { }
    
    /**
     * 获得新的同步实例
     */
    public static TimingDatabaseDataSynchronization newInstance()
    {
        return new TimingDatabaseDataSynchronization();
    }
    
    // syn工作的batch大小
    private int batchSize = Constants.DEFAULT_BATCH_SIZE;
    
    // 源、目的数据库
    private Database source;
    private Database dest;
    
    // 同步参数的设置
    private TableImportSetting setting = TableImportSetting.DEFAULT;
    
    // 支持定时操作的线程池
    private ScheduledExecutorService schedule = null;
    private AtomicBoolean timingIsSet = new AtomicBoolean(false);
    
    // 是否停止的标志
    private AtomicBoolean isShutdown = new AtomicBoolean(false);
    
    // 设置表相关的信息
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
        // 复制结构看情况进行，有可能已经有一样的表结构，这样就不适合删除重建了
        this.source = source;
        this.dest = dest;
        this.isShutdown.set(false);
        // 就是返回true，没什么可以判断的
        return true;
    }
    
    @Override
    public Pair<Database , Database> getConnPair()
    {
        // 如果没有成功build
        if (this.source == null || this.dest == null)
            return null;
        else
            return Pair.newPair(this.source , this.dest);
    }

    /**
     * 定时执行同步任务
     */
    @Override
    public void timingExecute(long time , TimeUnit timeUnit)
    {
        // 如果没有设置timing
        if (!this.timingIsSet.get())
        {
            this.timingIsSet.set(true);
            // 一个线程就够了，只要管理定时操作就可以了
            schedule = Executors.newSingleThreadScheduledExecutor();
            // 定时执行同步操作
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
                    // 如果表可以
                    if (check(sourceTable))
                    {
                        Table destTable = DatabaseUtil.transformTable(source.databaseType() , dest.databaseType() , sourceTable);
                        
                        // 存在没有destTable的可能，因为没有执行一遍copySchema。所以需要判断一下
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
     * 停止数据同步线程
     */
    public void shutdown()
    {
        isShutdown.set(true);
        this.timingIsSet.set(false);
        if (this.schedule != null)
            this.schedule.shutdown();
    }
    
    /**
     * 判断一个表是不是应该进行同步
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
