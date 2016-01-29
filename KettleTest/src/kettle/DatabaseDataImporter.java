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
 * 数据导入类，实现将一个数据库中的所有表数据导入到另一个数据库中的功能。采用的导入策略是：首先复制表的结构，如果目的数据库中
 * 有同名表，就进行重建。然后进行数据的导入
 */
public class DatabaseDataImporter implements DatabaseImporter , TimingImporter 
{
    /**
     * 工作状态
     */
    public enum STATE
    {
        NEW , BUILD , EXECUTE;
    }
    
    // 进行import操作的batch大小
    private int batchSize = Constants.DEFAULT_BATCH_SIZE;
    
    // 源数据库和目的数据库
    private Database source = null;
    private Database dest = null;
    
    // 初始状态是new
    private STATE state = STATE.NEW;
    
    // 导入设置
    private TableImportSetting setting = TableImportSetting.DEFAULT;
    
    // 判断是否shutdown
    private AtomicBoolean isShutdown = new AtomicBoolean(false);
    
    // 条件table，作为导入时的前提条件
    private AtomicInteger setFlag = new AtomicInteger(0);
    private Set<String> tables;
    
    // 支持定时执行操作
    private AtomicBoolean timingIsSet = new AtomicBoolean(false);
    private ScheduledExecutorService schedule = null;
    
    private DatabaseDataImporter() { }
    
    /**
     * 新建一个数据导入对象
     */
    public static DatabaseDataImporter newImporter()
    {
        return new DatabaseDataImporter();
    }
    
    @Override
    public void setSetting(TableImportSetting setting)
    {
        this.setting = setting;
    }
    
    @Override
    public void setIncludedTables(Iterable<String> tables) 
    {
        // 如果没有进行过设置
        if (this.setFlag.get() == 0)
        {
            this.setFlag.decrementAndGet();
            this.tables = new HashSet<String>();
            for (String table : tables)
                this.tables.add(table.toUpperCase());
        }
    }

    @Override
    public void setExcludedTables(Iterable<String> tables) 
    {
        // 如果没有进行过设置
        if (this.setFlag.get() == 0)
        {
            this.setFlag.incrementAndGet();
            this.tables = new HashSet<String>();
            for (String table : tables)
                this.tables.add(table.toUpperCase());
        }
    }
    
    @Override
    public boolean build(Database source , Database dest)
    {
        // 默认的设置
        this.isShutdown.set(false);
        System.out.println("Start build the necessary things for transformation...");
        // 如果是初始状态或者build过
        if (state == STATE.NEW || state == STATE.BUILD)
        {
            // 如果复制表成功
            if (DatabaseUtil.copyScheme(source , dest))
            {
                System.out.println("copy scheme success...");
                // 改变状态
                state = STATE.BUILD;
                // 设置数据源和数据目的地
                this.source = source;
                this.dest = dest;
                return true;
            }   
            else
            {
                System.out.println("copy scheme fail...");
                return false;
            }
        }
        else
        {
            System.out.println("You should finish executing first...");
            return false;
        }
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
    
    @Override
    public boolean execute()
    {
        // 如果已经BUILD了
        if (state == STATE.BUILD)
        {
            // 改变状态为执行
            state = STATE.EXECUTE;
            
            try
            {
                long curTime = System.currentTimeMillis();
                System.out.println("Start import...");
             
                // 批量执行转换
                int cur = 0;
                // 获得tables
                List<Table> tableList = source.tables();
                while (cur < tableList.size() && !isShutdown.get())
                {
                    int cnt = 1;
                    TransMeta transMeta = new TransMeta();
                    Trans trans = null;
                    
                    // 可能有batchSize个，也可能没有这么多
                    while (cur < tableList.size() && cnt <= batchSize)
                    {
                        Table sourceTable = tableList.get(cur);
                        // 如果需要进行导入
                        if (check(sourceTable))
                        {
                            Table destTable = DatabaseUtil.transformTable(source.databaseType() , dest.databaseType() , sourceTable);
                            KettleUtil.addImportComponent(transMeta , source , sourceTable , dest , destTable , this.setting , cnt);
                            cnt ++;
                        }
                        cur ++;
                    }
                    
                    trans = new Trans(transMeta);
                    // 执行转换
                    trans.prepareExecution(null);
                    trans.startThreads();
                    trans.waitUntilFinished();
                    
                    transMeta = null;
                    trans = null;
                    
                    System.out.println(String.format("Import %d table success..." , cur));
                }
                
                long execTime = System.currentTimeMillis() - curTime;
                System.out.println(String.format("The import time is %fs" , ((double) execTime / 1000)));
                
            } catch (Exception e)
            {
                System.out.println("import fail...");
                return false;
            }

            // 状态回到初始
            state = STATE.NEW;
            return true;
        }
        else
        {
            System.out.println("You need build before execute...");
            return false;
        }
    }
    
    /**
     * execute之前需要执行build，否则结果不可预估。另外这里定时执行execute其实也是一种同步的方式，因为每次都会删除重建
     */
    @Override
    public void timingExecute(long time , TimeUnit timeUnit)
    {
        // 如果没有设置定时导入
        if (!timingIsSet.get())
        {
            timingIsSet.set(true);
            schedule = Executors.newSingleThreadScheduledExecutor();
            schedule.scheduleAtFixedRate(new Runnable()
            {
                @Override
                public void run() 
                {
                    execute();
                }
            } , 0 , time , timeUnit);
        }
    }   

    @Override
    public void setBatchSize(int batchSize) 
    {
        this.batchSize = batchSize;
    }
    
    @Override
    public void shutdown()
    {
        this.isShutdown.set(true);
        this.timingIsSet.set(false);
        // 如果不为null
        if (this.schedule != null)
        {
            this.schedule.shutdown();
            this.schedule = null;
        }
    }
    
    /**
     * 判断表是否需要导入
     */
    private boolean check(Table table)
    {
        // 如果没有进行设置
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



