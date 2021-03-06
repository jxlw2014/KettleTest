package kettle;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import kettle.ImportResult.STATE;
import util.KettleUtil.TableImportSetting;
import util.Stopwatch;

import common.Pair;

import database.Database;


/**
 * <p>处理多个数据库导入的管理类，执行顺序如下：<br>
 * 1. 首先执行set有关的操作进行参数的设置  setSetting , setIncludedTables , setExcludedTables ...<br>
 * 2. 然后进行build操作 <br>
 * 3. 最后进行exec操作  <br> <br>
 * <p>另外如果需要设置导入时的batch参数，需要按照buildByImporters的方式来进行，在每个importer中设置batchSize。
 * 如果采用连接来进行初始化，则采用Consts中的DEFAULT_BATCH_SIZE。因为如果同时进行很多库的导入，batch过大会消耗
 * 很多的资源，默认采用最为节省的方式，当然可以根据具体的情况进行参数的调优
 */
public class ImporterManager extends AbstractDatabaseImporterManager
{
    /**
     * 导入的异步任务
     */
    private static class ImportCallable implements Callable<ImportResult>
    {
        private DatabaseImporter importer = null;
        private String sourceDatabase , destDatabase;
        
        ImportCallable(DatabaseImporter importer , String sourceDatabase , String destDatabase)
        {
            this.importer = importer;
            this.sourceDatabase = sourceDatabase;
            this.destDatabase = destDatabase;
        }

        @Override
        public ImportResult call()
        {
            // 如果导入工具初始化失败
            if (this.importer == null)
                return ImportResult.FAIL;
            else
            {
                Stopwatch watch = Stopwatch.newWatch();
                watch.start();
                // 如果导入失败
                if (!importer.execute())
                    return ImportResult.FAIL;
                // 如果导入成功
                else
                {
                    ImportResult result = ImportResult.newResult();
                    result.setTime(watch.stop())
                          .setSourceDatabase(this.sourceDatabase)
                          .setDestDatabase(this.destDatabase)
                          .setState(STATE.SUCCESS);
                    return result;
                }
            }
        }
    }
    
    /**
     * manager的类型
     */
    public enum MANAGER_TYPE
    {
        /**
         * 用来第一次的数据导入
         */
        IMPORT , 
        /**
         * 用来进行数据的同步
         */
        SYN
    }
    
    /**
     * 为了支持异步导入
     */
    private ExecutorService asyncService = Executors.newCachedThreadPool();
    /**
     * 为了支持定时执行
     */
    private ScheduledExecutorService timingService = null;
    /**
     * 是否开始timing
     */
    private AtomicBoolean timingIsStart = new AtomicBoolean(false);
    
    /**
     * 管理类的类型
     */
    // TODO 是统一为一个类好还是分成两个导入管理类
    private final MANAGER_TYPE type;
    
    /**
     * 数据库导入的设置对象
     */
    private TableImportSetting setting;
    
    // 进行具体处理的表
    private Set<String> tables;
    // -1表示设置了included，1表示设置了excluded，0表示没有进行设置
    private AtomicInteger setFlag = new AtomicInteger(0);
    
    private ImporterManager(MANAGER_TYPE type) 
    {
        this.type = type;
    }
    
    /**
     * 得到新的数据导入的管理对象
     * @param setting 导入参数设置
     */
    public static ImporterManager newDataImportManager(TableImportSetting setting)
    {
        ImporterManager manager = new ImporterManager(MANAGER_TYPE.IMPORT);
        manager.setting = setting;
        return manager;
    }
    
    /**
     * 得到新的数据同步的管理对象
     * @param setting 同步参数设置
     */
    public static ImporterManager newDataSynManager(TableImportSetting setting)
    {
        ImporterManager manager = new ImporterManager(MANAGER_TYPE.SYN);
        manager.setting = setting;
        return manager;
    }
    
    @Override
    public void setIncludedTables(Iterable<String> tables)
    {   
        if (setFlag.get() == 0)
        {
            setFlag.decrementAndGet();
            this.tables = new HashSet<String>();
            for (String str : tables)
                this.tables.add(str.toUpperCase());
        }
    }
    
    @Override
    public void setExcluedTables(Iterable<String> tables)
    {
        if (setFlag.get() == 0)
        {
            setFlag.incrementAndGet();
            this.tables = new HashSet<String>();
            for (String str : tables)
                this.tables.add(str.toUpperCase());
        }
    }
    
    @Override
    public void buildByImporters(Iterable<DatabaseImporter> importers)
    {
        this.connList.clear();
        this.importers.clear();
        
        // 判断所有的数据库导入对象
        for (DatabaseImporter importer : importers)
        {
            // 如果是合法的导入对象
            if (importer != null)
            {
                Pair<Database , Database> conn = importer.getConnPair();
                // 如果导入已经成功build了
                if (conn != null)
                {
                    // 判断管理类的类型
                    switch (this.type)
                    {
                        case IMPORT:
                            // 检查类型
                            if (importer instanceof DatabaseDataImporter)
                                this.connList.add(conn);
                                this.importers.add(importer);
                                
                        case SYN:
                            // 检查类型
                            if (importer instanceof TimingDatabaseDataSynchronization)
                                this.connList.add(conn);
                                this.importers.add(importer);
                    }
                }
            }
        }
        
        // 执行前的初始化
        initBeforeExecute();
    }

    @Override
    public List<ImportResult> executeSequential() 
    {
        System.out.println("Start import sequential...");
        
        List<ImportResult> results = new ArrayList<ImportResult>();
        int index = 0;
        // 依次处理所有的importer
        for (DatabaseImporter importer : this.importers)
        {
            // 如果导入工具初始化失败了
            if (importer == null)
                results.add(ImportResult.FAIL);
            else
            {
                Pair<Database , Database> pair = this.connList.get(index);
                // 获得result中的时间
                Stopwatch watch = Stopwatch.newWatch();
                watch.start();
                if (importer.execute())
                {
                    ImportResult result = ImportResult.newResult();
                    result.setSourceDatabase(pair.first.toString())
                          .setDestDatabase(pair.second.toString())
                          .setState(STATE.SUCCESS)
                          .setTime(watch.stop());
                    results.add(result);
                }
                else
                    results.add(ImportResult.FAIL);
            }
            index ++;
        }
        return results;
    }

    @Override
    public List<Future<ImportResult>> executeAsync()
    {
        System.out.println("Start import async...");
        
        List<Future<ImportResult>> results = new ArrayList<Future<ImportResult>>();
        // 所有的导入类
        int index = 0;
        // 处理所有的导入对象
        for (DatabaseImporter importer : this.importers)
        {
            Pair<Database , Database> pair = this.connList.get(index);
            ImportCallable callable = new ImportCallable(importer , pair.first.toString() , pair.second.toString());
            Future<ImportResult> result = asyncService.submit(callable);
            results.add(result);
            index ++;
        }
        return results;
    }
    
    @Override
    public void timingExecute(long time, TimeUnit unit) 
    {
        // 如果是第一次执行
        if (!timingIsStart.get())
        {   
            timingIsStart.set(true);
            // 定期执行任务
            timingService.scheduleAtFixedRate(new Runnable()
            {
                @Override
                public void run()
                {
                    // 判断所有的importer
                    for (DatabaseImporter importer : importers)
                    {
                        // 如果不为null
                        if (importer != null)
                            importer.execute();
                    }
                }
            } , 0 , time , unit);
        }
    }

    @Override
    public void shutdown() 
    {
        // 所有的导入工具进行shutdown
        for (DatabaseImporter importer : this.importers)
            importer.shutdown();
        // 停掉线程池
        if (asyncService != null)
            asyncService.shutdown();
        if (timingService != null)
            timingService.shutdown();
    }

    @Override
    protected void buildImporters() 
    {
        // 对每个数据库连接得到对应的导入类 
        for (Pair<Database , Database> pair : this.connList)
        {
            // 获得需要的importer
            DatabaseImporter importer = getImporter();
            // 如果importer获取失败
            if (importer == null)
                this.importers.add(null);
            else
            {
                // 如果build失败了，放个null占个位置
                if (importer.build(pair.first , pair.second))
                    this.importers.add(importer);
                else
                    this.importers.add(null);
            }
        }
    }
    
    /**
     * 执行之前的操作
     */
    @Override
    protected void initBeforeExecute()
    {
        if (setFlag.get() != 0)
        {
            // init the importers，according to the value of setFlag
            for (DatabaseImporter importer : this.importers)
            {
                if (setFlag.get() < 0)
                    importer.setIncludedTables(this.tables);
                else
                    importer.setExcludedTables(this.tables);
            }
        }
    }
    
    /**
     * 根据ManagerType获得对应的importer
     */
    private DatabaseImporter getImporter()
    {
        // 获得需要的importer
        switch (this.type)
        {
            case IMPORT:
                DatabaseDataImporter importer = DatabaseDataImporter.newImporter();
                importer.setSetting(setting);
                return importer;
                
            case SYN:
                TimingDatabaseDataSynchronization syn = TimingDatabaseDataSynchronization.newInstance();
                syn.setSetting(setting);
                return syn;
                
            default:
                return null;
        }
    }
    
}


