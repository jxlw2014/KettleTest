package kettle;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import kettle.DatabaseImporterManager.ImportResult.STATE;
import util.KettleUtil.ImportSetting;
import util.KettleUtil.SynchronizationSetting;
import util.Stopwatch;

import common.Pair;

import database.Database;


/**
 * 处理多个数据库导入的管理类
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
                          .setSourceDatabasename(this.sourceDatabase)
                          .setDestDatabasename(this.destDatabase)
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
    private ExecutorService asyncService = null;
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
    private final MANAGER_TYPE type;
    
    // TODO 最好能够为两个setting找到一个父类，以取代这里的Object。这样的实现不太好
    private Object setting;
    
    private ImporterManager(MANAGER_TYPE type) 
    {
        this.type = type;
    }
    
    /**
     * 得到新的数据导入的管理对象
     * @param setting 导入参数设置
     */
    public static ImporterManager newDataImporterMananger(ImportSetting setting)
    {
        ImporterManager manager = new ImporterManager(MANAGER_TYPE.IMPORT);
        manager.setting = setting;
        return manager; 
    }
    
    /**
     * 得到新的数据同步的管理对象
     * @param setting 同步参数设置
     */
    public static ImporterManager newDataSynManager(SynchronizationSetting setting)
    {
        ImporterManager manager = new ImporterManager(MANAGER_TYPE.SYN);
        manager.setting = setting;
        return manager;
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
                    result.setSourceDatabasename(pair.first.toString())
                          .setDestDatabasename(pair.second.toString())
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
        asyncService = Executors.newCachedThreadPool();
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
     * 根据ManagerType获得对应的importer
     */
    private DatabaseImporter getImporter()
    {
        // 获得需要的importer
        switch (this.type)
        {
            case IMPORT:
                DataImporter importer = DataImporter.newImporter();
                importer.setSetting((ImportSetting) setting);
                return importer;
                
            case SYN:
                TimingDataSynchronization syn = TimingDataSynchronization.newInstance();
                syn.setSetting((SynchronizationSetting) setting);
                return syn;
                
            default:
                return null;
        }
    }
    
}


