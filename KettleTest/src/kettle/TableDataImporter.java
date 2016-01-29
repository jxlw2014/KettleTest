package kettle;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import kettle.ImportResult.STATE;

import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;

import util.DatabaseUtil;
import util.KettleUtil;
import util.KettleUtil.TableImportSetting;
import util.Stopwatch;
import database.Database;
import database.Table;

/**
 * 表对表的数据导入类
 */
public class TableDataImporter implements TableImporter
{
    private TableDataImporter() { }
    
    /**
     * 获得一个新的导入对象
     */
    public static TableDataImporter newImporter()
    {
        return new TableDataImporter();
    }
    
    // 源表、目的表以及导入的策略
    private Database source;
    private Database dest;
    private Table sourceTable;
    private Table destTable;
    private IMPORT_STRATEGY strategy;
    
    // 表导入的参数设置
    private TableImportSetting setting = TableImportSetting.DEFAULT;
    
    // 支持异步执行，单线程保证先来先服务
    private ExecutorService asyncExecutor = Executors.newSingleThreadExecutor();
    
    @Override
    public boolean build(Database source, Table sourceTable, Database dest , IMPORT_STRATEGY strategy) 
    {
        this.source = source;
        this.dest = dest;
        this.sourceTable = sourceTable;
        this.destTable = DatabaseUtil.transformTable(source.databaseType() , dest.databaseType() , sourceTable);
        this.strategy = strategy;
        return true;
    }

    @Override
    public boolean execute() 
    {
        // 判断导入的策略
        switch (strategy)
        {
            case DIRECT_IMPORT:
                return executeImp();
                
            case IMPORT_IF_EXIST:
                if (dest.containsTable(destTable.getTableName()))
                    return executeImp();
                else
                    return false;
                
            case IMPORT_AFTER_DELETE:
                // 如果存在表，先删除
                if (dest.containsTable(destTable.getTableName()))
                    dest.dropTable(destTable.getTableName());
                dest.createTable(destTable);
                return executeImp();
                
            default:
                return false;
        }
    }

    @Override
    public Future<ImportResult> executeAsync() 
    {
        return asyncExecutor.submit(new Callable<ImportResult>()
        {
            @Override
            public ImportResult call() throws Exception 
            {
                Stopwatch watch = Stopwatch.newWatch();
                watch.start();
                // 如果执行成功
                if (executeImp())
                    return ImportResult.newResult()
                                       .setTime(watch.stop())
                                       .setState(STATE.SUCCESS)
                                       .setSourceDatabase(source.toString())
                                       .setDestDatabase(dest.toString());
                else
                    return ImportResult.FAIL;
            }
        });
    }
    
    @Override
    public void setSetting(TableImportSetting setting)
    {
        this.setting = setting;
    }
    
    @Override
    public void shutdown()
    {
        if (asyncExecutor != null)
            asyncExecutor.shutdown();
    }
    
    // 导入在kettle里面的具体实现
    private boolean executeImp()
    {
        TransMeta transMeta = new TransMeta();
        KettleUtil.addImportComponent(transMeta , source , sourceTable , dest , destTable , setting , 1);
        Trans trans = new Trans(transMeta);
        try
        {
            trans.prepareExecution(null);
            trans.startThreads();
            trans.waitUntilFinished();
            return true;
            
        } catch (Exception e)
        {
            return false;
        }
    }

}
