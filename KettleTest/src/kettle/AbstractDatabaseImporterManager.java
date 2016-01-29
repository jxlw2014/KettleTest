package kettle;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;

import kettle.ImportResult.STATE;

import common.Pair;

import database.Database;

/**
 * 数据库导入工具的通用实现
 */
public abstract class AbstractDatabaseImporterManager implements DatabaseImporterManager
{
    /**
     * 保存所有的数据库连接
     */
    protected List<Pair<Database , Database>> connList = new ArrayList<Pair<Database , Database>>();
    /**
     * 底层保存导入工具的列表
     */
    protected List<DatabaseImporter> importers = new ArrayList<DatabaseImporter>();
    
    /**
     * build采用的策略是对每个连接对进行顺序处理，并不是并发处理
     */
    @Override
    public void buildByConnPairs(Iterable<Pair<Database, Database>> pairs) 
    {
        this.connList.clear();
        this.importers.clear();
        
        for (Pair<Database , Database> pair : pairs)
            connList.add(pair);
        buildImporters();
            
        // 执行前的初始化
        initBeforeExecute();
    }

    /**
     * build采用的策略是对每个连接对进行顺序处理，并不是并发处理
     */
    @Override
    public void buildByConnPairs(Pair<Database, Database>... pairs) 
    {
        this.connList.clear();
        this.importers.clear();
        
        for (Pair<Database , Database> pair : pairs)
            connList.add(pair);
        buildImporters();
            
        // 执行前的初始化
        initBeforeExecute();
    } 
    
    @Override
    public boolean execute(boolean isAsync)
    {
        // 如果是异步
        if (isAsync)
        {
            List<Future<ImportResult>> results = executeAsync();
            for (Future<ImportResult> result : results)
            {
                try
                {
                    // 如果失败了
                    if (result.get().state() == STATE.FAIL)
                        return false;
                } catch (Exception e)
                {
                    // 异常也算执行失败了
                    return false;
                }
            }
            return true;
        }
        else
        {
            List<ImportResult> results = executeSequential();
            // 判断所有的结果
            for (ImportResult result : results)
            {
                // 如果存在一个导入失败
                if (result.state() == STATE.FAIL)
                    return false;
            }
            return true;
        }
    }
    
    /**
     * 执行之前的初始化
     */
    protected abstract void initBeforeExecute();
    
    /**
     * 对每个连接pair构造importer对象
     */
    protected abstract void buildImporters();
    
}
