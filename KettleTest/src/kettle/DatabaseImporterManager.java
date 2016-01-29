package kettle;

import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import common.Pair;

import database.Database;

/**
 * 完全导入工具管理类的接口
 */
public interface DatabaseImporterManager 
{   
    /**
     * 用给出的多个数据库连接对来初始化
     */
    public void buildByConnPairs(Iterable<Pair<Database , Database>> pairs);
   
    /**
     * 用给出的多个数据库连接对来初始化
     */
    public void buildByConnPairs(Pair<Database , Database>... pairs);
    
    /**
     * 直接设置importers来进行build的初始化，这里的importer需要进行过build，不然无法进行导入
     */
    public void buildByImporters(Iterable<DatabaseImporter> importers);
    
    /**
     * 执行导入，返回是否所有的导入都成功了，如果都成功返回true，否则返回false。执行前必须build，否则结果不可预计。另外中间某个导入的失败并不会影响全部导入全部执行一遍
     * @param  isAsnyc 是否按照异步的方式进行执行
     */
    public boolean execute(boolean isAsync);
    
    /**
     * 执行所有数据库对的导入，执行顺序为依次执行。执行前必须build，否则结果不可预计。另外中间某个导入的失败并不会影响全部导入全部执行一遍
     */
    public List<ImportResult> executeSequential();
    
    /**
     * 异步执行导入，所有的数据库对导入并发执行。这时需要注意可能一个数据库打开的连接过多，因为所有的导入需要的连接都会打开；另外线程数可能也会过多。所以建议不要同时执行过多的数据库导入操作。执行前必须build，否则结果不可预计。
     * 另外中间某个导入的失败并不会影响全部导入全部执行一遍
     */
    public List<Future<ImportResult>> executeAsync();
    
    /**
     * 定时执行所有的importer，如果某个importer初始化失败或者执行失败都不会影响定时执行
     */
    public void timingExecute(long time , TimeUnit unit);
    
    /**
     * 进行导入的表的名称，如果设置了这个excludedTables就不能够进行设置，只能设置一次。这里的名称指的是所有源表中的名称，注意不同的数据库源中有相同的表名的情况，这样可能会造成两个表同时不被导入。
     * importer自身可能已经设置了表，这个设置对于该importer就变得无效了。执行需要在build之前，否则无效
     */
    public void setIncludedTables(Iterable<String> tables);
    
    /**
     * 不进行导入的表的名称，如果设置了这个includedTables就不能够进行设置，只能设置一次。这里的名称指的是所有源表中的名称，注意不同的数据库源中有相同的表名的情况，这样可能会造成两个表同时不被导入。
     * importer自身可能已经设置了表，这个设置对于该importer就变得无效了。执行需要在build之前，否则无效
     */
    public void setExcluedTables(Iterable<String> tables);
    
    /**
     * 停止导入操作
     */
    public void shutdown();

}

