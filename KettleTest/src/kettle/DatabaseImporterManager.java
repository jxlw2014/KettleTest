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
     * 一次导入的结果
     */
    public static class ImportResult
    {
        /**
         * 导入的状态
         */
        public enum STATE
        {
            SUCCESS , FAIL;
        }
        
        /**
         * 失败结果使用的统一对象
         */
        public static final ImportResult FAIL = new ImportResult().setState(STATE.FAIL);
        
        // 导入状态
        private STATE state;
        // 导入时间
        private double time;
        // 导入的源数据库名
        private String sourceDatabasename;
        // 导入的目的数据库名
        private String destDatabasename;
        
        private ImportResult() { }
        
        /**
         * 获得一个新的导入结果
         */
        static ImportResult newResult()
        {
            return new ImportResult();
        }

        ImportResult setState(STATE state)
        {
            this.state = state;
            return this;
        }

        ImportResult setTime(double time) 
        {
            this.time = time;
            return this;
        }

        ImportResult setSourceDatabasename(String sourceDatabasename) 
        {
            this.sourceDatabasename = sourceDatabasename;
            return this;
        }

        ImportResult setDestDatabasename(String destDatabasename) 
        {
            this.destDatabasename = destDatabasename;
            return this;
        }
        
        /**
         * 获得导入的状态
         */
        public STATE state()
        {
            return this.state;
        }
        
        /**
         * 获得导入需要的时间
         */
        public double time()
        {
            return this.time;
        }
        
        /**
         * 获得源数据库名
         */
        public String sourceDatabasename()
        {
            return this.sourceDatabasename;
        }
        
        /**
         * 获得目的数据库名
         */
        public String destDatabasename()
        {
            return this.destDatabasename;
        }
    }
    
    /**
     * 用给出的多个数据库连接对来初始化
     */
    public void build(Iterable<Pair<Database , Database>> pairs);
   
    /**
     * 用给出的多个数据库连接对来初始化
     */
    public void build(Pair<Database , Database>... pairs);
    
    /**
     * 执行导入，返回是否所有的导入都成功了，如果都成功返回true，否则返回false。执行前必须build，否则结果不可预计。另外中间某个导入的失败并不会影响全部导入全部执行一遍
     * @param isAsnyc 是否按照异步的方式进行执行
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
     * 停止导入操作
     */
    public void shutdown();

}

