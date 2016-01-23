package kettle;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;

import util.DatabaseUtil;
import util.KettleUtil;
import util.KettleUtil.ImportSetting;
import database.Database;
import database.Table;

/**
 * 数据导入类，实现将一个数据库中的所有表数据导入到另一个数据库中的功能，底层使用kettle核心类实现
 */
public class DataImporter implements DatabaseImporter 
{
    /**
     * 工作状态
     */
    public enum STATE
    {
        NEW , BUILD , EXECUTE;
    }
    
    // 进行import操作的batch大小
    private int batchSize = 5;
    
    // 源数据库和目的数据库
    private Database source = null;
    private Database dest = null;
    
    // 初始状态是new
    private STATE state = STATE.NEW;
    
    // 导入设置
    private ImportSetting setting = ImportSetting.DEFAULT;
    
    // 判断是否shutdown
    private AtomicBoolean isShutdown = new AtomicBoolean(false);
    
    private DataImporter() { }
    
    /**
     * 新建一个数据导入对象
     */
    public static DataImporter newImporter()
    {
        return new DataImporter();
    }
    
    /**
     * 设置导入的参数
     */
    public void setSetting(ImportSetting setting)
    {
        this.setting = setting;
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
                        Table destTable = DatabaseUtil.transformTable(source.databaseType() , dest.databaseType() , sourceTable);
                        KettleUtil.addImportComponent(transMeta , source , sourceTable , dest , destTable , this.setting , cnt);
                        cur ++;
                        cnt ++;
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

    @Override
    public void setBatchSize(int batchSize) 
    {
        this.batchSize = batchSize;
    }
    
    @Override
    public void shutdown()
    {
        this.isShutdown.set(true);
    }

}



