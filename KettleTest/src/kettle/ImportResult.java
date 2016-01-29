package kettle;

/**
 * 一次导入后的执行结果
 */
public class ImportResult
{
    /**
     * 导入的状态
     */
    public enum STATE
    {
        SUCCESS , 
        FAIL;
    }
    
    /**
     * 失败结果使用的统一对象
     */
    public static final ImportResult FAIL = new ImportResult().setState(STATE.FAIL);
    
    // 导入状态
    private STATE state;
    // 导入时间
    private double time;
    // 导入的源数据库信息
    private String sourceDatabase;
    // 导入的目的数据库信息
    private String destDatabase;
    
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

    ImportResult setSourceDatabase(String sourceDatabase) 
    {
        this.sourceDatabase = sourceDatabase;
        return this;
    }

    ImportResult setDestDatabase(String destDatabase) 
    {
        this.destDatabase = destDatabase;
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
     * 获得源数据库信息
     */
    public String sourceDatabase()
    {
        return this.sourceDatabase;
    }
    
    /**
     * 获得目的数据库信息
     */
    public String destDatabase()
    {
        return this.destDatabase;
    }
    
    @Override
    public String toString()
    {
        // state , time , sourceDatabase , destDatabase
        StringBuilder builder = new StringBuilder();
        builder.append("{");
        builder.append(String.format("state:%s" , this.state.toString()));
        // 如果失败了
        if (this.state == STATE.FAIL)
            builder.append("}");
        // 如果成功了，后面的东西都要
        else
        {
            builder.append(", ");
            builder.append(String.format("time:%f" , this.time));
            builder.append(", ");
            builder.append(String.format("source_db:%s" , this.sourceDatabase));
            builder.append(", ");
            builder.append(String.format("dest_db:%s" , this.destDatabase));
            builder.append("}");
        }
        return builder.toString();
    }
}
