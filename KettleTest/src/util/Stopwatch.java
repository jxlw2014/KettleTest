package util;

/**
 * 简单的秒表实现
 */
public class Stopwatch 
{
    private static final class Singleton
    {
        public static final Stopwatch instance = new Stopwatch();
    }
    
    private Stopwatch() { }
    
    private long curTime = - 1;
    
    /**
     * 获得单例对象
     */
    public static Stopwatch getWatch()
    {
        return Singleton.instance;
    }
    
    /**
     * 开始计时
     */
    public synchronized void start()
    {
        this.curTime = System.currentTimeMillis();
    }
    
    /**
     * 停止计时，如果之前执行过start，返回过去的时间，否则返回0
     */
    public synchronized double stop()
    {
        // 如果没有执行过start
        if (this.curTime < 0)
            return 0;
        // 如果执行过start
        else
        {
            long time = System.currentTimeMillis() - this.curTime;
            this.curTime = - 1;
            return (double) time / (double) 1000;
        }
    }
    
}
