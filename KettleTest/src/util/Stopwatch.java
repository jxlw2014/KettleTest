package util;

/**
 * �򵥵����ʵ��
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
     * ��õ�������
     */
    public static Stopwatch getWatch()
    {
        return Singleton.instance;
    }
    
    /**
     * ��ʼ��ʱ
     */
    public synchronized void start()
    {
        this.curTime = System.currentTimeMillis();
    }
    
    /**
     * ֹͣ��ʱ�����֮ǰִ�й�start�����ع�ȥ��ʱ�䣬���򷵻�0
     */
    public synchronized double stop()
    {
        // ���û��ִ�й�start
        if (this.curTime < 0)
            return 0;
        // ���ִ�й�start
        else
        {
            long time = System.currentTimeMillis() - this.curTime;
            this.curTime = - 1;
            return (double) time / (double) 1000;
        }
    }
    
}
