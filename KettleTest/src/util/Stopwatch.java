package util;

/**
 * �򵥵����ʵ��
 */
public class Stopwatch 
{   
    private Stopwatch() { }
    
    private long curTime = - 1;
    
    /**
     * ����µĶ���
     */
    public synchronized static Stopwatch newWatch()
    {
        return new Stopwatch();
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
