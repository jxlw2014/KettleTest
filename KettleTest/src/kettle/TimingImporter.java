package kettle;

import java.util.concurrent.TimeUnit;


/**
 * ��ʱ����ӿ�
 */
public interface TimingImporter 
{
    /**
     * ִ�ж�ʱ���룬ִ��֮ǰһ����Ҫִ��build������������Ԥ��
     */
    public void timingExecute(long time , TimeUnit timeUnit);
}
