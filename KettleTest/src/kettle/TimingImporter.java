package kettle;

import java.util.concurrent.TimeUnit;


/**
 * 定时导入接口
 */
public interface TimingImporter 
{
    /**
     * 执行定时导入，执行之前一定需要执行build，否则结果不可预估
     */
    public void timingExecute(long time , TimeUnit timeUnit);
}
