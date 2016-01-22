package kettle;

import database.Database;

/**
 * 从一个数据库向另一个数据库进行完全导入的导入接口
 */
public interface EntireImporter 
{
    /**
     * 预处理导入需要的信息，主要包括数据库结构的复制和转换结构的设置
     * @param source 源数据库
     * @param dest   目的数据库
     */
    public void build(Database source , Database dest);
    
    /**
     * 每次导入处理的表的数目，如果太大可能会导致数据库连接过多而导致kettle无法使用
     * @param batchSize 表的数目
     */
    public void setBatchSize(int batchSize);

    /**
     * 执行导入操作
     */
    public void execute();
    
    /**
     * 停止导入
     */
    public void shutdown();
}








