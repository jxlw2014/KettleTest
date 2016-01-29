package kettle;

import util.KettleUtil.TableImportSetting;

import common.Pair;

import database.Database;

/**
 * 从一个数据库向另一个数据库进行完全导入的导入接口
 */
// TODO 有没有必要提供异步执行接口?
public interface DatabaseImporter 
{
    /**
     * 预处理导入需要的信息，主要包括数据库结构的复制和转换结构的设置
     * @param source 源数据库
     * @param dest   目的数据库
     */
    public boolean build(Database source , Database dest);
    
    /**
     * 每次导入处理的表的数目，如果太大可能会导致数据库连接过多而导致kettle无法使用。默认的batch大小在Constants中设置
     * @param batchSize 并发进行处理的表的数目
     */
    public void setBatchSize(int batchSize);

    /**
     * 执行一次导入操作，如果是导入就是完整的导入一次，如果是同步就是完整的同步一次。在执行exsecute之前必须进行build，否则结果不可预估
     */
    public boolean execute();
    
    /**
     * 设置导入的参数
     */
    public void setSetting(TableImportSetting setting);
    
    /**
     * 设置进行导入的表的名称，如果设置了excludedTables就不能够进行设置
     */
    public void setIncludedTables(Iterable<String> tables);
    
    /**
     * 设置不进行导入的表的名称，如果设置了includedTables就不能够再进行设置
     */
    public void setExcludedTables(Iterable<String> tables);
    
    /**
     * 获得导入的数据库连接对
     */
    public Pair<Database , Database> getConnPair();
    
    /**
     * 停止导入
     */
    public void shutdown();
}








