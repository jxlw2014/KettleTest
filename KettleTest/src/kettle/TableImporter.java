package kettle;

import java.util.concurrent.Future;

import util.KettleUtil.TableImportSetting;
import database.Database;
import database.Table;

/**
 * 表对表导入的接口，相较于全库的导入，表对表导入可以提供更细致的控制
 */

// TODO 如果需要对一些表采用特殊的导入，同步方案；全库导入和这个应该可以结合起来用
public interface TableImporter 
{
    /**
     * 导入的策略
     */
    public enum IMPORT_STRATEGY
    {
        /**
         * 直接进行导入，不做其它的判断，也不管目的表是不是存在
         */
        DIRECT_IMPORT ,   
        /**
         * 如果目的表存在才进行导入，否则不做任何操作。但是不检查目的表的schema是否合法
         */
        IMPORT_IF_EXIST , 
        /**
         * 如果存在目的表，删除重建然后重新导入 。如果不存在目的表，建表后进行导入
         */
        IMPORT_AFTER_DELETE , 
    }
    
    /**
     * 建立importer
     * @param source            源数据库
     * @param sourceTable       源表
     * @param dest              目的数据库
     */
    public boolean build(Database source , Table sourceTable , Database dest , IMPORT_STRATEGY strategy);
        
    /**
     * 执行导入操作，导入之前需要进行build，否则结果不可预估
     */
    public boolean execute();
    
    /**
     * 设置导入的参数
     */
    public void setSetting(TableImportSetting setting);
    
    /**
     * 异步执行导入操作，导入之前需要进行build，否则结果不可预估。加入异步操作的目的是为了让这个类的功能更为独立，
     * 因为需要表单独执行的情况比较少，
     */
    public Future<ImportResult> executeAsync();
    
    /**
     * 停止表导入服务
     */
    public void shutdown();
}





