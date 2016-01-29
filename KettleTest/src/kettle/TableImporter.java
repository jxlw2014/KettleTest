package kettle;

import java.util.concurrent.Future;

import util.KettleUtil.TableImportSetting;
import database.Database;
import database.Table;

/**
 * ��Ա���Ľӿڣ������ȫ��ĵ��룬��Ա�������ṩ��ϸ�µĿ���
 */

// TODO �����Ҫ��һЩ���������ĵ��룬ͬ��������ȫ�⵼������Ӧ�ÿ��Խ��������
public interface TableImporter 
{
    /**
     * ����Ĳ���
     */
    public enum IMPORT_STRATEGY
    {
        /**
         * ֱ�ӽ��е��룬�����������жϣ�Ҳ����Ŀ�ı��ǲ��Ǵ���
         */
        DIRECT_IMPORT ,   
        /**
         * ���Ŀ�ı���ڲŽ��е��룬�������κβ��������ǲ����Ŀ�ı��schema�Ƿ�Ϸ�
         */
        IMPORT_IF_EXIST , 
        /**
         * �������Ŀ�ı�ɾ���ؽ�Ȼ�����µ��� �����������Ŀ�ı��������е���
         */
        IMPORT_AFTER_DELETE , 
    }
    
    /**
     * ����importer
     * @param source            Դ���ݿ�
     * @param sourceTable       Դ��
     * @param dest              Ŀ�����ݿ�
     */
    public boolean build(Database source , Table sourceTable , Database dest , IMPORT_STRATEGY strategy);
        
    /**
     * ִ�е������������֮ǰ��Ҫ����build������������Ԥ��
     */
    public boolean execute();
    
    /**
     * ���õ���Ĳ���
     */
    public void setSetting(TableImportSetting setting);
    
    /**
     * �첽ִ�е������������֮ǰ��Ҫ����build������������Ԥ���������첽������Ŀ����Ϊ���������Ĺ��ܸ�Ϊ������
     * ��Ϊ��Ҫ����ִ�е�����Ƚ��٣�
     */
    public Future<ImportResult> executeAsync();
    
    /**
     * ֹͣ�������
     */
    public void shutdown();
}





