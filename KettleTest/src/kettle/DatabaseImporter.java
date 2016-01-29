package kettle;

import util.KettleUtil.TableImportSetting;

import common.Pair;

import database.Database;

/**
 * ��һ�����ݿ�����һ�����ݿ������ȫ����ĵ���ӿ�
 */
// TODO ��û�б�Ҫ�ṩ�첽ִ�нӿ�?
public interface DatabaseImporter 
{
    /**
     * Ԥ��������Ҫ����Ϣ����Ҫ�������ݿ�ṹ�ĸ��ƺ�ת���ṹ������
     * @param source Դ���ݿ�
     * @param dest   Ŀ�����ݿ�
     */
    public boolean build(Database source , Database dest);
    
    /**
     * ÿ�ε��봦��ı����Ŀ�����̫����ܻᵼ�����ݿ����ӹ��������kettle�޷�ʹ�á�Ĭ�ϵ�batch��С��Constants������
     * @param batchSize �������д���ı����Ŀ
     */
    public void setBatchSize(int batchSize);

    /**
     * ִ��һ�ε������������ǵ�����������ĵ���һ�Σ������ͬ������������ͬ��һ�Ρ���ִ��exsecute֮ǰ�������build������������Ԥ��
     */
    public boolean execute();
    
    /**
     * ���õ���Ĳ���
     */
    public void setSetting(TableImportSetting setting);
    
    /**
     * ���ý��е���ı�����ƣ����������excludedTables�Ͳ��ܹ���������
     */
    public void setIncludedTables(Iterable<String> tables);
    
    /**
     * ���ò����е���ı�����ƣ����������includedTables�Ͳ��ܹ��ٽ�������
     */
    public void setExcludedTables(Iterable<String> tables);
    
    /**
     * ��õ�������ݿ����Ӷ�
     */
    public Pair<Database , Database> getConnPair();
    
    /**
     * ֹͣ����
     */
    public void shutdown();
}








