package kettle;

import database.Database;

/**
 * ��һ�����ݿ�����һ�����ݿ������ȫ����ĵ���ӿ�
 */
public interface EntireImporter 
{
    /**
     * Ԥ��������Ҫ����Ϣ����Ҫ�������ݿ�ṹ�ĸ��ƺ�ת���ṹ������
     * @param source Դ���ݿ�
     * @param dest   Ŀ�����ݿ�
     */
    public void build(Database source , Database dest);
    
    /**
     * ÿ�ε��봦��ı����Ŀ�����̫����ܻᵼ�����ݿ����ӹ��������kettle�޷�ʹ��
     * @param batchSize �����Ŀ
     */
    public void setBatchSize(int batchSize);

    /**
     * ִ�е������
     */
    public void execute();
    
    /**
     * ֹͣ����
     */
    public void shutdown();
}








