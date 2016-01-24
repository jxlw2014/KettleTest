package kettle;

import database.Database;

/**
 * ��һ�����ݿ�����һ�����ݿ������ȫ����ĵ���ӿ�
 */
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
     * ֹͣ����
     */
    public void shutdown();
}








