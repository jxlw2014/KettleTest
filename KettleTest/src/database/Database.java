package database;

import java.util.List;

/**
 * ͨ�õ����ݿ�ʵ�ֽӿڣ��漰��������ز���
 */
public interface Database extends KettleDatabase
{
    /**
     * ���ݿ������
     */
    public enum DATABASE_TYPE
    {
        ORACLE("Oracle") , MYSQL("Mysql");
     
        private String codename;
        
        private DATABASE_TYPE(String codename)
        {
            this.codename = codename;
        }
        
        /**
         * ���ش�����ʹ�õ�����
         */
        public String toString()
        {
            return this.codename;
        }
    }
    
    /**
     * ������ݿ��ж�Ӧ�ı����Ϣ
     */
    public List<Table> tables();

    /**
     * ������ݿ������
     */
    public String databaseName();
    
    /**
     * ������ݿ������
     */
    public DATABASE_TYPE databaseType();

    /**
     * �������ݿ����Ϣ�ĸ��£�����ĸ�����һ�������¼��һ�飬���ϵ��ÿ��ܻ�Ӱ��Ч��
     */
    public void refreshTables();
    
    /**
     * �����ݿ���ǿ�н�����Ӧ�ı���������Ѿ����ھͽ���ɾ�����ؽ����������ʧ�ܷ���false�����򷵻�true
     */
    public boolean forceCreateTable(Table table);
    
    /**
     * �����ݿ��н�����������Ѿ������򷵻�false
     */
    public boolean createTable(Table table);
    
    /**
     * ����������
     * @param tables ��Ҫ�����ı�
     * @param isForce �����Ѿ����ڵı������ǲ���ɾ���ؽ����Ƿ�������
     */
    public boolean batchCreateTable(Iterable<Table> tables , boolean isForce);
    
    /**
     * �Ͽ����ݿ������
     */
    public void disconnect();
}




