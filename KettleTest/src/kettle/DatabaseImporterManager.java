package kettle;

import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import common.Pair;

import database.Database;

/**
 * ��ȫ���빤�߹�����Ľӿ�
 */
public interface DatabaseImporterManager 
{
    /**
     * һ�ε���Ľ��
     */
    public static class ImportResult
    {
        /**
         * �����״̬
         */
        public enum STATE
        {
            SUCCESS , FAIL;
        }
        
        /**
         * ʧ�ܽ��ʹ�õ�ͳһ����
         */
        public static final ImportResult FAIL = new ImportResult().setState(STATE.FAIL);
        
        // ����״̬
        private STATE state;
        // ����ʱ��
        private double time;
        // �����Դ���ݿ���
        private String sourceDatabasename;
        // �����Ŀ�����ݿ���
        private String destDatabasename;
        
        private ImportResult() { }
        
        /**
         * ���һ���µĵ�����
         */
        static ImportResult newResult()
        {
            return new ImportResult();
        }

        ImportResult setState(STATE state)
        {
            this.state = state;
            return this;
        }

        ImportResult setTime(double time) 
        {
            this.time = time;
            return this;
        }

        ImportResult setSourceDatabasename(String sourceDatabasename) 
        {
            this.sourceDatabasename = sourceDatabasename;
            return this;
        }

        ImportResult setDestDatabasename(String destDatabasename) 
        {
            this.destDatabasename = destDatabasename;
            return this;
        }
        
        /**
         * ��õ����״̬
         */
        public STATE state()
        {
            return this.state;
        }
        
        /**
         * ��õ�����Ҫ��ʱ��
         */
        public double time()
        {
            return this.time;
        }
        
        /**
         * ���Դ���ݿ���
         */
        public String sourceDatabasename()
        {
            return this.sourceDatabasename;
        }
        
        /**
         * ���Ŀ�����ݿ���
         */
        public String destDatabasename()
        {
            return this.destDatabasename;
        }
    }
    
    /**
     * �ø����Ķ�����ݿ����Ӷ�����ʼ��
     */
    public void build(Iterable<Pair<Database , Database>> pairs);
   
    /**
     * �ø����Ķ�����ݿ����Ӷ�����ʼ��
     */
    public void build(Pair<Database , Database>... pairs);
    
    /**
     * ִ�е��룬�����Ƿ����еĵ��붼�ɹ��ˣ�������ɹ�����true�����򷵻�false��ִ��ǰ����build������������Ԥ�ơ������м�ĳ�������ʧ�ܲ�����Ӱ��ȫ������ȫ��ִ��һ��
     * @param isAsnyc �Ƿ����첽�ķ�ʽ����ִ��
     */
    public boolean execute(boolean isAsync);
    
    /**
     * ִ���������ݿ�Եĵ��룬ִ��˳��Ϊ����ִ�С�ִ��ǰ����build������������Ԥ�ơ������м�ĳ�������ʧ�ܲ�����Ӱ��ȫ������ȫ��ִ��һ��
     */
    public List<ImportResult> executeSequential();
    
    /**
     * �첽ִ�е��룬���е����ݿ�Ե��벢��ִ�С���ʱ��Ҫע�����һ�����ݿ�򿪵����ӹ��࣬��Ϊ���еĵ�����Ҫ�����Ӷ���򿪣������߳�������Ҳ����ࡣ���Խ��鲻Ҫͬʱִ�й�������ݿ⵼�������ִ��ǰ����build������������Ԥ�ơ�
     * �����м�ĳ�������ʧ�ܲ�����Ӱ��ȫ������ȫ��ִ��һ��
     */
    public List<Future<ImportResult>> executeAsync();
    
    /**
     * ��ʱִ�����е�importer�����ĳ��importer��ʼ��ʧ�ܻ���ִ��ʧ�ܶ�����Ӱ�춨ʱִ��
     */
    public void timingExecute(long time , TimeUnit unit);
    
    /**
     * ֹͣ�������
     */
    public void shutdown();

}

