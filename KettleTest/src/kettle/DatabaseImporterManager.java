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
        // �����Դ���ݿ���Ϣ
        private String sourceDatabase;
        // �����Ŀ�����ݿ���Ϣ
        private String destDatabase;
        
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

        ImportResult setSourceDatabasename(String sourceDatabase) 
        {
            this.sourceDatabase = sourceDatabase;
            return this;
        }

        ImportResult setDestDatabasename(String destDatabase) 
        {
            this.destDatabase = destDatabase;
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
         * ���Դ���ݿ���Ϣ
         */
        public String sourceDatabase()
        {
            return this.sourceDatabase;
        }
        
        /**
         * ���Ŀ�����ݿ���
         */
        public String destDatabase()
        {
            return this.destDatabase;
        }
        
        @Override
        public String toString()
        {
            // state , time , sourceDatabase , destDatabase
            StringBuilder builder = new StringBuilder();
            builder.append("{");
            builder.append(String.format("state:%s" , this.state.toString()));
            // ���ʧ����
            if (this.state == STATE.FAIL)
                builder.append("}");
            // ����ɹ��ˣ�����Ķ�����Ҫ
            else
            {
                builder.append(", ");
                builder.append(String.format("time:%f" , this.time));
                builder.append(", ");
                builder.append(String.format("source_db:%s" , this.sourceDatabase));
                builder.append(", ");
                builder.append(String.format("dest_db:%s" , this.destDatabase));
                builder.append("}");
            }
            return builder.toString();
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

