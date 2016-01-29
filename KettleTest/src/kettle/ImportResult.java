package kettle;

/**
 * һ�ε�����ִ�н��
 */
public class ImportResult
{
    /**
     * �����״̬
     */
    public enum STATE
    {
        SUCCESS , 
        FAIL;
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

    ImportResult setSourceDatabase(String sourceDatabase) 
    {
        this.sourceDatabase = sourceDatabase;
        return this;
    }

    ImportResult setDestDatabase(String destDatabase) 
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
     * ���Ŀ�����ݿ���Ϣ
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
