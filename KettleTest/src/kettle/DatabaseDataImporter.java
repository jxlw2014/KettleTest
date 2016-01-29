package kettle;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;

import util.DatabaseUtil;
import util.KettleUtil;
import util.KettleUtil.TableImportSetting;

import common.Pair;

import database.Database;
import database.Table;
import env.Constants;

/**
 * ���ݵ����࣬ʵ�ֽ�һ�����ݿ��е����б����ݵ��뵽��һ�����ݿ��еĹ��ܡ����õĵ�������ǣ����ȸ��Ʊ�Ľṹ�����Ŀ�����ݿ���
 * ��ͬ�����ͽ����ؽ���Ȼ��������ݵĵ���
 */
public class DatabaseDataImporter implements DatabaseImporter , TimingImporter 
{
    /**
     * ����״̬
     */
    public enum STATE
    {
        NEW , BUILD , EXECUTE;
    }
    
    // ����import������batch��С
    private int batchSize = Constants.DEFAULT_BATCH_SIZE;
    
    // Դ���ݿ��Ŀ�����ݿ�
    private Database source = null;
    private Database dest = null;
    
    // ��ʼ״̬��new
    private STATE state = STATE.NEW;
    
    // ��������
    private TableImportSetting setting = TableImportSetting.DEFAULT;
    
    // �ж��Ƿ�shutdown
    private AtomicBoolean isShutdown = new AtomicBoolean(false);
    
    // ����table����Ϊ����ʱ��ǰ������
    private AtomicInteger setFlag = new AtomicInteger(0);
    private Set<String> tables;
    
    // ֧�ֶ�ʱִ�в���
    private AtomicBoolean timingIsSet = new AtomicBoolean(false);
    private ScheduledExecutorService schedule = null;
    
    private DatabaseDataImporter() { }
    
    /**
     * �½�һ�����ݵ������
     */
    public static DatabaseDataImporter newImporter()
    {
        return new DatabaseDataImporter();
    }
    
    @Override
    public void setSetting(TableImportSetting setting)
    {
        this.setting = setting;
    }
    
    @Override
    public void setIncludedTables(Iterable<String> tables) 
    {
        // ���û�н��й�����
        if (this.setFlag.get() == 0)
        {
            this.setFlag.decrementAndGet();
            this.tables = new HashSet<String>();
            for (String table : tables)
                this.tables.add(table.toUpperCase());
        }
    }

    @Override
    public void setExcludedTables(Iterable<String> tables) 
    {
        // ���û�н��й�����
        if (this.setFlag.get() == 0)
        {
            this.setFlag.incrementAndGet();
            this.tables = new HashSet<String>();
            for (String table : tables)
                this.tables.add(table.toUpperCase());
        }
    }
    
    @Override
    public boolean build(Database source , Database dest)
    {
        // Ĭ�ϵ�����
        this.isShutdown.set(false);
        System.out.println("Start build the necessary things for transformation...");
        // ����ǳ�ʼ״̬����build��
        if (state == STATE.NEW || state == STATE.BUILD)
        {
            // ������Ʊ�ɹ�
            if (DatabaseUtil.copyScheme(source , dest))
            {
                System.out.println("copy scheme success...");
                // �ı�״̬
                state = STATE.BUILD;
                // ��������Դ������Ŀ�ĵ�
                this.source = source;
                this.dest = dest;
                return true;
            }   
            else
            {
                System.out.println("copy scheme fail...");
                return false;
            }
        }
        else
        {
            System.out.println("You should finish executing first...");
            return false;
        }
    }   
    
    @Override
    public Pair<Database , Database> getConnPair()
    {
        // ���û�гɹ�build
        if (this.source == null || this.dest == null)
            return null;
        else
            return Pair.newPair(this.source , this.dest);
    }
    
    @Override
    public boolean execute()
    {
        // ����Ѿ�BUILD��
        if (state == STATE.BUILD)
        {
            // �ı�״̬Ϊִ��
            state = STATE.EXECUTE;
            
            try
            {
                long curTime = System.currentTimeMillis();
                System.out.println("Start import...");
             
                // ����ִ��ת��
                int cur = 0;
                // ���tables
                List<Table> tableList = source.tables();
                while (cur < tableList.size() && !isShutdown.get())
                {
                    int cnt = 1;
                    TransMeta transMeta = new TransMeta();
                    Trans trans = null;
                    
                    // ������batchSize����Ҳ����û����ô��
                    while (cur < tableList.size() && cnt <= batchSize)
                    {
                        Table sourceTable = tableList.get(cur);
                        // �����Ҫ���е���
                        if (check(sourceTable))
                        {
                            Table destTable = DatabaseUtil.transformTable(source.databaseType() , dest.databaseType() , sourceTable);
                            KettleUtil.addImportComponent(transMeta , source , sourceTable , dest , destTable , this.setting , cnt);
                            cnt ++;
                        }
                        cur ++;
                    }
                    
                    trans = new Trans(transMeta);
                    // ִ��ת��
                    trans.prepareExecution(null);
                    trans.startThreads();
                    trans.waitUntilFinished();
                    
                    transMeta = null;
                    trans = null;
                    
                    System.out.println(String.format("Import %d table success..." , cur));
                }
                
                long execTime = System.currentTimeMillis() - curTime;
                System.out.println(String.format("The import time is %fs" , ((double) execTime / 1000)));
                
            } catch (Exception e)
            {
                System.out.println("import fail...");
                return false;
            }

            // ״̬�ص���ʼ
            state = STATE.NEW;
            return true;
        }
        else
        {
            System.out.println("You need build before execute...");
            return false;
        }
    }
    
    /**
     * execute֮ǰ��Ҫִ��build������������Ԥ�����������ﶨʱִ��execute��ʵҲ��һ��ͬ���ķ�ʽ����Ϊÿ�ζ���ɾ���ؽ�
     */
    @Override
    public void timingExecute(long time , TimeUnit timeUnit)
    {
        // ���û�����ö�ʱ����
        if (!timingIsSet.get())
        {
            timingIsSet.set(true);
            schedule = Executors.newSingleThreadScheduledExecutor();
            schedule.scheduleAtFixedRate(new Runnable()
            {
                @Override
                public void run() 
                {
                    execute();
                }
            } , 0 , time , timeUnit);
        }
    }   

    @Override
    public void setBatchSize(int batchSize) 
    {
        this.batchSize = batchSize;
    }
    
    @Override
    public void shutdown()
    {
        this.isShutdown.set(true);
        this.timingIsSet.set(false);
        // �����Ϊnull
        if (this.schedule != null)
        {
            this.schedule.shutdown();
            this.schedule = null;
        }
    }
    
    /**
     * �жϱ��Ƿ���Ҫ����
     */
    private boolean check(Table table)
    {
        // ���û�н�������
        if (this.setFlag.get() == 0)
            return true;
        else
        {
            if (this.setFlag.get() < 0 && this.tables.contains(table.getTableName()))
                return true;
            else if (this.setFlag.get() > 0 && !this.tables.contains(table.getTableName()))
                return true;
            else
                return false;
        }
    }

}



