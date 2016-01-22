package kettle;

import java.util.List;

import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;

import util.DatabaseUtil;
import util.KettleUtil;
import util.KettleUtil.ImportSetting;
import database.Database;
import database.Table;

/**
 * ���ݵ����࣬ʵ�ֽ�һ�����ݿ��е����б����ݵ��뵽��һ�����ݿ��еĹ��ܣ��ײ�ʹ��kettle������ʵ��
 */
public class DataImporter implements EntireImporter 
{
    /**
     * ����״̬
     */
    public enum STATE
    {
        NEW , BUILD , EXECUTE;
    }
    
    // Դ���ݿ��Ŀ�����ݿ�
    private Database source , dest;
    
    // ��ʼ״̬��new
    private STATE state = STATE.NEW;
    
    private DataImporter() { }
    
    /**
     * �½�һ�����ݵ������
     */
    public static DataImporter newImporter()
    {
        return new DataImporter();
    }
    
    @Override
    public void build(Database source , Database dest)
    {
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
            }   
            else
                System.out.println("copy scheme fail...");
        }
        else
            System.out.println("You should finish executing first...");
    }   
    
    @Override
    public void execute()
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
                int batchSize = 20 , cur = 0;
                // ���tables
                List<Table> tableList = source.tables();
                while (cur < tableList.size())
                {
                    int cnt = 1;
                    TransMeta transMeta = new TransMeta();
                    Trans trans = null;
                    
                    // ������batchSize����Ҳ����û����ô��
                    while (cur < tableList.size() && cnt <= batchSize)
                    {
                        Table sourceTable = tableList.get(cur);
                        Table destTable = DatabaseUtil.transformTable(source.databaseType() , dest.databaseType() , sourceTable);
                        KettleUtil.addImportComponent(transMeta , source , sourceTable , dest , destTable , ImportSetting.DEFAULT , cnt);
                        cur ++;
                        cnt ++;
                    }
                    
                    trans = new Trans(transMeta);
                    // ִ��ת��
                    trans.prepareExecution(null);
                    trans.startThreads();
                    trans.waitUntilFinished();
                }
                
                long execTime = System.currentTimeMillis() - curTime;
                System.out.println(String.format("The import time is %fs" , ((double) execTime / 1000)));
                
            } catch (Exception e)
            {
                System.out.println("import fail...");
            }

            // ״̬�ص���ʼ
            state = STATE.NEW;
        }
        else
            System.out.println("You need build before execute...");
    }

}
