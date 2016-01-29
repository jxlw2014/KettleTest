package kettle;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import kettle.ImportResult.STATE;

import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;

import util.DatabaseUtil;
import util.KettleUtil;
import util.KettleUtil.TableImportSetting;
import util.Stopwatch;
import database.Database;
import database.Table;

/**
 * ��Ա�����ݵ�����
 */
public class TableDataImporter implements TableImporter
{
    private TableDataImporter() { }
    
    /**
     * ���һ���µĵ������
     */
    public static TableDataImporter newImporter()
    {
        return new TableDataImporter();
    }
    
    // Դ��Ŀ�ı��Լ�����Ĳ���
    private Database source;
    private Database dest;
    private Table sourceTable;
    private Table destTable;
    private IMPORT_STRATEGY strategy;
    
    // ����Ĳ�������
    private TableImportSetting setting = TableImportSetting.DEFAULT;
    
    // ֧���첽ִ�У����̱߳�֤�����ȷ���
    private ExecutorService asyncExecutor = Executors.newSingleThreadExecutor();
    
    @Override
    public boolean build(Database source, Table sourceTable, Database dest , IMPORT_STRATEGY strategy) 
    {
        this.source = source;
        this.dest = dest;
        this.sourceTable = sourceTable;
        this.destTable = DatabaseUtil.transformTable(source.databaseType() , dest.databaseType() , sourceTable);
        this.strategy = strategy;
        return true;
    }

    @Override
    public boolean execute() 
    {
        // �жϵ���Ĳ���
        switch (strategy)
        {
            case DIRECT_IMPORT:
                return executeImp();
                
            case IMPORT_IF_EXIST:
                if (dest.containsTable(destTable.getTableName()))
                    return executeImp();
                else
                    return false;
                
            case IMPORT_AFTER_DELETE:
                // ������ڱ���ɾ��
                if (dest.containsTable(destTable.getTableName()))
                    dest.dropTable(destTable.getTableName());
                dest.createTable(destTable);
                return executeImp();
                
            default:
                return false;
        }
    }

    @Override
    public Future<ImportResult> executeAsync() 
    {
        return asyncExecutor.submit(new Callable<ImportResult>()
        {
            @Override
            public ImportResult call() throws Exception 
            {
                Stopwatch watch = Stopwatch.newWatch();
                watch.start();
                // ���ִ�гɹ�
                if (executeImp())
                    return ImportResult.newResult()
                                       .setTime(watch.stop())
                                       .setState(STATE.SUCCESS)
                                       .setSourceDatabase(source.toString())
                                       .setDestDatabase(dest.toString());
                else
                    return ImportResult.FAIL;
            }
        });
    }
    
    @Override
    public void setSetting(TableImportSetting setting)
    {
        this.setting = setting;
    }
    
    @Override
    public void shutdown()
    {
        if (asyncExecutor != null)
            asyncExecutor.shutdown();
    }
    
    // ������kettle����ľ���ʵ��
    private boolean executeImp()
    {
        TransMeta transMeta = new TransMeta();
        KettleUtil.addImportComponent(transMeta , source , sourceTable , dest , destTable , setting , 1);
        Trans trans = new Trans(transMeta);
        try
        {
            trans.prepareExecution(null);
            trans.startThreads();
            trans.waitUntilFinished();
            return true;
            
        } catch (Exception e)
        {
            return false;
        }
    }

}
