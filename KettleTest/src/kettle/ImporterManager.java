package kettle;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import kettle.DatabaseImporterManager.ImportResult.STATE;
import util.Stopwatch;

import common.Pair;

import database.Database;


/**
 * ����������ݿ⵼��Ĺ�����
 */
public class ImporterManager extends AbstractDatabaseImporterManager
{
    /**
     * ������첽����
     */
    private static class ImportCallable implements Callable<ImportResult>
    {
        private DatabaseImporter importer = null;
        private String sourceDatabase , destDatabase;
        
        ImportCallable(DatabaseImporter importer , String sourceDatabase , String destDatabase)
        {
            this.importer = importer;
            this.sourceDatabase = sourceDatabase;
            this.destDatabase = destDatabase;
        }

        @Override
        public ImportResult call()
        {
            // ������빤�߳�ʼ��ʧ��
            if (this.importer == null)
                return ImportResult.FAIL;
            else
            {
                Stopwatch watch = Stopwatch.getWatch();
                watch.start();
                // �������ʧ��
                if (!importer.execute())
                    return ImportResult.FAIL;
                // �������ɹ�
                else
                {
                    ImportResult result = ImportResult.newResult();
                    result.setTime(watch.stop())
                          .setSourceDatabasename(this.sourceDatabase)
                          .setDestDatabasename(this.destDatabase)
                          .setState(STATE.SUCCESS);
                    return result;
                }
            }
        }
    }
    
    /**
     * manager������
     */
    public enum MANAGER_TYPE
    {
        /**
         * ������һ�ε����ݵ���
         */
        IMPORT , 
        /**
         * �����������ݵ�ͬ��
         */
        SYN
    }
    
    /**
     * Ϊ��֧���첽����
     */
    private ExecutorService asyncService = null;
    /**
     * Ϊ��֧�ֶ�ʱִ��
     */
    private ScheduledExecutorService timingService = null;
    /**
     * �Ƿ�ʼtiming
     */
    private AtomicBoolean timingIsStart = new AtomicBoolean(false);
    
    /**
     * �����������
     */
    private final MANAGER_TYPE type;
    
    private ImporterManager(MANAGER_TYPE type) 
    {
        this.type = type;
    }
    
    /**
     * �õ��µ�manager��������ֻ��������һ��
     */
    public static ImporterManager newMananger(MANAGER_TYPE type)
    {
        return new ImporterManager(type);
    }

    @Override
    public List<ImportResult> executeSequential() 
    {
        List<ImportResult> results = new ArrayList<ImportResult>();
        int index = 0;
        // ���δ������е�importer
        for (DatabaseImporter importer : this.importers)
        {
            // ������빤�߳�ʼ��ʧ����
            if (importer == null)
                results.add(ImportResult.FAIL);
            else
            {
                Pair<Database , Database> pair = this.connList.get(index);
                // ���result�е�ʱ��
                Stopwatch watch = Stopwatch.getWatch();
                watch.start();
                if (importer.execute())
                {
                    ImportResult result = ImportResult.newResult();
                    result.setSourceDatabasename(pair.first.databaseName())
                          .setDestDatabasename(pair.second.databaseName())
                          .setState(STATE.SUCCESS)
                          .setTime(watch.stop());
                    results.add(result);
                }
                else
                    results.add(ImportResult.FAIL);
            }
            index ++;
        }
        return results;
    }

    @Override
    public List<Future<ImportResult>> executeAsync()
    {
        List<Future<ImportResult>> results = new ArrayList<Future<ImportResult>>();
        asyncService = Executors.newCachedThreadPool();
        // ���еĵ�����
        int index = 0;
        // �������еĵ������
        for (DatabaseImporter importer : this.importers)
        {
            Pair<Database , Database> pair = this.connList.get(index);
            ImportCallable callable = new ImportCallable(importer , pair.first.databaseName() , pair.second.databaseName());
            Future<ImportResult> result = asyncService.submit(callable);
            results.add(result);
            index ++;
        }
        return results;
    }
    
    @Override
    public void timingExecute(long time, TimeUnit unit) 
    {
        // ����ǵ�һ��ִ��
        if (!timingIsStart.get())
        {   
            timingIsStart.set(true);
            // ����ִ������
            timingService.scheduleAtFixedRate(new Runnable()
            {
                @Override
                public void run()
                {
                    // �ж����е�importer
                    for (DatabaseImporter importer : importers)
                    {
                        // �����Ϊnull
                        if (importer != null)
                            importer.execute();
                    }
                }
            } , 0 , time , unit);
        }
    }

    @Override
    public void shutdown() 
    {
        // ���еĵ��빤�߽���shutdown
        for (DatabaseImporter importer : this.importers)
            importer.shutdown();
        // ͣ���̳߳�
        if (asyncService != null)
            asyncService.shutdown();
        if (timingService != null)
            timingService.shutdown();
    }

    @Override
    protected void buildImporters() 
    {
        // ��ÿ�����ݿ����ӵõ���Ӧ�ĵ����� 
        for (Pair<Database , Database> pair : this.connList)
        {
            // �����Ҫ��importer
            DatabaseImporter importer = getImporter();
            // ���importer��ȡʧ��
            if (importer == null)
                this.importers.add(null);
            else
            {
                // ���buildʧ���ˣ��Ÿ�nullռ��λ��
                if (importer.build(pair.first , pair.second))
                    this.importers.add(importer);
                else
                    this.importers.add(null);
            }
        }
    }
    
    /**
     * ����ManagerType��ö�Ӧ��importer
     */
    private DatabaseImporter getImporter()
    {
        // �����Ҫ��importer
        switch (this.type)
        {
            case IMPORT:
                return DataImporter.newImporter();
            case SYN:
                return TimingDataSynchronization.newInstance();
            default:
                return null;
        }
    }
    
}

