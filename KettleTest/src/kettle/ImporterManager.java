package kettle;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import kettle.DatabaseImporterManager.ImportResult.STATE;
import util.KettleUtil.DatabaseImporterSetting;
import util.Stopwatch;

import common.Pair;

import database.Database;


/**
 * ���������ݿ⵼��Ĺ�����
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
                Stopwatch watch = Stopwatch.newWatch();
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
    // TODO ��ͳһΪһ����û��Ƿֳ��������������
    private final MANAGER_TYPE type;
    
    /**
     * ���ݿ⵼������ö���
     */
    private DatabaseImporterSetting setting;
    
    // ���о��崦��ı�
    private Set<String> tables;
    // -1��ʾ������included��1��ʾ������excluded��0��ʾû�н�������
    private AtomicInteger setFlag = new AtomicInteger(0);
    
    private ImporterManager(MANAGER_TYPE type) 
    {
        this.type = type;
    }
    
    /**
     * �õ��µ����ݵ���Ĺ������
     * @param setting �����������
     */
    public static ImporterManager newDataImportManager(DatabaseImporterSetting setting)
    {
        ImporterManager manager = new ImporterManager(MANAGER_TYPE.IMPORT);
        manager.setting = setting;
        return manager;
    }
    
    /**
     * �õ��µ�����ͬ���Ĺ������
     * @param setting ͬ����������
     */
    public static ImporterManager newDataSynManager(DatabaseImporterSetting setting)
    {
        ImporterManager manager = new ImporterManager(MANAGER_TYPE.SYN);
        manager.setting = setting;
        return manager;
    }
    
    @Override
    public void setIncludedTables(Iterable<String> tables)
    {   
        if (setFlag.get() == 0)
        {
            setFlag.decrementAndGet();
            this.tables = new HashSet<String>();
            for (String str : tables)
                this.tables.add(str.toUpperCase());
        }
    }
    
    @Override
    public void setExcluedTables(Iterable<String> tables)
    {
        if (setFlag.get() == 0)
        {
            setFlag.incrementAndGet();
            this.tables = new HashSet<String>();
            for (String str : tables)
                this.tables.add(str.toUpperCase());
        }
    }
    
    @Override
    public void buildByImporters(Iterable<DatabaseImporter> importers)
    {
        this.connList.clear();
        this.importers.clear();
        
        // �ж����е����ݿ⵼�����
        for (DatabaseImporter importer : importers)
        {
            // ����ǺϷ��ĵ������
            if (importer != null)
            {
                Pair<Database , Database> conn = importer.getConnPair();
                // ��������Ѿ��ɹ�build��
                if (conn != null)
                {
                    // �жϹ����������
                    switch (this.type)
                    {
                        case IMPORT:
                            // �������
                            if (importer instanceof DataImporter)
                                this.connList.add(conn);
                                this.importers.add(importer);
                                
                        case SYN:
                            // �������
                            if (importer instanceof TimingDataSynchronization)
                                this.connList.add(conn);
                                this.importers.add(importer);
                    }
                }
            }
        }
    }

    @Override
    public List<ImportResult> executeSequential() 
    {
        // ִ��֮ǰ�ĳ�ʼ������
        initBeforeExecute();
        
        System.out.println("Start import sequential...");
        
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
                Stopwatch watch = Stopwatch.newWatch();
                watch.start();
                if (importer.execute())
                {
                    ImportResult result = ImportResult.newResult();
                    result.setSourceDatabasename(pair.first.toString())
                          .setDestDatabasename(pair.second.toString())
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
        // ִ��֮ǰ�ĳ�ʼ������
        initBeforeExecute();
        
        System.out.println("Start import async...");
        
        List<Future<ImportResult>> results = new ArrayList<Future<ImportResult>>();
        asyncService = Executors.newCachedThreadPool();
        // ���еĵ�����
        int index = 0;
        // �������еĵ������
        for (DatabaseImporter importer : this.importers)
        {
            Pair<Database , Database> pair = this.connList.get(index);
            ImportCallable callable = new ImportCallable(importer , pair.first.toString() , pair.second.toString());
            Future<ImportResult> result = asyncService.submit(callable);
            results.add(result);
            index ++;
        }
        return results;
    }
    
    @Override
    public void timingExecute(long time, TimeUnit unit) 
    {
        // ִ��֮ǰ�ĳ�ʼ������
        initBeforeExecute();
        
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
     * ִ��֮ǰ�Ĳ���
     */
    @Override
    protected void initBeforeExecute()
    {
        if (setFlag.get() != 0)
        {
            // init the importers��according to the value of setFlag
            for (DatabaseImporter importer : this.importers)
            {
                if (setFlag.get() < 0)
                    importer.setIncludedTables(this.tables);
                else
                    importer.setExcludedTables(this.tables);
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
                DataImporter importer = DataImporter.newImporter();
                importer.setSetting(setting);
                return importer;
                
            case SYN:
                TimingDataSynchronization syn = TimingDataSynchronization.newInstance();
                syn.setSetting(setting);
                return syn;
                
            default:
                return null;
        }
    }
    
}


