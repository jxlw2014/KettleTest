package kettle;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;

import kettle.ImportResult.STATE;

import common.Pair;

import database.Database;

/**
 * ���ݿ⵼�빤�ߵ�ͨ��ʵ��
 */
public abstract class AbstractDatabaseImporterManager implements DatabaseImporterManager
{
    /**
     * �������е����ݿ�����
     */
    protected List<Pair<Database , Database>> connList = new ArrayList<Pair<Database , Database>>();
    /**
     * �ײ㱣�浼�빤�ߵ��б�
     */
    protected List<DatabaseImporter> importers = new ArrayList<DatabaseImporter>();
    
    /**
     * build���õĲ����Ƕ�ÿ�����ӶԽ���˳���������ǲ�������
     */
    @Override
    public void buildByConnPairs(Iterable<Pair<Database, Database>> pairs) 
    {
        this.connList.clear();
        this.importers.clear();
        
        for (Pair<Database , Database> pair : pairs)
            connList.add(pair);
        buildImporters();
            
        // ִ��ǰ�ĳ�ʼ��
        initBeforeExecute();
    }

    /**
     * build���õĲ����Ƕ�ÿ�����ӶԽ���˳���������ǲ�������
     */
    @Override
    public void buildByConnPairs(Pair<Database, Database>... pairs) 
    {
        this.connList.clear();
        this.importers.clear();
        
        for (Pair<Database , Database> pair : pairs)
            connList.add(pair);
        buildImporters();
            
        // ִ��ǰ�ĳ�ʼ��
        initBeforeExecute();
    } 
    
    @Override
    public boolean execute(boolean isAsync)
    {
        // ������첽
        if (isAsync)
        {
            List<Future<ImportResult>> results = executeAsync();
            for (Future<ImportResult> result : results)
            {
                try
                {
                    // ���ʧ����
                    if (result.get().state() == STATE.FAIL)
                        return false;
                } catch (Exception e)
                {
                    // �쳣Ҳ��ִ��ʧ����
                    return false;
                }
            }
            return true;
        }
        else
        {
            List<ImportResult> results = executeSequential();
            // �ж����еĽ��
            for (ImportResult result : results)
            {
                // �������һ������ʧ��
                if (result.state() == STATE.FAIL)
                    return false;
            }
            return true;
        }
    }
    
    /**
     * ִ��֮ǰ�ĳ�ʼ��
     */
    protected abstract void initBeforeExecute();
    
    /**
     * ��ÿ������pair����importer����
     */
    protected abstract void buildImporters();
    
}
