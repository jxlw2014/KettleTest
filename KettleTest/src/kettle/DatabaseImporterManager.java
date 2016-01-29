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
     * �ø����Ķ�����ݿ����Ӷ�����ʼ��
     */
    public void buildByConnPairs(Iterable<Pair<Database , Database>> pairs);
   
    /**
     * �ø����Ķ�����ݿ����Ӷ�����ʼ��
     */
    public void buildByConnPairs(Pair<Database , Database>... pairs);
    
    /**
     * ֱ������importers������build�ĳ�ʼ���������importer��Ҫ���й�build����Ȼ�޷����е���
     */
    public void buildByImporters(Iterable<DatabaseImporter> importers);
    
    /**
     * ִ�е��룬�����Ƿ����еĵ��붼�ɹ��ˣ�������ɹ�����true�����򷵻�false��ִ��ǰ����build������������Ԥ�ơ������м�ĳ�������ʧ�ܲ�����Ӱ��ȫ������ȫ��ִ��һ��
     * @param  isAsnyc �Ƿ����첽�ķ�ʽ����ִ��
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
     * ���е���ı�����ƣ�������������excludedTables�Ͳ��ܹ��������ã�ֻ������һ�Ρ����������ָ��������Դ���е����ƣ�ע�ⲻͬ�����ݿ�Դ������ͬ�ı�����������������ܻ����������ͬʱ�������롣
     * importer��������Ѿ������˱�������ö��ڸ�importer�ͱ����Ч�ˡ�ִ����Ҫ��build֮ǰ��������Ч
     */
    public void setIncludedTables(Iterable<String> tables);
    
    /**
     * �����е���ı�����ƣ�������������includedTables�Ͳ��ܹ��������ã�ֻ������һ�Ρ����������ָ��������Դ���е����ƣ�ע�ⲻͬ�����ݿ�Դ������ͬ�ı�����������������ܻ����������ͬʱ�������롣
     * importer��������Ѿ������˱�������ö��ڸ�importer�ͱ����Ч�ˡ�ִ����Ҫ��build֮ǰ��������Ч
     */
    public void setExcluedTables(Iterable<String> tables);
    
    /**
     * ֹͣ�������
     */
    public void shutdown();

}

