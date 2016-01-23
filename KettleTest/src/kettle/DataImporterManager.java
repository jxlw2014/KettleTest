package kettle;

import java.util.List;
import java.util.concurrent.Future;

import common.Pair;

import database.Database;

/**
 * 处理多个数据库导入的管理类
 */
public class DataImporterManager implements DatabaseImporterManager
{
    @Override
    public void build(Iterable<Pair<Database, Database>> pairs) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void build(Pair<Database, Database>... pairs) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public boolean execute(boolean isAsnyc) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public List<ImportResult> executeSequential() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public List<Future<ImportResult>> executeAsyc() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void shutdown() {
        // TODO Auto-generated method stub
        
    }
}
