package database;

import java.util.List;

/**
 * SQLServer的对应实现
 */
public class SQLServerDatabase extends AbstractDatabase 
{
    
    @Override
    public List<Table> tables() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void refreshTables() {
        // TODO Auto-generated method stub
        
    }

    @Override
    public boolean forceCreateTable(Table table) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean createTable(Table table) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean batchCreateTable(Iterable<Table> tables, boolean isForce) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public void disconnect() {
        // TODO Auto-generated method stub
        
    }
    
}
