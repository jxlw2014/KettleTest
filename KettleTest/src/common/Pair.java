package common;

/**
 * pair
 */
public class Pair<T1 , T2> 
{
    /**
     * ��һ��Ԫ��
     */
    public final T1 first;
    /**
     * �ڶ���Ԫ��
     */
    public final T2 second;
    
    private Pair(T1 first , T2 second)
    {
        this.first = first;
        this.second = second;
    }
    
    /**
     * �õ�һ���µ�pair
     */
    public static <T1 , T2> Pair<T1 , T2> newPair(T1 first , T2 second)
    {
        return new Pair<T1 , T2>(first , second);
    }
}

