package common;

/**
 * pair
 */
public class Pair<T1 , T2> 
{
    /**
     * 第一个元素
     */
    public final T1 first;
    /**
     * 第二个元素
     */
    public final T2 second;
    
    private Pair(T1 first , T2 second)
    {
        this.first = first;
        this.second = second;
    }
    
    /**
     * 得到一个新的pair
     */
    public static <T1 , T2> Pair<T1 , T2> newPair(T1 first , T2 second)
    {
        return new Pair<T1 , T2>(first , second);
    }
}

