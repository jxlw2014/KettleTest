package common;

/**
 * һ��Ԫ��
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
    public static <V1 , V2> Pair<V1 , V2> newPair(V1 first , V2 second)
    {
        return new Pair<V1 , V2>(first , second);
    }
    
    @Override
    public int hashCode()
    {
        int result = 37;
        result = result * 17 + first.hashCode();
        result = result * 17 + second.hashCode();
        return result;
    }
    
    @Override
    public boolean equals(Object obj)
    {
        if (obj == null)
            return false;
        else
        {
            // �����һ��pair
            if (obj instanceof Pair)
            {
                @SuppressWarnings("unchecked")
                Pair<T1 , T2> pair = (Pair<T1 , T2>) obj;
                if (this.first.equals(pair.first) && this.second.equals(pair.second))
                    return true;
                else
                    return false;
            }
            else
                return false;
        }
    }

    /**
     * (first, second)
     */
    @Override
    public String toString()
    {
        return String.format("(%s, %s)" , this.first.toString() , this.second.toString());
    }
    
}

