package util;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import common.Pair;

/**
 * pair�Ĺ�����
 */
public final class Pairs 
{
    /**
     * �򵥵Ļ�������ĵ�����ʵ�֣�û�б�Ҫʹ��List������������������
     */
    private static class SimpleIterable<T1 , T2> implements Iterable<Pair<T1 , T2>>
    {
        // �ײ�洢����
        private Object[] ary;
        private int next = 0 , size;
        
        /**
         * ���ԷŶ��ٸ�pair
         */
        SimpleIterable(int size)
        {
            ary = new Object[size * 2];
            this.size = size * 2;
        }
        
        SimpleIterable<T1 , T2> addPair(T1 e1 , T2 e2)
        {
            // ������Լ���
            if (next + 1 < size)
            {
                ary[next] = e1;
                ary[next + 1] = e2;
                next += 2;
            }
            return this;
        }
        
        @Override
        public Iterator<Pair<T1 , T2>> iterator() 
        {
            return new Iterator<Pair<T1 , T2>>()
            {
                private int cur = 0;
                
                @Override
                public boolean hasNext() 
                {
                    if (cur + 1 < next)
                        return true;
                    else
                        return false;
                }

                @Override
                public Pair<T1, T2> next() 
                {
                    @SuppressWarnings("unchecked")
                    Pair<T1 , T2> pair = (Pair<T1 , T2>) Pair.newPair(ary[cur] , ary[cur + 1]);
                    cur += 2;
                    return pair;
                }

                @Override
                public void remove() 
                {
                    throw new UnsupportedOperationException("��֧��remove����");
                }
            };
        }
    }
    
    /**
     * ���һ��pair��Ӧ��Iterable
     */
    public static <T1 , T2> Iterable<Pair<T1 , T2>> toPair(T1 e1 , T2 e2)
    {
        return new SimpleIterable<T1, T2>(1).addPair(e1 , e2);
    }
    
    /**
     * ���pairs��Ӧ�ĵ�Iterable
     */
    public static <T1 , T2> Iterable<Pair<T1 , T2>> toPairs(T1 e1 , T2 e2 , T1 e3 , T2 e4)
    {
        return new SimpleIterable<T1, T2>(2).addPair(e1 , e2).addPair(e3 , e4);
    }

    /**
     * ���pairs��Ӧ�ĵ�Iterable
     */ 
    public static <T1 , T2> Iterable<Pair<T1 , T2>> toPairs(T1 e1 , T2 e2 , T1 e3 , T2 e4 , T1 e5 , T2 e6)
    {
        return new SimpleIterable<T1 , T2>(3).addPair(e1 , e2).addPair(e3 , e4).addPair(e5 , e6);
    }
    
    /**
     * pair�ĵ�һ��Ԫ�غ͵ڶ���Ԫ����Iterable���������������Ԫ�ظ�����һ�£�ȡǰ��һ�µ��ǲ���
     */
    public static <T1 , T2> Iterable<Pair<T1 , T2>> toPairs(Iterable<T1> first , Iterable<T2> second)
    {
        Iterator<T1> iterator1 = first.iterator();
        Iterator<T2> iterator2 = second.iterator();
        // ���Ȳ�ȷ�����Լ�ʵ�ֱ䳤��û��̫��������ˣ�����list
        List<Pair<T1 , T2>> list = new ArrayList<Pair<T1 , T2>>();
        while (iterator1.hasNext() && iterator2.hasNext())
        {
            T1 t1 = iterator1.next();
            T2 t2 = iterator2.next();
            list.add(Pair.newPair(t1 , t2));
        }
        return list;
    }
}

