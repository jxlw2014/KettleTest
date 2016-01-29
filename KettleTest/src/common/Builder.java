package common;

/**
 * 类支持构造对象的操作
 */
public interface Builder<T>
{
    /**
     * 获得对应类型的一个对象
     */
    public T build();
}
