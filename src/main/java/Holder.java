
public class Holder<T extends Comparable<T>> {
    private final Class<T> clazz;
    private final T value;

    public Holder(Class<T> clazz, T value) {

        this.clazz = clazz;
        this.value = value;
    }

    public Class<T> clazz() {
        return clazz;
    }

    public T value() {
        return value;
    }
}
