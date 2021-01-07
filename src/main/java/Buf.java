import sun.misc.Unsafe;

import java.lang.reflect.Field;

import static sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;

public class Buf {
    private static final Unsafe unsafe = getUnsafe();
    private final byte[] bytes;
    private int position;

    private static Unsafe getUnsafe() {
        try {
            Field unsafeField = Unsafe.class.getDeclaredField("theUnsafe");
            unsafeField.setAccessible(true);

            return (Unsafe) unsafeField.get(null);
        } catch (IllegalAccessException | NoSuchFieldException e) {
            throw new UnsupportedOperationException(e.getMessage(), e);
        }
    }

    public Buf(int initialCapacity) {
        bytes = new byte[initialCapacity];
    }

    public void put(byte[] source) {
        System.arraycopy(source, 0, bytes, position, source.length);
        position += source.length;
    }

    public byte[] array() {
        return bytes;
    }

    public int arrayOffset() {
        return 0;
    }

    public int position() {
        return position;
    }

    public int position(int value) {
        return position = value;
    }

    public void putShort(short value) {
        setShort(position, Short.reverseBytes(value));
        position += 2;
    }

    private void setShort(int position, short value) {
        unsafe.putShort(bytes, ARRAY_BYTE_BASE_OFFSET + position, value);
    }

    public void putInt(int value) {
        setInt(position, Integer.reverseBytes(value));
        position += 4;
    }

    private void setInt(int position, int value) {
        unsafe.putInt(bytes, ARRAY_BYTE_BASE_OFFSET + position, value);
    }

    public void putLong(long value) {
        setLong(position, Long.reverseBytes(value));
        position += 8;
    }

    private void setLong(int position, long value) {
        unsafe.putLong(bytes, ARRAY_BYTE_BASE_OFFSET + position, value);
    }
}
