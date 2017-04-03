package algos.utils;

import java.lang.reflect.Array;
import java.util.function.Supplier;
import java.util.stream.IntStream;

/**
 * Created by volodymyr.bakhmatiuk on 3/30/17.
 */
public class InMemoryDatasetUtils {

    public static <T> T[] generateSimilarArray(Class<T> c, T[] origin, int size, Supplier<T> differentSupplier, float similarity) {
        if (similarity > 1.0 || similarity < 0) throw new IllegalArgumentException("Wrong similarity argument");
        //fill array with duplicates
        final int duplicatesAmount = Math.round(size * similarity);
        T[] copy = (T[]) Array.newInstance(c, size);
        System.arraycopy(origin, 0, copy, 0, Math.min(duplicatesAmount, origin.length));

        // overblow array with duplicates if it's impossible to take needed amount from origin
        for (int pointer = origin.length; duplicatesAmount > pointer; pointer += origin.length) {
            System.arraycopy(copy, 0, copy, pointer, Math.min(duplicatesAmount - pointer, origin.length));
        }

        //fill array with unique values. At this point array should have {@code duplicatesAmount} elemets from origin array, other should be non initialized
        T[] uniqueValues = generateArray(c, differentSupplier, size - duplicatesAmount);
        System.arraycopy(uniqueValues, 0, copy, duplicatesAmount, uniqueValues.length);

        return copy;
    }

    public static <T> T[] generateArray(Class<T> c, Supplier<T> supplier, int size) {
        return IntStream
                .range(0, size)
                .boxed()
                .map(i -> supplier.get())
                .toArray(i -> (T[]) Array.newInstance(c, size));
    }
}
