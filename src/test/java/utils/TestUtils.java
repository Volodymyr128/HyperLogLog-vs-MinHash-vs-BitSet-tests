package utils;

import java.math.BigDecimal;
import java.util.function.Supplier;
import java.util.stream.IntStream;


/**
 * Created by volodymyr.bakhmatiuk on 3/29/17.
 */
public class TestUtils {

    public static float round(float num, int precision) {
        BigDecimal bd = new BigDecimal(Float.toString(num));
        bd = bd.setScale(precision, BigDecimal.ROUND_HALF_UP);
        return bd.floatValue();
    }

    public static float getDeviation(float expectedResult, float actualResult) {
        return Math.abs(expectedResult - actualResult);
    }

    public static float getDeviation(float expectedResult, float actualResult, int precision) {
        return round(Math.abs(expectedResult - actualResult), precision);
    }

    public static Supplier<String> getSupplier(int from, int size, String prefix) {
        return IntStream.range(from, from + size).boxed().map(i -> prefix + i).iterator()::next;
    }

    public static DuplicateSupplier<String> getDuplicateSupplier(int from, int size, float duplicatesPercentage, String prefix) {
        return new DuplicateSupplier<>(getSupplier(from, from + size, prefix), duplicatesPercentage);
    }

    public static Supplier<String> getDuplicateSupplier(float duplicatesPercentage, String prefix) {
        Supplier<String> uniqueSupplier = getSupplier(1_000_000, 2_000_000 , prefix);
        return  duplicatesPercentage == 0.0F ? uniqueSupplier : new DuplicateSupplier<>(uniqueSupplier, duplicatesPercentage);
    }

    public static String convertSize(long bytes) {
        String[] suffixes = new String[] { "BYTES", "KBS", "MBS", "GBS" };
        int counter = 0;
        while (true) {
            counter++;
            if (bytes / Math.pow(1024, counter) < 500) {
                break;
            }
        }
        return round(new Double(bytes / Math.pow(1024, counter)).longValue(), 2) + " " + suffixes[counter];
    }
}
