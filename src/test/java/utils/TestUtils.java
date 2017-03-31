package utils;

import java.math.BigDecimal;
import static junit.framework.TestCase.assertTrue;

/**
 * Created by volodymyr.bakhmatiuk on 3/29/17.
 */
public class TestUtils {

    public static float round(float num, int precision) {
        BigDecimal bd = new BigDecimal(Float.toString(num));
        bd = bd.setScale(precision, BigDecimal.ROUND_HALF_UP);
        return bd.floatValue();
    }

    public static float getDeviation(float expectedResult, float actualResult, int precision) {
        return round(Math.abs(expectedResult - actualResult), precision);
    }
}
