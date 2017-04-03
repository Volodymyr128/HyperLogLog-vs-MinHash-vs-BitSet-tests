package algos.utils;

import static algos.utils.TestUtils.round;

/**
 * Created by volodymyr.bakhmatiuk on 3/30/17.
 */
public enum SizeMetric {

    BYTES(1, "BYTES"), KBS(2, "KBS"), MBS(2, "MBS"), GBS(3, "GBS");

    private final int coefficient;
    private final String name;

    SizeMetric(int order, String name) {
        coefficient = (int) Math.pow(1024, order);
        this.name = name;
    }

    public String convert(float bytes, int precision) {
        return round(bytes / coefficient, precision) + " " + name;
    }
}
