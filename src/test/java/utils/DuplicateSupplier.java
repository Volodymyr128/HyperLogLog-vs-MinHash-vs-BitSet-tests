package utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.function.Supplier;

/**
 * Created by volodymyr.bakhmatiuk on 3/31/17.
 * A decorator for unique values supplier which make it return duplicates in strict proportion to unique values
 */
public final class DuplicateSupplier<T> implements Supplier<T> {

    public static final int BATCH_SIZE = 100;
    private static final Random random = new Random();

    private final Supplier<T> uniqueSupplier;
    private final int uniqueElementsPerBatch;

    List<T> lastValues = new ArrayList<>();
    private int counter = 0;

    public DuplicateSupplier(Supplier<T> uniqueSupplier, float duplicatesPercentage) {
        this.uniqueSupplier = uniqueSupplier;
        if (duplicatesPercentage < 0.0F || duplicatesPercentage >= 1.00F) {
            throw new IllegalArgumentException("Wrong duplicatesPercentage argumane value: " + duplicatesPercentage);
        }
        uniqueElementsPerBatch = new Float(BATCH_SIZE * ( 1 - duplicatesPercentage)).intValue();
    }

    @Override
    public T get() {
        if (counter++ % BATCH_SIZE == 0) {
            lastValues.clear();
        }
        T next = null;
        if (lastValues.size() < uniqueElementsPerBatch) {
            next = uniqueSupplier.get();
            lastValues.add(next);
        } else if (lastValues.size() < BATCH_SIZE) {
            next = lastValues.get(random.nextInt(lastValues.size()));
        }
        return next;
    }

    public int getUniqueElementsPerBatch() {
        return uniqueElementsPerBatch;
    }
}
