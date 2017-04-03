package algos;

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;

import java.io.File;
import java.util.function.Supplier;

import static junit.framework.TestCase.assertTrue;
import static algos.utils.OnDiskDatasetUtils.makeBitSetFromFile;
import static algos.utils.OnDiskDatasetUtils.makeHLLFromFile;
import static algos.utils.OnDiskDatasetUtils.persistDataToFile;

/**
 * Created by volodymyr.bakhmatiuk on 4/3/17.
 */
public abstract class CardinalityTestBase {

    static final String FILE_NAME = "cardinality-test-file-name";

    protected int testHLLCardinalityOnDisk(int dataSetSize, Supplier<String> supplier) {
        File f = new File(FILE_NAME);
        if (f.exists()) f.delete();
        persistDataToFile(FILE_NAME, dataSetSize, supplier, false);
        HyperLogLogPlus hll = makeHLLFromFile(FILE_NAME);
        final int actualDeviation = Math.abs(new Long(hll.cardinality()).intValue() - dataSetSize);
        System.out.println(
                String.format("Got cardinality %1$d and deviation %2$d on dataset of size %3$d", hll.cardinality(), actualDeviation, dataSetSize)
        );
        return actualDeviation;
    }

    protected int testBitSetCardinalityOnDisk(int dataSetSize, Supplier<String> supplier) {
        File f = new File(FILE_NAME);
        if (f.exists()) f.delete();
        persistDataToFile(FILE_NAME, dataSetSize, supplier, false);
        SparseBitSet bitSet = makeBitSetFromFile(FILE_NAME);
        final int actualDeviation = Math.abs(new Long(bitSet.cardinality()).intValue() - dataSetSize);
        System.out.println(
                String.format("Got cardinality %1$d and deviation %2$d on dataset of size %3$d", bitSet.cardinality(), actualDeviation, dataSetSize)
        );
        return actualDeviation;
    }

}
