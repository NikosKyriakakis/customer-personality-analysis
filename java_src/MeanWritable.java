import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MeanWritable implements Writable {
    private int sum;
    private int count;

    public MeanWritable() {

    }

    public MeanWritable(int count, int sum) {
        this.sum = sum;
        this.count = count;
    }

    public int getSum() {
        return this.sum;
    }

    public int getCount() {
        return this.count;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(count);
        dataOutput.writeInt(sum);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        count = dataInput.readInt();
        sum = dataInput.readInt();
    }

    public static MeanWritable read(DataInput dataInput) throws IOException {
        MeanWritable writable = new MeanWritable();
        writable.readFields(dataInput);
        return writable;
    }
}