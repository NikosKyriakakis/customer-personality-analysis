import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TupleWritable implements WritableComparable<TupleWritable> {
    public int offset;
    private int id;
    private int age;
    private double income;
    private double mntWines;
    private String education;
    private String maritalStatus;

    public TupleWritable() {

    }

    public TupleWritable(
            int id,
            int age,
            double income,
            double mntWines,
            String education,
            String maritalStatus
    ) {
        this.id = id;
        this.age = age;
        this.income = income;
        this.mntWines = mntWines;
        this.education = education;
        this.maritalStatus = maritalStatus;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(id);
        dataOutput.writeInt(age);
        dataOutput.writeDouble(income);
        dataOutput.writeDouble(mntWines);
        dataOutput.writeUTF(education);
        dataOutput.writeUTF(maritalStatus);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        id = dataInput.readInt();
        age = dataInput.readInt();
        income = dataInput.readDouble();
        mntWines = dataInput.readDouble();
        education = dataInput.readUTF();
        maritalStatus = dataInput.readUTF();
    }

    public static TupleWritable read(DataInput dataInput) throws IOException {
        TupleWritable writable = new TupleWritable();
        writable.readFields(dataInput);
        return writable;
    }

    @Override
    public int compareTo(TupleWritable other) {
        int compare = Double.compare(this.mntWines, other.mntWines);
        if (compare == 0) {
            compare = Double.compare(this.income, other.income);
            if (compare == 0) {
                compare = Integer.compare(this.id, other.id);
            }
        }
        return compare;
    }

    @Override
    public String toString() {
        return offset + ", " + id + ", " + age + ", " + education + ", " + maritalStatus + ", " + income + ", " + mntWines;
    }
}