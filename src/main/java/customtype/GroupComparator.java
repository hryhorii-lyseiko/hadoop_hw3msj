package customtype;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class GroupComparator extends WritableComparator {
    public GroupComparator() {
        super(CustomKey.class, true);
    }

    @Override
    public int compare(WritableComparable first, WritableComparable second) {
        CustomKey theFirst = (CustomKey)first;
        CustomKey theSecond = (CustomKey)second;

        return theFirst.compareTo(theSecond);

    }
}
