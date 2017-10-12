package customtype;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SortComparator extends WritableComparator {
    public SortComparator() {
        super(CustomKey.class, true);
    }

    @Override
    public int compare(WritableComparable first, WritableComparable second) {
        CustomKey theFirst = (CustomKey)first;
        CustomKey theSecond = (CustomKey)second;

        int res = theFirst.getOSType().compareTo(theSecond.getOSType());
        if(res == 0) {
            res = theFirst.getCityName().compareTo(theSecond.getCityName());
        }

        return res;
    }
}