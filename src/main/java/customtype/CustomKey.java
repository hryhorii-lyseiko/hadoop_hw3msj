package customtype;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CustomKey implements WritableComparable<CustomKey> {

    private Text cityName = new Text();
    private Text OSType = new Text();

    public CustomKey() {

        }

    public CustomKey(Text cityName, Text OSType) {
        super();
        this.cityName.set(cityName);
        this.OSType.set(OSType);
        }

    public Text getOSType() {
        return OSType;
        }

    public Text getCityName() {
        return cityName;
    }

    public void setCityName(Text cityName) {
        this.cityName = cityName;
    }

    public void setOSType(Text OSType) {
        this.OSType = OSType;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        cityName.write(out);
        OSType.write(out);
        }

    @Override
    public void readFields(DataInput in) throws IOException {
        cityName.readFields(in);
        OSType.readFields(in);
        }

    @Override
    public int hashCode() {
    final int prime = 31;
        int result = 1;
        result = prime * result + ((OSType == null) ? 0 : OSType.hashCode());
        result = prime * result + ((cityName == null) ? 0 : cityName.hashCode());
        return result;
        }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
        return true;
        if (obj == null)
        return false;
        if (getClass() != obj.getClass())
        return false;
        CustomKey other = (CustomKey) obj;
        if (OSType == null) {
        if (other.OSType != null)
        return false;
        } else if (!OSType.equals(other.OSType))
        return false;
        if (cityName == null) {
        if (other.cityName != null)
        return false;
        } else if (!cityName.equals(other.cityName))
        return false;
        return true;
        }

    @Override
    public int compareTo(CustomKey o) {
        int returnValue = OSType.compareTo( o.getOSType());
        if (returnValue != 0) {
            returnValue = cityName.compareTo(o.getCityName());
        }
        return returnValue;
        }


    @Override
    public String toString() {
        return "util.CustomKey [CityName=" + cityName.toString() + ", OSType=" + OSType.toString() + "]";
        }
}
