package customtype;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CustomKey implements WritableComparable<CustomKey> {
    private String cityName;
    private String OSType;


    public String getCityName() {
        return cityName; }

    public CustomKey() {

        }

    public CustomKey(String cityName, String OSType) {
        super();
        this.cityName = cityName;
        this.OSType = OSType;
        }

    public String getOSType() {
        return OSType;
        }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeBytes(cityName);
        out.writeBytes(OSType);
        }

    @Override
    public void readFields(DataInput in) throws IOException {
        cityName = in.readLine();
        OSType = in.readLine();
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
        int returnValue = compare(OSType, o.getOSType());
        if (returnValue != 0) {
        return returnValue;
        }
        return compare(cityName, o.getCityName());
        }

    public static int compare(String k1, String k2) {
        return (k1 == k2 ? 0 : 1);
        }

    @Override
    public String toString() {
        return "util.CustomKey [CityName=" + cityName + ", OSType=" + OSType + "]";
        }
}
