package lab3.WangXin20213599.hbase.inputSource;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MemberMapper extends TableMapper<Writable, Writable> {
    private Text k = new Text();
    private Text v = new Text();
    public static final String FIELD_COMMON_SEPARATOR = "\u0001";

    @Override
    protected void setup(Mapper<ImmutableBytesWritable, Result, Writable, Writable>.Context context) throws IOException, InterruptedException {

    }

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Mapper<ImmutableBytesWritable, Result, Writable, Writable>.Context context) throws IOException, InterruptedException {
        String res = null;
        String rowKey = new String(key.get());

        byte[] columnFamily = null;
        byte[] columnQualifier = null;

        long ts = 0L;

        try {
            for (Cell cell : value.listCells()){
                res = Bytes.toStringBinary(cell.getValueArray());
                columnFamily = cell.getFamilyArray();
                columnQualifier = cell.getQualifierArray();

                ts = cell.getTimestamp();
                k.set(rowKey);
                v.set(Bytes.toString(columnFamily) + FIELD_COMMON_SEPARATOR + Bytes.toString(columnQualifier)
                        + FIELD_COMMON_SEPARATOR + res + FIELD_COMMON_SEPARATOR + ts);
                context.write(k, v);
            }
        }catch (Exception e){
            e.printStackTrace();
            System.err.println("Error:" + e.getMessage() + ",Row:" + Bytes.toString(key.get()) + ",value" + res);
        }
    }
}
