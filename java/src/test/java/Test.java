import org.apache.storm.shade.com.google.common.io.ByteArrayDataOutput;
import org.apache.storm.shade.com.google.common.io.ByteStreams;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

public class Test {

    public static void main(String[] args) {
        Map<ByteBuffer, Integer> map = new TreeMap<>();
        for (Integer i = 0; i<=20; i++) {
            ByteArrayDataOutput byteOutPut = ByteStreams.newDataOutput();
            byteOutPut.writeInt(i);
            map.put(ByteBuffer.wrap(byteOutPut.toByteArray()), i );
        }

        Iterator<Map.Entry<ByteBuffer, Integer>> iterator = map.entrySet().iterator();
        while(iterator.hasNext()) {
            Map.Entry<ByteBuffer, Integer> entry = iterator.next();
            System.out.println("key : " + entry.getKey().getInt() + " value : " + entry.getValue());
        }
    }
}
