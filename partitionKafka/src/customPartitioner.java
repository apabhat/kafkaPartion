import java.util.*;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;

public class customPartitioner implements Partitioner {
    private String messageTypeName;

  @Override
  public void configure(Map<String, ?> configs) {
      messageTypeName = configs.get("message.type.name").toString();

  }

  @Override
  
  public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {

      List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
      int numPartitions = partitions.size();
      int sp = (int) Math.abs(numPartitions * 0.3);
      int p = 0;
      System.out.println("numPartitions:"+numPartitions + "sp:" + sp  );

      if ((keyBytes == null) || (!(key instanceof String)))
          throw new InvalidRecordException("All messages must have sensor name as key");

      if (((String) key).equals(messageTypeName))
          p = Utils.toPositive(Utils.murmur2(valueBytes)) % sp;
      else
          p = Utils.toPositive(Utils.murmur2(keyBytes)) % (numPartitions - sp) + sp;

      System.out.println("Key = " + (String) key + " Partition = " + p);
      return p;
  }


  
//  public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes,
//      Cluster cluster) {
//
//    int partition = 0;
//    String userName = (String) key;
//    // Find the id of current user based on the username
//    Integer userId = userService.findUserId(userName);
//    // If the userId not found, default partition is 0
//    if ( != null) {
//      partition = userId;
//
//    }
//    return partition;
//  }

  @Override
  public void close() {

  }
}