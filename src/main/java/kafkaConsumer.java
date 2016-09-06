import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;




/**
 * 接收数据
 * 接收到: message: 10
接收到: message: 11
接收到: message: 12
接收到: message: 13
接收到: message: 14
 * @author zm
 *
 */
public class kafkaConsumer extends Thread{

	private String topic;
	
	public kafkaConsumer(String topic){
		super();
		this.topic = topic;
	}
	
	
	@Override
	public void run() {
		ConsumerConnector consumer = createConsumer();
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(topic, 1); // 一次从主题中获取一个数据
		 Map<String, List<KafkaStream<byte[], byte[]>>>  messageStreams = consumer.createMessageStreams(topicCountMap);
		 KafkaStream<byte[], byte[]> stream = messageStreams.get(topic).get(0);// 获取每次接收到的这个数据
		 ConsumerIterator<byte[], byte[]> iterator =  stream.iterator();
		 while(iterator.hasNext()){
			 String message = new String(iterator.next().message());
			 System.out.println("接收到: " + message);
		 }
	}

	private ConsumerConnector createConsumer() {
		Properties properties = new Properties();
		properties.put("zookeeper.connect", "123.57.84.60:2181");
		properties.put("group.id", "group4");
		properties.put("auto.commit.enable", "true");
		properties.put("zookeeper.session.timeout.ms", "20000");
		properties.put("auto.offset.reset", "smallest");
		properties.put("fetch.message.max.bytes", "10486000");
		return Consumer.createJavaConsumerConnector(new ConsumerConfig(properties));
	 }
	
	
	public static void main(String[] args) {
		new kafkaConsumer("test_charge").start();// 使用kafka集群中创建好的主题 test
		
	}
	 
}
