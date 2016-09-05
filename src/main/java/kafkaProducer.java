import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import com.liaiai.ChargeResponseVo;
import com.alibaba.fastjson.JSON;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.serializer.StringEncoder;



/**
 * 发送数据
 * 发送了: 0
发送了: 1
发送了: 2
发送了: 3
发送了: 4
发送了: 5
发送了: 6
发送了: 7
发送了: 8
发送了: 9
发送了: 10
发送了: 11
发送了: 12
发送了: 13
发送了: 14
发送了: 15
发送了: 16
发送了: 17
发送了: 18
 * @author zm
 *
 */
public class kafkaProducer extends Thread{

	private String topic;
	
	public kafkaProducer(String topic){
		super();
		this.topic = topic;
	}
	
	
	@Override
	public void run() {
		Producer producer = createProducer();
		int i=0;
		while(true){
			ChargeResponseVo chargeResponseVo=new ChargeResponseVo();
			chargeResponseVo.setUserId("10000000af797eb4d6a84e7ab8029d6883739c3410000212");
			chargeResponseVo.setAmount(new BigDecimal("12.1"));
			chargeResponseVo.setLiveId("3918");
			chargeResponseVo.setOrderId("100000462");
			ChargeResponseVo chargeResponseVo2=new ChargeResponseVo();
			chargeResponseVo2.setUserId("10000000af797eb4d6a84e7ab8029d6883739c3410000212");
			chargeResponseVo2.setAmount(new BigDecimal("122.1"));
			chargeResponseVo2.setLiveId("3918");
			chargeResponseVo2.setOrderId("100000462");
			List<ChargeResponseVo> list=new ArrayList<ChargeResponseVo>();
			list.add(chargeResponseVo);
			list.add(chargeResponseVo2);
			producer.send(new KeyedMessage<Integer, String>(topic, JSON.toJSONString(list)));
			System.out.println("发送了: " + JSON.toJSONString(list));
			try {
				TimeUnit.SECONDS.sleep(5);
				i++;
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	private Producer createProducer() {
		Properties properties = new Properties();
//		properties.put("zookeeper.connect", "localhost:2181");
		properties.put("serializer.class", StringEncoder.class.getName());
		properties.put("metadata.broker.list", "123.57.84.60:9092");
		return new Producer<Integer, String>(new ProducerConfig(properties));
	 }
	
	
	public static void main(String[] args) {
		new kafkaProducer("test_charge_callback").start();// 使用kafka集群中创建好的主题 test
		
	}
	 
}
