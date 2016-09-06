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
			chargeResponseVo.setPayerId("10000000af797eb4d6a84e7ab8029d6883739c3410000212");
			chargeResponseVo.setAmount(new BigDecimal("12.1"));
			chargeResponseVo.setOrderId(100000462l);
			chargeResponseVo.setLiveUserId("10000000af797eb4d6a84e7ab8029d6883739c3410000212");
			chargeResponseVo.setPayHeadImg("http://test.fada.cim/qdja/skaldksaldk.jpg");
			chargeResponseVo.setSettingId("dda");
			chargeResponseVo.setDistribution("asda");
			chargeResponseVo.setAppId("wtx");
//			ChargeResponseVo chargeResponseVo2=new ChargeResponseVo();
//			chargeResponseVo2.setPayerId("10000000af797eb4d6a84e7ab8029d6883739c3410000212");
//			chargeResponseVo2.setAmount(new BigDecimal("12.1"));
//			chargeResponseVo2.setOrderId(100000462l);
//			chargeResponseVo2.setLiveUserId("10000000af797eb4d6a84e7ab8029d6883739c3410000212");
//			chargeResponseVo2.setPayHeadImg("http://test.fada.cim/qdja/skaldksaldk.jpg");
//			chargeResponseVo2.setSettingId("dda");
//			chargeResponseVo2.setDistribution("asda");
//			List<ChargeResponseVo> list=new ArrayList<ChargeResponseVo>();
//			list.add(chargeResponseVo);
//			list.add(chargeResponseVo2);
			producer.send(new KeyedMessage<Integer, String>(topic, JSON.toJSONString(chargeResponseVo)));
			System.out.println("发送了: " + JSON.toJSONString(chargeResponseVo));
			try {
				TimeUnit.SECONDS.sleep(1);
				i++;
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	private Producer createProducer() {
		Properties properties = new Properties();

		properties.put("serializer.class", StringEncoder.class.getName());
		properties.put("metadata.broker.list", "#");
		return new Producer<Integer, String>(new ProducerConfig(properties));
	 }
	
	
	public static void main(String[] args) {
		new kafkaProducer("test_charge").start();// 使用kafka集群中创建好的主题 test 123.57.84.60:9092,123.56.150.115:9092
		
	}
	 
}
