package com.creallies.wvp.redis;


import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.listener.PatternTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import com.creallies.wvp.constants.VideoManagerConstants;

@Configuration
@Order(value=2)
@Slf4j
public class WvpDriverRedisMsgListenConfig {

	@Autowired
	private WvpDriverRedisPushDeviceStatusListMsgListener redisPushStreamListMsgListener;


	/**
	 * redis消息监听器容器 可以添加多个监听不同话题的redis监听器，只需要把消息监听器和相应的消息订阅处理器绑定，该消息监听器
	 * 通过反射技术调用消息订阅处理器的相关方法进行一些业务处理
	 * 
	 * @param connectionFactory
	 * @return
	 */
	@Bean
	RedisMessageListenerContainer wvpDriverContainer(RedisConnectionFactory connectionFactory) {
		log.info("wvp-driver: init redis listen!");
        RedisMessageListenerContainer container = new RedisMessageListenerContainer();
        container.setConnectionFactory(connectionFactory);
		container.addMessageListener(redisPushStreamListMsgListener, new PatternTopic(VideoManagerConstants.VM_MSG_SUBSCRIBE_DEVICE_STATUS));
        return container;
    }
}
