package com.creallies.wvp.redis;

import com.alibaba.fastjson2.JSONObject;
import com.creallies.wvp.enums.VideoTypeEnum;
import com.creallies.wvp.mqtt.MyMqttClient;
import com.genersoft.iot.vmp.gb28181.bean.DeviceChannel;
import com.genersoft.iot.vmp.storager.dao.DeviceChannelMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.stereotype.Component;
import com.creallies.wvp.constants.VideoConstants;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
@Slf4j
public class WvpDriverRedisPushDeviceStatusListMsgListener implements MessageListener {

    @Autowired
    private DeviceChannelMapper deviceChannelMapper;

    @Override
    public void onMessage(Message message, byte[] bytes) {
        log.info(" wvp-driver ====[REDIS消息-推流设备列表更新]： {}", new String(message.getBody()));
        String msg = new String(message.getBody());
        String[] data = msg.split(" ");
        Map<String, Object> result = new HashMap<>();
        result.put("type", VideoTypeEnum.onlineStatus.getValue());
        result.put("typeName", VideoTypeEnum.onlineStatus.getText());
        result.put("deviceId", data[0]);
        result.put("status", "ON".equals(data[1]) ? 1 : 0);
        List<DeviceChannel> channels = deviceChannelMapper.queryAllChannels(data[0]);
        for (DeviceChannel channel : channels) {
            result.put("channelId", channel.getChannelId());
            log.info("topic  === {}", VideoConstants.DEVICE_STATUS_TOPIC);
            MyMqttClient.publish(VideoConstants.DEVICE_STATUS_TOPIC + channel.getDeviceId() + "/" + channel.getChannelId(), JSONObject.toJSONString(result));
        }
    }
}
