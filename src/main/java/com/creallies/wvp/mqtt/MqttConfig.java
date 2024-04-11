package com.creallies.wvp.mqtt;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@ConfigurationProperties(prefix = "mqtt")
@Data
public class MqttConfig {
    private String name;
    private String host;
    private String clientId;
    private String groupId;
    private String username;
    private String password;
    private Integer qos = 1;
    private Boolean cleanStart = true;
    private Integer timeout = 10;
    private Integer keepAlive = 60;
    private Integer receiveMaximum = 1000;
    private Boolean autoReconnect = true;
    private List<String> topics;
}
