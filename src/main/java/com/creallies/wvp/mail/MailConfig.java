package com.creallies.wvp.mail;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "mail")
@Data
public class MailConfig {
    private String ip;
    private String username;
    private String password;
    private String topic;
    private String uploadUrl;
    private String loginUrl;
    private String loginUsername;
    private String loginPassword;
}
