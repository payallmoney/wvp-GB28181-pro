package com.creallies.wvp.mqtt;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.creallies.wvp.mail.MailConfig;
import com.genersoft.iot.vmp.common.StreamInfo;
import com.genersoft.iot.vmp.conf.UserSetting;
import com.genersoft.iot.vmp.gb28181.bean.Device;
import com.genersoft.iot.vmp.gb28181.bean.RecordInfo;
import com.genersoft.iot.vmp.gb28181.transmit.callback.DeferredResultHolder;
import com.genersoft.iot.vmp.gb28181.transmit.callback.RequestMessage;
import com.genersoft.iot.vmp.gb28181.transmit.cmd.impl.SIPCommander;
import com.genersoft.iot.vmp.media.bean.MediaServer;
import com.genersoft.iot.vmp.service.IPlayService;
import com.genersoft.iot.vmp.service.bean.InviteErrorCode;
import com.genersoft.iot.vmp.storager.IVideoManagerStorage;
import com.genersoft.iot.vmp.storager.dao.DeviceChannelMapper;
import com.genersoft.iot.vmp.storager.dao.DeviceMapper;
import com.genersoft.iot.vmp.utils.DateUtil;
import com.genersoft.iot.vmp.vmanager.bean.ErrorCode;
import com.genersoft.iot.vmp.vmanager.bean.StreamContent;
import com.genersoft.iot.vmp.vmanager.bean.WVPResult;
import com.genersoft.iot.vmp.vmanager.gb28181.playback.PlaybackController;
import com.genersoft.iot.vmp.vmanager.gb28181.ptz.PtzController;
import com.genersoft.iot.vmp.vmanager.gb28181.record.GBRecordController;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.compress.utils.FileNameUtils;
import org.eclipse.paho.mqttv5.client.*;
import org.eclipse.paho.mqttv5.client.persist.MemoryPersistence;
import org.eclipse.paho.mqttv5.common.MqttException;
import org.eclipse.paho.mqttv5.common.MqttMessage;
import org.eclipse.paho.mqttv5.common.MqttPersistenceException;
import org.eclipse.paho.mqttv5.common.packet.MqttProperties;
import org.eclipse.paho.mqttv5.common.packet.UserProperty;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.FileSystemResource;
import org.springframework.http.*;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.util.StringUtils;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.context.request.async.DeferredResult;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import javax.mail.*;
import javax.mail.internet.MimeUtility;
import javax.mail.search.FlagTerm;
import javax.sip.InvalidArgumentException;
import javax.sip.SipException;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.text.ParseException;
import java.util.*;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.creallies.wvp.constants.VideoConstants.DEVICE_CONTROL;
import static com.creallies.wvp.constants.VideoConstants.DEVICE_CONTROL_REPLY;


/**
 * mqtt 客户端
 */
@Slf4j
@Component
public class MyMqttClient {
    private static MqttAsyncClient CLIENT;

    @Autowired
    private MqttConfig config;

    @Autowired
    private DeviceChannelMapper deviceChannelMapper;
    @Autowired
    private DeviceMapper deviceMapper;

    @Autowired
    private IPlayService playService;

    @Autowired
    private PtzController ptzController;

    @Autowired
    private GBRecordController gbRecordController;

    @Autowired
    private PlaybackController playbackController;

    private static Vector<Map<String, Object>> failMessageList = new Vector<>();

    @Autowired
    private SIPCommander cmder;

    @Autowired
    private IVideoManagerStorage storager;

    @Autowired
    private UserSetting userSetting;

    @Autowired
    private MailConfig mailConfig;

    public static Map<String, String> ALARM_TYPE = new HashMap<>();

    static {
        ALARM_TYPE.put("Average Temperature", "平均温度");
        ALARM_TYPE.put("MIN. Temperature", "最低温度");
        ALARM_TYPE.put("MAX. Temperature", "最高温度");
        ALARM_TYPE.put("Diff. Temperature", "温差");
        ALARM_TYPE.put("more than", "大于");
        ALARM_TYPE.put("less than", "小于");
        ALARM_TYPE.put("平均温度", "avg");
        ALARM_TYPE.put("最低温度", "min");
        ALARM_TYPE.put("最高温度", "max");
        ALARM_TYPE.put("温差", "diff");
        ALARM_TYPE.put("大于", "more");
        ALARM_TYPE.put("小于", "less");
    }

    public static String TEMPER_REGEX = "^Current Temperature (\\d+\\.\\d+) °C (\\S+ than) Trigger Temperature (\\d+\\.\\d+) °C.$";

    public static String[] getTempers(String text) {
        Pattern pattern = Pattern.compile(TEMPER_REGEX);
        Matcher matcher = pattern.matcher(text);
        if (matcher.find()) {
            return new String[]{matcher.group(1), matcher.group(3), matcher.group(2)};
        } else {
            return null;
        }
    }


    @PostConstruct
    public void init() {
        log.info("[mqtt] 初始化并启动......");
        this.connect();
    }

    public static String fileToBase64(String filePath) {
        try {
            byte[] fileBytes = Files.readAllBytes(Paths.get(filePath));
            return Base64.getEncoder().encodeToString(fileBytes);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }


    /**
     * 连接mqtt服务器
     */
    private void connect() {
        MemoryPersistence persistence = new MemoryPersistence();
        try {
            String name = UUID.randomUUID().toString();
            String clientId = config.getClientId();
            if (!StringUtils.hasLength(clientId)) clientId = UUID.randomUUID().toString();
//            MqttClient client = new MqttClient(config.getHost(), clientId, persistence);
            MqttAsyncClient client = new MqttAsyncClient(config.getHost(), clientId, persistence);
            // MQTT 连接选项
            MqttConnectionOptions connectionOptions = new MqttConnectionOptions();
            if (StringUtils.hasLength(config.getUsername())) {
                connectionOptions.setUserName(config.getUsername());
            }
            if (StringUtils.hasLength(config.getPassword())) {
                connectionOptions.setPassword(config.getPassword().getBytes(StandardCharsets.UTF_8));
            }
            // 保留会话
            connectionOptions.setCleanStart(config.getCleanStart());
            connectionOptions.setConnectionTimeout(config.getTimeout());
            connectionOptions.setAutomaticReconnect(config.getAutoReconnect());
            connectionOptions.setKeepAliveInterval(config.getKeepAlive());
            connectionOptions.setReceiveMaximum(config.getReceiveMaximum());


            log.info("[mqtt:{}] Connected", config.getName());
            log.info("[mqtt:{}] Subscribe topic: {}", config.getName(), config.getTopics());
            IMqttToken connectToken = client.connect(connectionOptions);
            connectToken.waitForCompletion(3000);
            // 设置回调
            client.setCallback(new MqttCallback() {
                @Override
                public void disconnected(MqttDisconnectResponse mqttDisconnectResponse) {
                    log.info("mqtt disconnected ==  {}", JSON.toJSONString(mqttDisconnectResponse));
                }

                @Override
                public void mqttErrorOccurred(MqttException e) {
                    log.error("mqtt mqttErrorOccurred ==" + e.getMessage(), e);
                }

                @Override
                public void messageArrived(String s, MqttMessage mqttMessage) throws Exception {
                    log.info("mqtt messageArrived == {} , {}", s, JSON.toJSONString(mqttMessage));
                    processMessage(s, mqttMessage);
                }

                @Override
                public void deliveryComplete(IMqttToken iMqttToken) {
                    log.info("mqtt deliveryComplete ==  {}", JSON.toJSONString(iMqttToken));
                }

                @Override
                public void connectComplete(boolean b, String s) {
                    log.info("mqtt connectComplete ==  {} , {}", s, DEVICE_CONTROL);

                }

                @Override
                public void authPacketArrived(int i, MqttProperties mqttProperties) {
                    log.info("authPacketArrived ==  {}", JSON.toJSONString(mqttProperties));
                }
            });
            CLIENT = client;
            subscribe(DEVICE_CONTROL, config.getQos());
            for (Map<String, Object> row : failMessageList) {
                failMessageList.remove(row);
                publish((String) row.get("topic"), (String) row.get("data"), (MqttProperties) row.get("props"));
            }
            // 建立连接
            log.info("[mqtt:{}] Connecting to broker: {} ", config.getName(), config.getHost());

        } catch (MqttException me) {
            log.error("[mqtt:{}] connect err：", config.getName(), me);
        }
    }

    public void processMessage(String topic, MqttMessage message) throws Exception {
        String replyTopic = DEVICE_CONTROL_REPLY;
        MqttProperties mqttProperties = message.getProperties();
        List<UserProperty> userProperties = mqttProperties.getUserProperties();
        UserProperty repIdProperty = userProperties.stream().filter(item -> item.getKey().equalsIgnoreCase("requestId")).findFirst().orElse(null);
        MqttProperties replyMqttProperties_temp = null;
        if (repIdProperty != null) {
            replyMqttProperties_temp = new MqttProperties();
            replyMqttProperties_temp.setResponseTopic(replyTopic);
            List<UserProperty> replyUserProperties = new ArrayList<>();
            replyUserProperties.add(repIdProperty);
            replyMqttProperties_temp.setUserProperties(replyUserProperties);
        }
        final MqttProperties replyMqttProperties = replyMqttProperties_temp;
        String id = null;
        try {
            String payloadData = new String(message.getPayload());
            log.info("mqtt topic == {} ,messageArrived == {}", topic, payloadData);
            JSONObject param = null;
            try {
                param = JSON.parseObject(payloadData);
//                param = JSON.parseObject(param.getJSONObject("params").getString("value"));
            } catch (Exception ex) {
                log.error("视频请求格式不正确,无法解析为json!");
                throw new Exception("视频请求格式不正确,无法解析为json!");
            }
            id = param.getString("id");
            Map<String, Object> contentData = new HashMap<>();
            contentData.put("id", id);
            String action = param.getString("action");
            if (!StringUtils.hasLength(action)) {
                log.error("未设置action,无法处理mqtt请求!");
                throw new Exception("未设置action,无法处理mqtt请求!");
            }
            String value = null;
            String deviceId = null;
            String channelId = null;
            try {
                deviceId = param.getString("deviceId");
                channelId = param.getString("channelId");
            } catch (Exception e) {
                log.error("未设置视频的设备ID和通道ID,应该为\"{设备ID}/{通道ID}\",无法处理mqtt请求!");
                throw new Exception("未设置视频的设备ID和通道ID,应该为\"{设备ID}/{通道ID}\",无法处理mqtt请求!");
            }

            switch (action) {
                case "snap":
                    log.debug("获取截图: {}/{}", deviceId, channelId);
                    String fileName = deviceId + "_" + channelId + "_" + DateUtil.getNowForUrl() + ".jpg";
                    playService.getSnap(deviceId, channelId, fileName, (code, msg, data) -> {
                        Map<String, Object> sendPayload = new HashMap<>();
                        if (code == InviteErrorCode.SUCCESS.getCode()) {
                            contentData.put("type", "base64");
                            contentData.put("data", fileToBase64(((File) data).getAbsolutePath()));
                            sendPayload.put("code", 200);
                            sendPayload.put("data", contentData);
                        } else {
                            sendPayload.put("code", 500);
                            sendPayload.put("data", contentData);
                            sendPayload.put("msg", "失败");
                        }
                        publish(replyTopic, JSON.toJSONString(sendPayload), replyMqttProperties);
                    });
                    break;
                case "control": {
                    log.debug("执行命令: {}/{}", deviceId, channelId);
                    String command = param.getString("command");
                    Integer horizonSpeed = param.getInteger("horizonSpeed");
                    Integer verticalSpeed = param.getInteger("verticalSpeed");
                    Integer zoomSpeed = param.getInteger("zoomSpeed");
                    Map<String, Object> sendPayload = new HashMap<>();
                    ptzController.ptz(deviceId, channelId, command, horizonSpeed, verticalSpeed, zoomSpeed);
                    sendPayload.put("id", id);
                    sendPayload.put("code", 200);
                    sendPayload.put("msg", "成功");
                    sendPayload.put("data", contentData);
                    publish(replyTopic, JSON.toJSONString(sendPayload), replyMqttProperties);
                    break;
                }
                case "front_end_command": {
                    log.debug("执行命令: {}/{}", deviceId, channelId);
                    int cmdCode = param.getInteger("cmdCode");
                    int parameter1 = param.getInteger("parameter1");
                    int parameter2 = param.getInteger("parameter2");
                    int combindCode2 = param.getInteger("combindCode2");

                    Map<String, Object> sendPayload = new HashMap<>();
                    ptzController.frontEndCommand(deviceId, channelId, cmdCode, parameter1, parameter2, combindCode2);
                    sendPayload.put("code", 200);
                    sendPayload.put("msg", "成功");
                    sendPayload.put("data", contentData);
                    publish(replyTopic, JSON.toJSONString(sendPayload), replyMqttProperties);
                    break;
                }
                case "gb_record/query": {
                    log.debug("执行命令: {}/{}", deviceId, channelId);
                    String startTime = param.getString("startTime");
                    String endTime = param.getString("endTime");
                    recordPlay1(replyTopic, id, deviceId, channelId, startTime, endTime, replyMqttProperties);

                    break;
                }
                case "playback/start": {
                    log.debug("执行命令: {}/{}", deviceId, channelId);
                    String startTime = param.getString("startTime");
                    String endTime = param.getString("endTime");
                    playbackStart1(replyTopic, id, deviceId, channelId, startTime, endTime, replyMqttProperties);


                    break;
                }
                case "restartPlay": {
                    log.debug("执行命令: {}/{}", deviceId, channelId);
                    String startTime = param.getString("startTime");
                    String endTime = param.getString("endTime");
                    restartPlay(replyTopic, id, deviceId, channelId, replyMqttProperties);
                    break;
                }
            }
        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
            Map<String, Object> data = new HashMap<>();
            data.put("id", id);
            Map<String, Object> sendPayload = new HashMap<>();
            sendPayload.put("code", 500);
            sendPayload.put("data", data);
            sendPayload.put("msg", ex.getMessage());
            publish(replyTopic, JSON.toJSONString(sendPayload), replyMqttProperties);

        }
    }


    @Async
    public void restartPlay(String replyTopic, String requestId, String deviceId, String channelId, MqttProperties
            replyMqttProperties) {
        Device device = storager.queryVideoDevice(deviceId);
        MediaServer newMediaServerItem = playService.getNewMediaServerItem(device);
        Map<String, Object> sendPayload = new HashMap<>();
        Map<String, Object> contentData = new HashMap<>();
        contentData.put("id", requestId);
//        playService.stopPlay(device, channelId);
        try {
            cmder.streamByeCmd(device, channelId, "stop", null, null);
        } catch (Exception ignored) {
        }
        playService.play(newMediaServerItem, deviceId, channelId, null, null);
        sendPayload.put("code", 200);
        sendPayload.put("msg", "成功");
        sendPayload.put("data", contentData);
        publish(replyTopic, JSON.toJSONString(sendPayload), replyMqttProperties);
    }


    @Async
    public void playbackStart1(String replyTopic, String requestId, String deviceId, String channelId, String
            startTime, String endTime, MqttProperties replyMqttProperties) {
        MockHttpServletRequest request = new MockHttpServletRequest();
        request.setLocalAddr("0.0.0.0");
        DeferredResult<WVPResult<StreamContent>> rs = playbackController.start(request, deviceId, channelId, startTime, endTime);
        Map<String, Object> sendPayload = new HashMap<>();
        Map<String, Object> contentData = new HashMap<>();
        contentData.put("id", requestId);
        rs.setResultHandler(new DeferredResult.DeferredResultHandler() {
            @Override
            public void handleResult(Object result) {
                log.debug("执行命令:  playback/Start success");
                contentData.put("data", result);
                sendPayload.put("code", 200);
                sendPayload.put("msg", "成功");
                sendPayload.put("data", contentData);
                publish(replyTopic, JSON.toJSONString(sendPayload), replyMqttProperties);
            }
        });

        rs.onError(new Consumer<Throwable>() {
            @Override
            public void accept(Throwable throwable) {

                sendPayload.put("code", 500);
                sendPayload.put("msg", throwable.getMessage());
                sendPayload.put("data", contentData);
                publish(replyTopic, JSON.toJSONString(sendPayload), replyMqttProperties);
            }
        });

        rs.onTimeout(new Runnable() {
            @Override
            public void run() {
                sendPayload.put("code", 500);
                sendPayload.put("msg", "请求超时");
                sendPayload.put("data", contentData);
                publish(replyTopic, JSON.toJSONString(sendPayload), replyMqttProperties);
            }
        });
    }

    @Async
    public void playbackStart(String replyTopic, String requestId, String deviceId, String channelId, String
            startTime, String endTime, MqttProperties replyMqttProperties) {
        Map<String, Object> sendPayload = new HashMap<>();
        MockHttpServletRequest request = new MockHttpServletRequest();


        RequestMessage requestMessage = new RequestMessage();
        try {
            playService.playBack(deviceId, channelId, startTime, endTime, (code, msg, data) -> {

                WVPResult<StreamContent> wvpResult = new WVPResult<>();
                if (code == InviteErrorCode.SUCCESS.getCode()) {
                    wvpResult.setCode(ErrorCode.SUCCESS.getCode());
                    wvpResult.setMsg(ErrorCode.SUCCESS.getMsg());

                    if (data != null) {
                        StreamInfo streamInfo = (StreamInfo) data;
                        if (userSetting.getUseSourceIpAsStreamIp()) {
                            streamInfo = streamInfo.clone();//深拷贝
                            String host;
                            try {
                                URL url = new URL(request.getRequestURL().toString());
                                host = url.getHost();
                            } catch (MalformedURLException e) {
                                host = request.getLocalAddr();
                            }
                            streamInfo.channgeStreamIp(host);
                        }
                        wvpResult.setData(new StreamContent(streamInfo));
                    }
                } else {
                    wvpResult.setCode(code);
                    wvpResult.setMsg(msg);
                }
                requestMessage.setData(wvpResult);
                log.debug("执行命令:  playback/Start success");
                sendPayload.put("id", requestId);
                sendPayload.put("code", 200);
                sendPayload.put("data", requestMessage);
                publish(replyTopic, JSON.toJSONString(sendPayload), replyMqttProperties);
            });
        } catch (Exception e) {
            log.debug("执行命令:  playback/Start onError");
            sendPayload.put("id", requestId);
            sendPayload.put("code", 500);
            sendPayload.put("data", e.getMessage());
            publish(replyTopic, JSON.toJSONString(sendPayload), replyMqttProperties);
        }

    }

    @Async
    public void recordPlay1(String replyTopic, String requestId, String deviceId, String channelId, String
            startTime, String endTime, MqttProperties replyMqttProperties) {
        DeferredResult<WVPResult<RecordInfo>> rs = gbRecordController.recordinfo(deviceId, channelId, startTime, endTime);
        Map<String, Object> sendPayload = new HashMap<>();
        Map<String, Object> contentData = new HashMap<>();
        contentData.put("id", requestId);
        rs.setResultHandler(new DeferredResult.DeferredResultHandler() {
            @Override
            public void handleResult(Object result) {
                log.debug("执行命令:  playback/Start success");
                contentData.put("data", result);
                sendPayload.put("code", 200);
                sendPayload.put("data", contentData);
                publish(replyTopic, JSON.toJSONString(sendPayload), replyMqttProperties);
            }
        });

        rs.onError(new Consumer<Throwable>() {
            @Override
            public void accept(Throwable throwable) {
                sendPayload.put("code", 500);
                sendPayload.put("msg", throwable.getMessage());
                sendPayload.put("data", contentData);
                publish(replyTopic, JSON.toJSONString(sendPayload), replyMqttProperties);
            }
        });

        rs.onTimeout(new Runnable() {
            @Override
            public void run() {
                sendPayload.put("code", 500);
                sendPayload.put("msg", "请求超时");
                sendPayload.put("data", contentData);
                publish(replyTopic, JSON.toJSONString(sendPayload), replyMqttProperties);
            }
        });
    }

    @Async
    public void recordPlay(String replyTopic, String requestId, String deviceId, String channelId, String
            startTime, String endTime, MqttProperties replyMqttProperties) {
        Map<String, Object> sendPayload = new HashMap<>();

        Device device = storager.queryVideoDevice(deviceId);
        // 指定超时时间 1分钟30秒
        String uuid = UUID.randomUUID().toString();
        int sn = (int) ((Math.random() * 9 + 1) * 100000);
        String key = DeferredResultHolder.CALLBACK_CMD_RECORDINFO + deviceId + sn;
        RequestMessage msg = new RequestMessage();
        msg.setId(uuid);
        msg.setKey(key);
        try {
            cmder.recordInfoQuery(device, channelId, startTime, endTime, sn, null, null, null, (eventResult -> {
                WVPResult<RecordInfo> wvpResult = new WVPResult<>();
                wvpResult.setCode(ErrorCode.ERROR100.getCode());
                wvpResult.setMsg("查询录像失败, status: " + eventResult.statusCode + ", message: " + eventResult.msg);
                msg.setData(wvpResult);
                sendPayload.put("id", requestId);
                sendPayload.put("code", 200);
                sendPayload.put("data", msg);
                publish(replyTopic, JSON.toJSONString(sendPayload), replyMqttProperties);
            }));
        } catch (InvalidArgumentException | SipException | ParseException e) {
            log.debug("执行命令:  gb_record/query onError");
            sendPayload.put("id", requestId);
            sendPayload.put("code", 500);
            sendPayload.put("data", e.getMessage());
            publish(replyTopic, JSON.toJSONString(sendPayload), replyMqttProperties);
        }
    }

//    public MqttClient getClient() {
//        return client;
//    }
//

    /**
     * 订阅某个主题
     *
     * @param topic
     * @param qos
     */
    public static void subscribe(String topic, int qos) throws MqttException {
        try {
            log.info("[mqtt] subscribe topic:" + topic);
            CLIENT.subscribe(topic, qos);
        } catch (MqttException e) {
            log.error("[mqtt] 订阅异常：", e);
            throw e;
        }
    }

    public static void subscribe(String topic, int qos, IMqttMessageListener messageListener) throws MqttException {
        try {
            log.info("[mqtt] subscribe topic:" + topic);
            CLIENT.subscribe(topic, qos);
        } catch (MqttException e) {
            log.error("[mqtt] 订阅异常：", e);
            throw e;
        }
    }

    public static void publish(String topic, String msg) {
        try {
            publish(topic, msg, 1, false, null);
        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
            Map<String, Object> msgData = new HashMap<>();
            msgData.put("topic", topic);
            msgData.put("data", msg);
            failMessageList.add(msgData);
        }
    }

    /**
     * 发布，非持久化
     * <p>
     * qos 1
     *
     * @param topic
     * @param msg
     */
    public static void publish(String topic, String msg, MqttProperties properties) {
        try {
            publish(topic, msg, 1, false, properties);
        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
            Map<String, Object> msgData = new HashMap<>();
            msgData.put("topic", topic);
            msgData.put("data", msg);
            failMessageList.add(msgData);
        }
    }

    /**
     * 发布
     */
    public static void publish(String topic, String pushMessage, int qos, boolean retained, MqttProperties
            properties) throws Exception {
        if (CLIENT == null) {
            log.error("[mqtt] publish error. client not ready");
            throw new Exception("[mqtt] publish error. client not ready");
        }
        MqttMessage message = new MqttMessage(pushMessage.getBytes());
        message.setQos(qos);
        message.setRetained(retained);
        if (properties != null) message.setProperties(properties);
        try {
            CLIENT.publish(topic, message);
            log.debug("[mqtt] 消息发送 topic[{}] msg: {} ", topic, pushMessage);
        } catch (MqttPersistenceException e) {
            log.error("[mqtt] 消息发送失败: " + pushMessage, e);
            throw e;
        } catch (MqttException e) {
            log.error("[mqtt] 消息发送失败: " + pushMessage, e);
            throw e;
        }
    }

    @Scheduled(cron = "0 0/1 * * * ?")
    public synchronized void mailAlarmToMqtt() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("mail.store.protocol", "imap");
        Session session = Session.getDefaultInstance(properties, null);
        Store store = session.getStore("imap");
        store.connect(mailConfig.getIp(), mailConfig.getUsername(), mailConfig.getPassword());
        Folder inbox = store.getFolder("INBOX");
        inbox.open(Folder.READ_WRITE);
        FlagTerm unreadFlagTerm = new FlagTerm(new Flags(Flags.Flag.SEEN), false);
        Message[] unreadMessages = inbox.search(unreadFlagTerm);
        log.info("unreadMessages==={}", unreadMessages.length);
        for (Message message : unreadMessages) {
            if (message.isMimeType("multipart/*")) {
                Map<String, Object> messageJSON = new HashMap<>();
                Multipart multipart = (Multipart) message.getContent();
                int partCount = multipart.getCount();
                List<Map<String, Object>> attachList = new ArrayList();
                boolean isTemper = false;
                for (int i = 0; i < partCount; i++) {
                    BodyPart bodyPart = multipart.getBodyPart(i);
                    if (Part.ATTACHMENT.equalsIgnoreCase(bodyPart.getDisposition())) {
                        // 这是一个附件
                        String fileName = MimeUtility.decodeText(bodyPart.getFileName());
                        InputStream attachmentStream = bodyPart.getInputStream();
                        Map<String, Object> attach = new HashMap<>();
                        attach.put("content_name", fileName);
                        String filepath = uploadFile(attachmentStream, fileName, 0);
//                        attach.put("base64", convertInputStreamToBase64(attachmentStream));
                        attach.put("baseUrl", filepath);
                        attach.put("content_type", "image/" + FileNameUtils.getExtension(fileName).toLowerCase());
                        attachList.add(attach);
                    }
                }
                messageJSON.put("attach", attachList);
                for (int i = 0; i < partCount; i++) {
                    BodyPart bodyPart = multipart.getBodyPart(i);
                    if (!Part.ATTACHMENT.equalsIgnoreCase(bodyPart.getDisposition())) {
                        String body = bodyPart.getContent().toString();
                        messageJSON.put("content", body);
                        String[] lines = body.split("\r\n");
                        for (String line : lines) {
                            if (line.contains(":")) {
                                String[] info = line.split(":");
                                if (info.length >= 2) {
                                    List<String> end = Arrays.stream(info).collect(Collectors.toList());
                                    end.remove(0);
                                    String infoData = String.join(":", end.toArray(new String[0])).trim();

                                    String key = info[0];
                                    if ("EVENT TIME".equals(key)) {
                                        infoData = infoData.replace(",", " ").replace(".", "");
                                    }

                                    messageJSON.put(key, infoData);
                                    if (key.contains("S/N")) {
                                        messageJSON.put("SN", infoData);
                                    }
                                    if ("Tolerance Temperature ".equals(key)) {
                                        isTemper = true;
                                        sendMessage(messageJSON);
                                    }

                                }
                            } else {
                                String[] tempers = getTempers(line);
                                if (tempers != null) {
                                    messageJSON.put("当前温度", tempers[0]);
                                    messageJSON.put("限额温度", tempers[1]);
                                    messageJSON.put("报警类型", ALARM_TYPE.get((String) messageJSON.get("Alarm Type")));
                                    messageJSON.put("超限类型", ALARM_TYPE.get(tempers[2]));
                                }
                            }
                        }
                    }
                }
                //不是测温报警循环完成发送mqtt消息
                if (messageJSON.containsKey("SN") && !isTemper) {
                    sendMessage(messageJSON);
                    message.setFlag(Flags.Flag.SEEN, true);
                } else if (isTemper) {
                    message.setFlag(Flags.Flag.SEEN, true);
                }
            }
        }
        store.close();
    }

    private void sendMessage(Map messageJSON) {
        String eventType = (String) messageJSON.get("EVENT TYPE");
        switch (eventType) {
            case "Motion Detection":
                sendMoveMessage(messageJSON);
                break;
            case "Temperature Measurement Alarm.":
                sendTemperMessage(messageJSON);
                break;

        }
    }

    private void sendTemperMessage(Map messageJSON) {
        Map<String, Object> sendPayload = new HashMap<>();
        sendPayload.put("SN", messageJSON.get("SN"));
        sendPayload.put("type", "ALARM");
        sendPayload.put("name", "温度超限");
        sendPayload.put("subtype", "温度超限");
        sendPayload.put("startTime", messageJSON.get("EVENT TIME"));
        Map<String, Object> details = new HashMap<>();
        details.put("当前温度", messageJSON.get("当前温度"));
        details.put("限额温度", messageJSON.get("限额温度"));
        details.put("报警类型", messageJSON.get("报警类型"));
        details.put("超限类型", messageJSON.get("超限类型"));
//        sendPayload.put("details", "当前温度:" + messageJSON.get("当前温度") + ",限额温度:" + messageJSON.get("限额温度") + ",报警类型:" + messageJSON.get("报警类型") + ",超限类型:" + messageJSON.get("超限类型"));
        sendPayload.put("details", JSON.toJSONString(details));
//        sendPayload.put("details", messageJSON.toString());
        sendPayload.put("images", messageJSON.get("attach"));
        //测温报警发送mqtt消息(多次发送)
        publish(mailConfig.getTopic() + "/Temper/"+ALARM_TYPE.get((String)messageJSON.get("报警类型"))+"/"+ ALARM_TYPE.get((String)messageJSON.get("超限类型"))+"/"+ messageJSON.get("SN"), JSON.toJSONString(sendPayload), null);
    }

    private void sendMoveMessage(Map messageJSON) {
        Map<String, Object> sendPayload = new HashMap<>();
        sendPayload.put("SN", messageJSON.get("SN"));
        sendPayload.put("type", "EVENT");
        sendPayload.put("name", "移动侦测");
        sendPayload.put("subtype", "移动侦测");
        sendPayload.put("startTime", messageJSON.get("EVENT TIME"));
//        sendPayload.put("details", messageJSON.toString());
//        sendPayload.put("details", messageJSON.toString());
        sendPayload.put("images", messageJSON.get("attach"));
        //测温报警发送mqtt消息(多次发送)
        publish(mailConfig.getTopic() + "/MotionDetect/" + messageJSON.get("SN"), JSON.toJSONString(sendPayload), null);
    }


    private static String convertInputStreamToBase64(InputStream inputStream) {
        try {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            byte[] buffer = new byte[1024];
            int bytesRead;
            while ((bytesRead = inputStream.read(buffer)) != -1) {
                outputStream.write(buffer, 0, bytesRead);
            }
            byte[] bytes = outputStream.toByteArray();
            return Base64.getEncoder().encodeToString(bytes);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    private String token;


    private String uploadFile(InputStream file, String name, int num) throws IOException {
        RestTemplate restTemplate = new RestTemplate();
        String url = mailConfig.getUploadUrl(); // 替换为实际的接口 URL
        File tempFile = File.createTempFile(System.getProperty("java.io.tmpdir"), ".tmp");
        Files.copy(file, Paths.get(tempFile.getAbsolutePath()), StandardCopyOption.REPLACE_EXISTING);
// 构造 MultipartFile
        FileSystemResource fileResource = new FileSystemResource(tempFile);
        MultiValueMap<String, Object> body = new LinkedMultiValueMap<>();
        body.add("file", fileResource);
        body.add("path", "");
        body.add("name", UUID.randomUUID() + name);
        body.add("tags", "attach");
        body.add("remark", "");
        body.add("override", true);

// 构造请求头
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.MULTIPART_FORM_DATA);
        if (token == null) {
            token = login();
        }
        headers.set("Authorization", "Bearer " + token);

// 构造请求实体
        HttpEntity<MultiValueMap<String, Object>> requestEntity = new HttpEntity<>(body, headers);

// 发送请求
        ResponseEntity<String> response = restTemplate.exchange(url, HttpMethod.POST, requestEntity, String.class);
        int maxRetry = 1;
        if (response.getStatusCode() == HttpStatus.UNAUTHORIZED && num < maxRetry) {
            token = login();
            return uploadFile(file, name, num + 1);
        }
        System.out.println("uploadFile Response: " + response.getBody());
        JSONObject resp = JSON.parseObject(response.getBody());
        if (resp.getInteger("code") == 200) {
            return resp.getString("data");
        } else {
            return null;
        }

    }

    private String login() {
        RestTemplate restTemplate = new RestTemplate();
        String url = mailConfig.getLoginUrl(); // 替换为实际的接口 URL
// 构造 MultipartFile
        Map body = new HashMap();
        body.put("username", mailConfig.getLoginUsername());
        body.put("password", mailConfig.getLoginPassword());
// 构造请求头
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

// 构造请求实体
        HttpEntity<Map> requestEntity = new HttpEntity<>(body, headers);

// 发送请求
        ResponseEntity<String> response = restTemplate.exchange(url, HttpMethod.POST, requestEntity, String.class);
        System.out.println("login Response: " + response.getBody());
        JSONObject resp = JSON.parseObject(response.getBody());
        if (resp.getInteger("code") == 200) {
            return resp.getJSONObject("data").getString("token");
        } else {
            return null;
        }
    }
}
