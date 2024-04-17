package com.creallies.wvp.mqtt;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.genersoft.iot.vmp.common.StreamInfo;
import com.genersoft.iot.vmp.conf.UserSetting;
import com.genersoft.iot.vmp.conf.exception.ControllerException;
import com.genersoft.iot.vmp.gb28181.bean.Device;
import com.genersoft.iot.vmp.gb28181.bean.RecordInfo;
import com.genersoft.iot.vmp.gb28181.transmit.callback.DeferredResultHolder;
import com.genersoft.iot.vmp.gb28181.transmit.callback.RequestMessage;
import com.genersoft.iot.vmp.gb28181.transmit.cmd.impl.SIPCommander;
import com.genersoft.iot.vmp.service.IPlayService;
import com.genersoft.iot.vmp.service.bean.InviteErrorCode;
import com.genersoft.iot.vmp.storager.IVideoManagerStorage;
import com.genersoft.iot.vmp.storager.dao.DeviceChannelMapper;
import com.genersoft.iot.vmp.storager.dao.DeviceMapper;
import com.genersoft.iot.vmp.utils.DateUtil;
import com.genersoft.iot.vmp.vmanager.bean.ErrorCode;
import com.genersoft.iot.vmp.vmanager.bean.StreamContent;
import com.genersoft.iot.vmp.vmanager.bean.WVPResult;
import com.genersoft.iot.vmp.vmanager.gb28181.ptz.PtzController;
import com.genersoft.iot.vmp.vmanager.gb28181.record.GBRecordController;
import com.genersoft.iot.vmp.vmanager.gb28181.playback.PlaybackController;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.mqttv5.client.*;
import org.eclipse.paho.mqttv5.client.persist.MemoryPersistence;
import org.eclipse.paho.mqttv5.common.MqttException;
import org.eclipse.paho.mqttv5.common.MqttMessage;
import org.eclipse.paho.mqttv5.common.MqttPersistenceException;
import org.eclipse.paho.mqttv5.common.packet.MqttProperties;
import org.eclipse.paho.mqttv5.common.packet.UserProperty;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import org.springframework.web.context.request.async.DeferredResult;

import javax.annotation.PostConstruct;
import javax.servlet.http.HttpServletRequest;
import javax.sip.InvalidArgumentException;
import javax.sip.SipException;
import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.ParseException;
import java.util.*;
import java.util.function.Consumer;

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
    public void playbackStart1(String replyTopic, String requestId, String deviceId, String channelId, String startTime, String endTime, MqttProperties replyMqttProperties) {
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
                contentData.put("data",result);
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
    public void playbackStart(String replyTopic, String requestId, String deviceId, String channelId, String startTime, String endTime, MqttProperties replyMqttProperties) {
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
    public void recordPlay1(String replyTopic, String requestId, String deviceId, String channelId, String startTime, String endTime, MqttProperties replyMqttProperties) {
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
    public void recordPlay(String replyTopic, String requestId, String deviceId, String channelId, String startTime, String endTime, MqttProperties replyMqttProperties) {
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
    public static void publish(String topic, String pushMessage, int qos, boolean retained, MqttProperties properties) throws Exception {
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
}
