package com.genersoft.iot.vmp.gb28181.transmit.cmd.impl;

import com.alibaba.fastjson2.JSON;
import com.genersoft.iot.vmp.common.InviteSessionType;
import com.genersoft.iot.vmp.conf.DynamicTask;
import com.genersoft.iot.vmp.conf.UserSetting;
import com.genersoft.iot.vmp.conf.exception.SsrcTransactionNotFoundException;
import com.genersoft.iot.vmp.gb28181.SipLayer;
import com.genersoft.iot.vmp.gb28181.bean.*;
import com.genersoft.iot.vmp.gb28181.event.SipSubscribe;
import com.genersoft.iot.vmp.gb28181.session.VideoStreamSessionManager;
import com.genersoft.iot.vmp.gb28181.transmit.SIPSender;
import com.genersoft.iot.vmp.gb28181.transmit.cmd.ISIPCommanderForPlatform;
import com.genersoft.iot.vmp.gb28181.transmit.cmd.SIPRequestHeaderPlarformProvider;
import com.genersoft.iot.vmp.gb28181.utils.SipUtils;
import com.genersoft.iot.vmp.media.bean.MediaServer;
import com.genersoft.iot.vmp.media.event.hook.Hook;
import com.genersoft.iot.vmp.media.event.hook.HookSubscribe;
import com.genersoft.iot.vmp.media.event.hook.HookType;
import com.genersoft.iot.vmp.media.service.IMediaServerService;
import com.genersoft.iot.vmp.service.bean.GPSMsgInfo;
import com.genersoft.iot.vmp.service.bean.SSRCInfo;
import com.genersoft.iot.vmp.storager.IRedisCatchStorage;
import com.genersoft.iot.vmp.storager.dao.dto.PlatformRegisterInfo;
import com.genersoft.iot.vmp.utils.DateUtil;
import com.genersoft.iot.vmp.utils.GitUtil;
import gov.nist.javax.sip.message.MessageFactoryImpl;
import gov.nist.javax.sip.message.SIPRequest;
import gov.nist.javax.sip.message.SIPResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.DependsOn;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;
import org.springframework.util.ObjectUtils;

import javax.sip.InvalidArgumentException;
import javax.sip.ResponseEvent;
import javax.sip.SipException;
import javax.sip.SipFactory;
import javax.sip.header.CallIdHeader;
import javax.sip.header.WWWAuthenticateHeader;
import javax.sip.message.Request;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

@Component
@DependsOn("sipLayer")
public class SIPCommanderFroPlatform implements ISIPCommanderForPlatform {

    private final Logger logger = LoggerFactory.getLogger(SIPCommanderFroPlatform.class);

    @Autowired
    private SIPRequestHeaderPlarformProvider headerProviderPlatformProvider;

    @Autowired
    private IRedisCatchStorage redisCatchStorage;

    @Autowired
    private IMediaServerService mediaServerService;

    @Autowired
    private SipSubscribe sipSubscribe;

    @Autowired
    private SipLayer sipLayer;

    @Autowired
    private SIPSender sipSender;

    @Autowired
    private HookSubscribe subscribe;

    @Autowired
    private UserSetting userSetting;


    @Autowired
    private VideoStreamSessionManager streamSession;

    @Autowired
    private DynamicTask dynamicTask;

    @Autowired
    private GitUtil gitUtil;

    @Override
    public void register(ParentPlatform parentPlatform, SipSubscribe.Event errorEvent , SipSubscribe.Event okEvent) throws InvalidArgumentException, ParseException, SipException {
        register(parentPlatform, null, null, errorEvent, okEvent, true);
    }

    @Override
    public void register(ParentPlatform parentPlatform, SipTransactionInfo sipTransactionInfo, SipSubscribe.Event errorEvent , SipSubscribe.Event okEvent) throws InvalidArgumentException, ParseException, SipException {

        register(parentPlatform, sipTransactionInfo, null, errorEvent, okEvent, true);
    }

    @Override
    public void unregister(ParentPlatform parentPlatform, SipTransactionInfo sipTransactionInfo, SipSubscribe.Event errorEvent , SipSubscribe.Event okEvent) throws InvalidArgumentException, ParseException, SipException {
        register(parentPlatform, sipTransactionInfo, null, errorEvent, okEvent, false);
    }

    @Override
    public void register(ParentPlatform parentPlatform, @Nullable SipTransactionInfo sipTransactionInfo, @Nullable WWWAuthenticateHeader www,
                            SipSubscribe.Event errorEvent , SipSubscribe.Event okEvent, boolean isRegister) throws SipException, InvalidArgumentException, ParseException {
            Request request;

            CallIdHeader callIdHeader = sipSender.getNewCallIdHeader(parentPlatform.getDeviceIp(),parentPlatform.getTransport());
            String fromTag = SipUtils.getNewFromTag();
            String toTag = null;
            if (sipTransactionInfo != null ) {
                if (sipTransactionInfo.getCallId() != null) {
                    callIdHeader.setCallId(sipTransactionInfo.getCallId());
                }
                if (sipTransactionInfo.getFromTag() != null) {
                    fromTag = sipTransactionInfo.getFromTag();
                }
                if (sipTransactionInfo.getToTag() != null) {
                    toTag = sipTransactionInfo.getToTag();
                }
            }

            if (www == null ) {
                request = headerProviderPlatformProvider.createRegisterRequest(parentPlatform,
                        redisCatchStorage.getCSEQ(), fromTag,
                        toTag, callIdHeader, isRegister? parentPlatform.getExpires() : 0);
                // 将 callid 写入缓存， 等注册成功可以更新状态
                String callIdFromHeader = callIdHeader.getCallId();
                redisCatchStorage.updatePlatformRegisterInfo(callIdFromHeader, PlatformRegisterInfo.getInstance(parentPlatform.getServerGBId(), isRegister));

                sipSubscribe.addErrorSubscribe(callIdHeader.getCallId(), (event)->{
                    if (event != null) {
                        logger.info("向上级平台 [ {} ] 注册发生错误： {} ",
                                parentPlatform.getServerGBId(),
                                event.msg);
                    }
                    redisCatchStorage.delPlatformRegisterInfo(callIdFromHeader);
                    if (errorEvent != null ) {
                        errorEvent.response(event);
                    }
                });

            }else {
                request = headerProviderPlatformProvider.createRegisterRequest(parentPlatform, fromTag, toTag, www, callIdHeader, isRegister? parentPlatform.getExpires() : 0);
            }

            sipSender.transmitRequest(parentPlatform.getDeviceIp(), request, null, okEvent);
    }

    @Override
    public String keepalive(ParentPlatform parentPlatform,SipSubscribe.Event errorEvent , SipSubscribe.Event okEvent) throws SipException, InvalidArgumentException, ParseException {
            String characterSet = parentPlatform.getCharacterSet();
            StringBuffer keepaliveXml = new StringBuffer(200);
            keepaliveXml.append("<?xml version=\"1.0\" encoding=\"")
                    .append(characterSet).append("\"?>\r\n")
                    .append("<Notify>\r\n")
                    .append("<CmdType>Keepalive</CmdType>\r\n")
                    .append("<SN>" + (int)((Math.random()*9+1)*100000) + "</SN>\r\n")
                    .append("<DeviceID>" + parentPlatform.getDeviceGBId() + "</DeviceID>\r\n")
                    .append("<Status>OK</Status>\r\n")
                    .append("</Notify>\r\n");

        CallIdHeader callIdHeader = sipSender.getNewCallIdHeader(parentPlatform.getDeviceIp(),parentPlatform.getTransport());

        Request request = headerProviderPlatformProvider.createMessageRequest(
                parentPlatform,
                keepaliveXml.toString(),
                SipUtils.getNewFromTag(),
                SipUtils.getNewViaTag(),
                callIdHeader);
        sipSender.transmitRequest(parentPlatform.getDeviceIp(), request, errorEvent, okEvent);
        return callIdHeader.getCallId();
    }

    /**
     * 向上级回复通道信息
     * @param channel 通道信息
     * @param parentPlatform 平台信息
     */
    @Override
    public void catalogQuery(DeviceChannel channel, ParentPlatform parentPlatform, String sn, String fromTag, int size) throws SipException, InvalidArgumentException, ParseException {

        if ( parentPlatform ==null) {
            return ;
        }
        List<DeviceChannel> channels = new ArrayList<>();
        if (channel != null) {
            channels.add(channel);
        }
        String catalogXml = getCatalogXml(channels, sn, parentPlatform, size);

        // callid
        CallIdHeader callIdHeader = sipSender.getNewCallIdHeader(parentPlatform.getDeviceIp(),parentPlatform.getTransport());

        Request request = headerProviderPlatformProvider.createMessageRequest(parentPlatform, catalogXml.toString(), fromTag, SipUtils.getNewViaTag(), callIdHeader);
        sipSender.transmitRequest(parentPlatform.getDeviceIp(), request);

    }

    @Override
    public void catalogQuery(List<DeviceChannel> channels, ParentPlatform parentPlatform, String sn, String fromTag) throws InvalidArgumentException, ParseException, SipException {
        if ( parentPlatform ==null) {
            return ;
        }
        sendCatalogResponse(channels, parentPlatform, sn, fromTag, 0, true);
    }
    private String getCatalogXml(List<DeviceChannel> channels, String sn, ParentPlatform parentPlatform, int size) {
        String characterSet = parentPlatform.getCharacterSet();
        StringBuffer catalogXml = new StringBuffer(600);
        catalogXml.append("<?xml version=\"1.0\" encoding=\"" + characterSet +"\"?>\r\n")
                .append("<Response>\r\n")
                .append("<CmdType>Catalog</CmdType>\r\n")
                .append("<SN>" +sn + "</SN>\r\n")
                .append("<DeviceID>" + parentPlatform.getDeviceGBId() + "</DeviceID>\r\n")
                .append("<SumNum>" + size + "</SumNum>\r\n")
                .append("<DeviceList Num=\"" + channels.size() +"\">\r\n");
        if (channels.size() > 0) {
            for (DeviceChannel channel : channels) {
                if (parentPlatform.getServerGBId().equals(channel.getParentId())) {
                    channel.setParentId(parentPlatform.getDeviceGBId());
                }
                catalogXml.append("<Item>\r\n");
                // 行政区划分组只需要这两项就可以
                catalogXml.append("<DeviceID>" + channel.getChannelId() + "</DeviceID>\r\n");
                catalogXml.append("<Name>" + channel.getName() + "</Name>\r\n");
                if (channel.getChannelId().length() <= 8) {
                    catalogXml.append("</Item>\r\n");
                    continue;
                }else {
                    if (channel.getChannelId().length() != 20) {
                        catalogXml.append("</Item>\r\n");
                        logger.warn("[编号长度异常] {} 长度错误，请使用20位长度的国标编号,当前长度：{}", channel.getChannelId(), channel.getChannelId().length());
                        catalogXml.append("</Item>\r\n");
                        continue;
                    }
                    switch (Integer.parseInt(channel.getChannelId().substring(10, 13))){
                        case 200:
//                            catalogXml.append("<Manufacturer>三永华通</Manufacturer>\r\n");
//                            GitUtil gitUtil = SpringBeanFactory.getBean("gitUtil");
//                            String model = (gitUtil == null || gitUtil.getBuildVersion() == null)?"1.0": gitUtil.getBuildVersion();
//                            catalogXml.append("<Model>" + model + "</Manufacturer>\r\n");
//                            catalogXml.append("<Owner>三永华通</Owner>\r\n");
                             if (channel.getCivilCode() != null) {
                                 catalogXml.append("<CivilCode>"+channel.getCivilCode()+"</CivilCode>\r\n");
                             }else {
                                 catalogXml.append("<CivilCode></CivilCode>\r\n");
                             }

                            catalogXml.append("<RegisterWay>1</RegisterWay>\r\n");
                            catalogXml.append("<Secrecy>0</Secrecy>\r\n");
                            break;
                        case 215:
                            if (!ObjectUtils.isEmpty(channel.getParentId())) {
                                catalogXml.append("<ParentID>" + channel.getParentId() + "</ParentID>\r\n");
                            }

                            break;
                        case 216:
                            if (!ObjectUtils.isEmpty(channel.getParentId())) {
                                catalogXml.append("<ParentID>" + channel.getParentId() + "</ParentID>\r\n");
                            }else {
                                catalogXml.append("<ParentID></ParentID>\r\n");
                            }
                            if (!ObjectUtils.isEmpty(channel.getBusinessGroupId())) {
                                catalogXml.append("<BusinessGroupID>" + channel.getBusinessGroupId() + "</BusinessGroupID>\r\n");
                            }else {
                                catalogXml.append("<BusinessGroupID></BusinessGroupID>\r\n");
                            }
                            break;
                        default:
                            // 通道项
                            if (channel.getManufacture() != null) {
                                catalogXml.append("<Manufacturer>" + channel.getManufacture() + "</Manufacturer>\r\n");
                            }else {
                                catalogXml.append("<Manufacturer></Manufacturer>\r\n");
                            }
                            if (channel.getSecrecy() != null) {
                                catalogXml.append("<Secrecy>" + channel.getSecrecy() + "</Secrecy>\r\n");
                            }else {
                                catalogXml.append("<Secrecy></Secrecy>\r\n");
                            }
                            catalogXml.append("<RegisterWay>" + channel.getRegisterWay() + "</RegisterWay>\r\n");
                            if (channel.getModel() != null) {
                                catalogXml.append("<Model>" + channel.getModel() + "</Model>\r\n");
                            }else {
                                catalogXml.append("<Model></Model>\r\n");
                            }
                            if (channel.getOwner() != null) {
                                catalogXml.append("<Owner>" + channel.getOwner()+ "</Owner>\r\n");
                            }else {
                                catalogXml.append("<Owner></Owner>\r\n");
                            }
                            if (channel.getCivilCode() != null) {
                                catalogXml.append("<CivilCode>" + channel.getCivilCode() + "</CivilCode>\r\n");
                            }else {
                                catalogXml.append("<CivilCode></CivilCode>\r\n");
                            }
                            if (channel.getAddress() == null) {
                                catalogXml.append("<Address></Address>\r\n");
                            }else {
                                catalogXml.append("<Address>" + channel.getAddress() + "</Address>\r\n");
                            }
                            if (!ObjectUtils.isEmpty(channel.getParentId())) {
                                catalogXml.append("<ParentID>" + channel.getParentId() + "</ParentID>\r\n");
                            }else {
                                catalogXml.append("<ParentID></ParentID>\r\n");
                            }
                            if (!ObjectUtils.isEmpty(channel.getBlock())) {
                                catalogXml.append("<Block>" + channel.getBlock() + "</Block>\r\n");
                            }else {
                                catalogXml.append("<Block></Block>\r\n");
                            }
                            if (!ObjectUtils.isEmpty(channel.getSafetyWay())) {
                                catalogXml.append("<SafetyWay>" + channel.getSafetyWay() + "</SafetyWay>\r\n");
                            }else {
                                catalogXml.append("<SafetyWay></SafetyWay>\r\n");
                            }
                            if (!ObjectUtils.isEmpty(channel.getCertNum())) {
                                catalogXml.append("<CertNum>" + channel.getCertNum() + "</CertNum>\r\n");
                            }else {
                                catalogXml.append("<CertNum></CertNum>\r\n");
                            }
                            if (!ObjectUtils.isEmpty(channel.getCertifiable())) {
                                catalogXml.append("<Certifiable>" + channel.getCertifiable() + "</Certifiable>\r\n");
                            }else {
                                catalogXml.append("<Certifiable></Certifiable>\r\n");
                            }
                            if (!ObjectUtils.isEmpty(channel.getErrCode())) {
                                catalogXml.append("<ErrCode>" + channel.getErrCode() + "</ErrCode>\r\n");
                            }else {
                                catalogXml.append("<ErrCode></ErrCode>\r\n");
                            }
                            if (!ObjectUtils.isEmpty(channel.getEndTime())) {
                                catalogXml.append("<EndTime>" + channel.getEndTime() + "</EndTime>\r\n");
                            }else {
                                catalogXml.append("<EndTime></EndTime>\r\n");
                            }
                            if (!ObjectUtils.isEmpty(channel.getSecrecy())) {
                                catalogXml.append("<Secrecy>" + channel.getSecrecy() + "</Secrecy>\r\n");
                            }else {
                                catalogXml.append("<Secrecy></Secrecy>\r\n");
                            }
                            if (!ObjectUtils.isEmpty(channel.getIpAddress())) {
                                catalogXml.append("<IPAddress>" + channel.getIpAddress() + "</IPAddress>\r\n");
                            }else {
                                catalogXml.append("<IPAddress></IPAddress>\r\n");
                            }
                            catalogXml.append("<Port>" + channel.getPort() + "</Port>\r\n");
                            if (!ObjectUtils.isEmpty(channel.getPassword())) {
                                catalogXml.append("<Password>" + channel.getPassword() + "</Password>\r\n");
                            }else {
                                catalogXml.append("<Password></Password>\r\n");
                            }
                            if (!ObjectUtils.isEmpty(channel.getPtzType())) {
                                catalogXml.append("<PTZType>" + channel.getPtzType() + "</PTZType>\r\n");
                            }else {
                                catalogXml.append("<PTZType></PTZType>\r\n");
                            }
                            catalogXml.append("<Status>" + (channel.isStatus() ?"ON":"OFF") + "</Status>\r\n");

                            catalogXml.append("<Longitude>" +
                                    (channel.getLongitudeWgs84() != 0? channel.getLongitudeWgs84():channel.getLongitude())
                                    + "</Longitude>\r\n");
                            catalogXml.append("<Latitude>" +
                                    (channel.getLatitudeWgs84() != 0? channel.getLatitudeWgs84():channel.getLatitude())
                                    + "</Latitude>\r\n");
                            break;

                    }
                    catalogXml.append("</Item>\r\n");
                }
            }
        }

        catalogXml.append("</DeviceList>\r\n");
        catalogXml.append("</Response>\r\n");
        return catalogXml.toString();
    }

    private void sendCatalogResponse(List<DeviceChannel> channels, ParentPlatform parentPlatform, String sn, String fromTag, int index, boolean sendAfterResponse) throws SipException, InvalidArgumentException, ParseException {
        if (index >= channels.size()) {
            return;
        }
        List<DeviceChannel> deviceChannels;
        if (index + parentPlatform.getCatalogGroup() < channels.size()) {
            deviceChannels = channels.subList(index, index + parentPlatform.getCatalogGroup());
        }else {
            deviceChannels = channels.subList(index, channels.size());
        }
        String catalogXml = getCatalogXml(deviceChannels, sn, parentPlatform, channels.size());
        // callid
        CallIdHeader callIdHeader = sipSender.getNewCallIdHeader(parentPlatform.getDeviceIp(),parentPlatform.getTransport());

        SIPRequest request = (SIPRequest)headerProviderPlatformProvider.createMessageRequest(parentPlatform, catalogXml, fromTag, SipUtils.getNewViaTag(), callIdHeader);

        String timeoutTaskKey = "catalog_task_" + parentPlatform.getServerGBId() + sn;

        String callId = request.getCallIdHeader().getCallId();

        logger.info("[命令发送] 国标级联{} 目录查询回复: 共{}条，已发送{}条", parentPlatform.getServerGBId(),
                channels.size(), Math.min(index + parentPlatform.getCatalogGroup(), channels.size()));
        logger.debug(catalogXml);
        if (sendAfterResponse) {
            // 默认按照收到200回复后发送下一条， 如果超时收不到回复，就以30毫秒的间隔直接发送。
            dynamicTask.startDelay(timeoutTaskKey, ()->{
                sipSubscribe.removeOkSubscribe(callId);
                int indexNext = index + parentPlatform.getCatalogGroup();
                try {
                    sendCatalogResponse(channels, parentPlatform, sn, fromTag, indexNext, false);
                } catch (SipException | InvalidArgumentException | ParseException e) {
                    logger.error("[命令发送失败] 国标级联 目录查询回复: {}", e.getMessage());
                }
            }, 3000);
            sipSender.transmitRequest(parentPlatform.getDeviceIp(), request, eventResult -> {
                logger.error("[目录推送失败] 国标级联 platform : {}, code: {}, msg: {}, 停止发送", parentPlatform.getServerGBId(), eventResult.statusCode, eventResult.msg);
                dynamicTask.stop(timeoutTaskKey);
            }, eventResult -> {
                dynamicTask.stop(timeoutTaskKey);
                int indexNext = index + parentPlatform.getCatalogGroup();
                try {
                    sendCatalogResponse(channels, parentPlatform, sn, fromTag, indexNext, true);
                } catch (SipException | InvalidArgumentException | ParseException e) {
                    logger.error("[命令发送失败] 国标级联 目录查询回复: {}", e.getMessage());
                }
            });
        }else {
            sipSender.transmitRequest(parentPlatform.getDeviceIp(), request, eventResult -> {
                logger.error("[目录推送失败] 国标级联 platform : {}, code: {}, msg: {}, 停止发送", parentPlatform.getServerGBId(), eventResult.statusCode, eventResult.msg);
                dynamicTask.stop(timeoutTaskKey);
            }, null);
            dynamicTask.startDelay(timeoutTaskKey, ()->{
                int indexNext = index + parentPlatform.getCatalogGroup();
                try {
                    sendCatalogResponse(channels, parentPlatform, sn, fromTag, indexNext, false);
                } catch (SipException | InvalidArgumentException | ParseException e) {
                    logger.error("[命令发送失败] 国标级联 目录查询回复: {}", e.getMessage());
                }
            }, 30);
        }
    }

    /**
     * 向上级回复DeviceInfo查询信息
     * @param parentPlatform 平台信息
     * @param sn
     * @param fromTag
     * @return
     */
    @Override
    public void deviceInfoResponse(ParentPlatform parentPlatform,Device device, String sn, String fromTag) throws SipException, InvalidArgumentException, ParseException {
        if (parentPlatform == null) {
            return;
        }
        String deviceId = device == null ? parentPlatform.getDeviceGBId() : device.getDeviceId();
        String deviceName = device == null ? parentPlatform.getName() : device.getName();
        String manufacturer = device == null ? "WVP-28181-PRO" : device.getManufacturer();
        String model = device == null ? "platform" : device.getModel();
        String firmware = device == null ? gitUtil.getBuildVersion() : device.getFirmware();
        String characterSet = parentPlatform.getCharacterSet();
        StringBuffer deviceInfoXml = new StringBuffer(600);
        deviceInfoXml.append("<?xml version=\"1.0\" encoding=\"" + characterSet + "\"?>\r\n");
        deviceInfoXml.append("<Response>\r\n");
        deviceInfoXml.append("<CmdType>DeviceInfo</CmdType>\r\n");
        deviceInfoXml.append("<SN>" +sn + "</SN>\r\n");
        deviceInfoXml.append("<DeviceID>" + deviceId + "</DeviceID>\r\n");
        deviceInfoXml.append("<DeviceName>" + deviceName + "</DeviceName>\r\n");
        deviceInfoXml.append("<Manufacturer>" + manufacturer + "</Manufacturer>\r\n");
        deviceInfoXml.append("<Model>" + model + "</Model>\r\n");
        deviceInfoXml.append("<Firmware>" + firmware + "</Firmware>\r\n");
        deviceInfoXml.append("<Result>OK</Result>\r\n");
        deviceInfoXml.append("</Response>\r\n");

        CallIdHeader callIdHeader = sipSender.getNewCallIdHeader(parentPlatform.getDeviceIp(),parentPlatform.getTransport());

        Request request = headerProviderPlatformProvider.createMessageRequest(parentPlatform, deviceInfoXml.toString(), fromTag, SipUtils.getNewViaTag(), callIdHeader);
        sipSender.transmitRequest(parentPlatform.getDeviceIp(), request);
    }


    /**
     * 向上级回复DeviceStatus查询信息
     * @param parentPlatform 平台信息
     * @param sn
     * @param fromTag
     * @return
     */
    @Override
    public void deviceStatusResponse(ParentPlatform parentPlatform,String channelId, String sn, String fromTag,boolean status) throws SipException, InvalidArgumentException, ParseException {
        if (parentPlatform == null) {
            return ;
        }
        String statusStr = (status)?"ONLINE":"OFFLINE";
        String characterSet = parentPlatform.getCharacterSet();
        StringBuffer deviceStatusXml = new StringBuffer(600);
        deviceStatusXml.append("<?xml version=\"1.0\" encoding=\"" + characterSet + "\"?>\r\n")
                .append("<Response>\r\n")
                .append("<CmdType>DeviceStatus</CmdType>\r\n")
                .append("<SN>" +sn + "</SN>\r\n")
                .append("<DeviceID>" + channelId + "</DeviceID>\r\n")
                .append("<Result>OK</Result>\r\n")
                .append("<Online>"+statusStr+"</Online>\r\n")
                .append("<Status>OK</Status>\r\n")
                .append("</Response>\r\n");

        CallIdHeader callIdHeader = sipSender.getNewCallIdHeader(parentPlatform.getDeviceIp(),parentPlatform.getTransport());

        Request request = headerProviderPlatformProvider.createMessageRequest(parentPlatform, deviceStatusXml.toString(), fromTag, SipUtils.getNewViaTag(), callIdHeader);
        sipSender.transmitRequest(parentPlatform.getDeviceIp(), request);
    }

    @Override
    public void sendNotifyMobilePosition(ParentPlatform parentPlatform, GPSMsgInfo gpsMsgInfo, SubscribeInfo subscribeInfo) throws InvalidArgumentException, ParseException, NoSuchFieldException, SipException, IllegalAccessException {
        if (parentPlatform == null) {
            return;
        }
        if (logger.isDebugEnabled()) {
            logger.debug("[发送 移动位置订阅] {}/{}->{},{}", parentPlatform.getServerGBId(), gpsMsgInfo.getId(), gpsMsgInfo.getLng(), gpsMsgInfo.getLat());
        }

        String characterSet = parentPlatform.getCharacterSet();
        StringBuffer deviceStatusXml = new StringBuffer(600);
        deviceStatusXml.append("<?xml version=\"1.0\" encoding=\"" + characterSet + "\"?>\r\n")
                .append("<Notify>\r\n")
                .append("<CmdType>MobilePosition</CmdType>\r\n")
                .append("<SN>" + (int)((Math.random()*9+1)*100000) + "</SN>\r\n")
                .append("<DeviceID>" + gpsMsgInfo.getId() + "</DeviceID>\r\n")
                .append("<Time>" + gpsMsgInfo.getTime() + "</Time>\r\n")
                .append("<Longitude>" + gpsMsgInfo.getLng() + "</Longitude>\r\n")
                .append("<Latitude>" + gpsMsgInfo.getLat() + "</Latitude>\r\n")
                .append("<Speed>" + gpsMsgInfo.getSpeed() + "</Speed>\r\n")
                .append("<Direction>" + gpsMsgInfo.getDirection() + "</Direction>\r\n")
                .append("<Altitude>" + gpsMsgInfo.getAltitude() + "</Altitude>\r\n")
                .append("</Notify>\r\n");

       sendNotify(parentPlatform, deviceStatusXml.toString(), subscribeInfo, eventResult -> {
            logger.error("发送NOTIFY通知消息失败。错误：{} {}", eventResult.statusCode, eventResult.msg);
        }, null);

    }

    @Override
    public void sendAlarmMessage(ParentPlatform parentPlatform, DeviceAlarm deviceAlarm) throws SipException, InvalidArgumentException, ParseException {
        if (parentPlatform == null) {
            return;
        }
        logger.info("[发送报警通知]平台： {}/{}->{},{}: {}", parentPlatform.getServerGBId(), deviceAlarm.getChannelId(),
                deviceAlarm.getLongitude(), deviceAlarm.getLatitude(), JSON.toJSONString(deviceAlarm));
        String characterSet = parentPlatform.getCharacterSet();
        StringBuffer deviceStatusXml = new StringBuffer(600);
        deviceStatusXml.append("<?xml version=\"1.0\" encoding=\"" + characterSet + "\"?>\r\n")
                .append("<Notify>\r\n")
                .append("<CmdType>Alarm</CmdType>\r\n")
                .append("<SN>" + (int)((Math.random()*9+1)*100000) + "</SN>\r\n")
                .append("<DeviceID>" + deviceAlarm.getChannelId() + "</DeviceID>\r\n")
                .append("<AlarmPriority>" + deviceAlarm.getAlarmPriority() + "</AlarmPriority>\r\n")
                .append("<AlarmMethod>" + deviceAlarm.getAlarmMethod() + "</AlarmMethod>\r\n")
                .append("<AlarmTime>" + DateUtil.yyyy_MM_dd_HH_mm_ssToISO8601(deviceAlarm.getAlarmTime()) + "</AlarmTime>\r\n")
                .append("<AlarmDescription>" + deviceAlarm.getAlarmDescription() + "</AlarmDescription>\r\n")
                .append("<Longitude>" + deviceAlarm.getLongitude() + "</Longitude>\r\n")
                .append("<Latitude>" + deviceAlarm.getLatitude() + "</Latitude>\r\n")
                .append("<info>\r\n")
                .append("<AlarmType>" + deviceAlarm.getAlarmType() + "</AlarmType>\r\n")
                .append("</info>\r\n")
                .append("</Notify>\r\n");

        CallIdHeader callIdHeader = sipSender.getNewCallIdHeader(parentPlatform.getDeviceIp(),parentPlatform.getTransport());

        Request request = headerProviderPlatformProvider.createMessageRequest(parentPlatform, deviceStatusXml.toString(), SipUtils.getNewFromTag(), SipUtils.getNewViaTag(), callIdHeader);
        sipSender.transmitRequest(parentPlatform.getDeviceIp(), request);

    }

    @Override
    public void sendNotifyForCatalogAddOrUpdate(String type, ParentPlatform parentPlatform, List<DeviceChannel> deviceChannels, SubscribeInfo subscribeInfo, Integer index) throws InvalidArgumentException, ParseException, NoSuchFieldException, SipException, IllegalAccessException {
        if (parentPlatform == null || deviceChannels == null || deviceChannels.isEmpty() || subscribeInfo == null) {
            return;
        }
        if (index == null) {
            index = 0;
        }
        if (index >= deviceChannels.size()) {
            return;
        }
        List<DeviceChannel> channels;
        if (index + parentPlatform.getCatalogGroup() < deviceChannels.size()) {
            channels = deviceChannels.subList(index, index + parentPlatform.getCatalogGroup());
        }else {
            channels = deviceChannels.subList(index, deviceChannels.size());
        }
        Integer finalIndex = index;
        String catalogXmlContent = getCatalogXmlContentForCatalogAddOrUpdate(parentPlatform, channels,
                deviceChannels.size(), type, subscribeInfo);
        logger.info("[发送NOTIFY通知]类型： {}，发送数量： {}", type, channels.size());
        sendNotify(parentPlatform, catalogXmlContent, subscribeInfo, eventResult -> {
            logger.error("发送NOTIFY通知消息失败。错误：{} {}", eventResult.statusCode, eventResult.msg);
        }, (eventResult -> {
            try {
                sendNotifyForCatalogAddOrUpdate(type, parentPlatform, deviceChannels, subscribeInfo,
                        finalIndex + parentPlatform.getCatalogGroup());
            } catch (InvalidArgumentException | ParseException | NoSuchFieldException | SipException |
                     IllegalAccessException e) {
                logger.error("[命令发送失败] 国标级联 NOTIFY通知: {}", e.getMessage());
            }
        }));
    }

    private void sendNotify(ParentPlatform parentPlatform, String catalogXmlContent,
                                   SubscribeInfo subscribeInfo, SipSubscribe.Event errorEvent,  SipSubscribe.Event okEvent )
            throws SipException, ParseException, InvalidArgumentException {
        MessageFactoryImpl messageFactory = (MessageFactoryImpl) SipFactory.getInstance().createMessageFactory();
        String characterSet = parentPlatform.getCharacterSet();
        // 设置编码， 防止中文乱码
        messageFactory.setDefaultContentEncodingCharset(characterSet);

        SIPRequest notifyRequest = headerProviderPlatformProvider.createNotifyRequest(parentPlatform, catalogXmlContent, subscribeInfo);

        sipSender.transmitRequest(parentPlatform.getDeviceIp(), notifyRequest, errorEvent, okEvent);
    }

    private  String getCatalogXmlContentForCatalogAddOrUpdate(ParentPlatform parentPlatform, List<DeviceChannel> channels, int sumNum, String type, SubscribeInfo subscribeInfo) {
        StringBuffer catalogXml = new StringBuffer(600);

        String characterSet = parentPlatform.getCharacterSet();
        catalogXml.append("<?xml version=\"1.0\" encoding=\"" + characterSet + "\"?>\r\n")
                .append("<Notify>\r\n")
                .append("<CmdType>Catalog</CmdType>\r\n")
                .append("<SN>" + (int) ((Math.random() * 9 + 1) * 100000) + "</SN>\r\n")
                .append("<DeviceID>" + parentPlatform.getDeviceGBId() + "</DeviceID>\r\n")
                .append("<SumNum>"+ sumNum +"</SumNum>\r\n")
                .append("<DeviceList Num=\"" + channels.size() + "\">\r\n");
        if (!channels.isEmpty()) {
            for (DeviceChannel channel : channels) {
                if (parentPlatform.getServerGBId().equals(channel.getParentId())) {
                    channel.setParentId(parentPlatform.getDeviceGBId());
                }
                catalogXml.append("<Item>\r\n");
                // 行政区划分组只需要这两项就可以
                catalogXml.append("<DeviceID>" + channel.getChannelId() + "</DeviceID>\r\n");
                catalogXml.append("<Name>" + channel.getName() + "</Name>\r\n");
                if (channel.getParentId() != null) {
                    // 业务分组加上这一项即可，提高兼容性，
                    catalogXml.append("<ParentID>" + channel.getParentId() + "</ParentID>\r\n");
                }
                if (channel.getChannelId().length() == 20 && Integer.parseInt(channel.getChannelId().substring(10, 13)) == 216) {
                    // 虚拟组织增加BusinessGroupID字段
                    catalogXml.append("<BusinessGroupID>" + channel.getParentId() + "</BusinessGroupID>\r\n");
                }
                catalogXml.append("<Parental>" + channel.getParental() + "</Parental>\r\n");
                if (channel.getParental() == 0) {
                    // 通道项
                    catalogXml.append("<Manufacturer>" + channel.getManufacture() + "</Manufacturer>\r\n")
                            .append("<Secrecy>" + channel.getSecrecy() + "</Secrecy>\r\n")
                            .append("<RegisterWay>" + channel.getRegisterWay() + "</RegisterWay>\r\n")
                            .append("<Status>" + (channel.isStatus() ? "ON" : "OFF") + "</Status>\r\n");

                    if (channel.getChannelType() != 2) {  // 业务分组/虚拟组织/行政区划 不设置以下属性
                        catalogXml.append("<Model>" + channel.getModel() + "</Model>\r\n")
                                .append("<Owner> " + channel.getOwner()+ "</Owner>\r\n")
                                .append("<CivilCode>" + channel.getCivilCode() + "</CivilCode>\r\n")
                                .append("<Address>" + channel.getAddress() + "</Address>\r\n");
                    }
                    if (!"presence".equals(subscribeInfo.getEventType())) {
                        catalogXml.append("<Event>" + type + "</Event>\r\n");
                    }

                }
                catalogXml.append("</Item>\r\n");
            }
        }
        catalogXml.append("</DeviceList>\r\n")
                .append("</Notify>\r\n");
        return catalogXml.toString();
    }

    @Override
    public void sendNotifyForCatalogOther(String type, ParentPlatform parentPlatform, List<DeviceChannel> deviceChannels,
                                             SubscribeInfo subscribeInfo, Integer index) throws InvalidArgumentException, ParseException, NoSuchFieldException, SipException, IllegalAccessException {
        if (parentPlatform == null
                || deviceChannels == null
                || deviceChannels.size() == 0
                || subscribeInfo == null) {
            logger.warn("[缺少必要参数]");
            return;
        }

        if (index == null) {
            index = 0;
        }
        if (index >= deviceChannels.size()) {
            return;
        }
        List<DeviceChannel> channels;
        if (index + parentPlatform.getCatalogGroup() < deviceChannels.size()) {
            channels = deviceChannels.subList(index, index + parentPlatform.getCatalogGroup());
        }else {
            channels = deviceChannels.subList(index, deviceChannels.size());
        }
        logger.info("[发送NOTIFY通知]类型： {}，发送数量： {}", type, channels.size());
        Integer finalIndex = index;
        String catalogXmlContent = getCatalogXmlContentForCatalogOther(parentPlatform, channels, type);
        sendNotify(parentPlatform, catalogXmlContent, subscribeInfo, eventResult -> {
            logger.error("发送NOTIFY通知消息失败。错误：{} {}", eventResult.statusCode, eventResult.msg);
        }, eventResult -> {
            try {
                sendNotifyForCatalogOther(type, parentPlatform, deviceChannels, subscribeInfo,
                        finalIndex + parentPlatform.getCatalogGroup());
            } catch (InvalidArgumentException | ParseException | NoSuchFieldException | SipException |
                     IllegalAccessException e) {
                logger.error("[命令发送失败] 国标级联 NOTIFY通知: {}", e.getMessage());
            }
        });
    }

    private String getCatalogXmlContentForCatalogOther(ParentPlatform parentPlatform, List<DeviceChannel> channels, String type) {

        String characterSet = parentPlatform.getCharacterSet();
        StringBuffer catalogXml = new StringBuffer(600);
        catalogXml.append("<?xml version=\"1.0\" encoding=\"" + characterSet + "\"?>\r\n")
                .append("<Notify>\r\n")
                .append("<CmdType>Catalog</CmdType>\r\n")
                .append("<SN>" + (int) ((Math.random() * 9 + 1) * 100000) + "</SN>\r\n")
                .append("<DeviceID>" + parentPlatform.getDeviceGBId() + "</DeviceID>\r\n")
                .append("<SumNum>1</SumNum>\r\n")
                .append("<DeviceList Num=\" " + channels.size() + " \">\r\n");
        if (channels.size() > 0) {
            for (DeviceChannel channel : channels) {
                if (parentPlatform.getServerGBId().equals(channel.getParentId())) {
                    channel.setParentId(parentPlatform.getDeviceGBId());
                }
                catalogXml.append("<Item>\r\n")
                        .append("<DeviceID>" + channel.getChannelId() + "</DeviceID>\r\n")
                        .append("<Event>" + type + "</Event>\r\n")
                        .append("</Item>\r\n");
            }
        }
        catalogXml.append("</DeviceList>\r\n")
                .append("</Notify>\r\n");
        return catalogXml.toString();
    }
    @Override
    public void recordInfo(DeviceChannel deviceChannel, ParentPlatform parentPlatform, String fromTag, RecordInfo recordInfo) throws SipException, InvalidArgumentException, ParseException {
        if ( parentPlatform ==null) {
            return ;
        }
        logger.info("[国标级联] 发送录像数据通道： {}", recordInfo.getChannelId());
        String characterSet = parentPlatform.getCharacterSet();
        StringBuffer recordXml = new StringBuffer(600);
        recordXml.append("<?xml version=\"1.0\" encoding=\"" + characterSet + "\"?>\r\n")
                .append("<Response>\r\n")
                .append("<CmdType>RecordInfo</CmdType>\r\n")
                .append("<SN>" +recordInfo.getSn() + "</SN>\r\n")
                .append("<DeviceID>" + deviceChannel.getChannelId() + "</DeviceID>\r\n")
                .append("<SumNum>" + recordInfo.getSumNum() + "</SumNum>\r\n");
        if (recordInfo.getRecordList() == null ) {
            recordXml.append("<RecordList Num=\"0\">\r\n");
        }else {
            recordXml.append("<RecordList Num=\"" + recordInfo.getRecordList().size()+"\">\r\n");
            if (recordInfo.getRecordList().size() > 0) {
                for (RecordItem recordItem : recordInfo.getRecordList()) {
                    recordXml.append("<Item>\r\n");
                    if (deviceChannel != null) {
                        recordXml.append("<DeviceID>" + deviceChannel.getChannelId() + "</DeviceID>\r\n")
                                .append("<Name>" + recordItem.getName() + "</Name>\r\n")
                                .append("<StartTime>" + DateUtil.yyyy_MM_dd_HH_mm_ssToISO8601(recordItem.getStartTime()) + "</StartTime>\r\n")
                                .append("<EndTime>" + DateUtil.yyyy_MM_dd_HH_mm_ssToISO8601(recordItem.getEndTime()) + "</EndTime>\r\n")
                                .append("<Secrecy>" + recordItem.getSecrecy() + "</Secrecy>\r\n")
                                .append("<Type>" + recordItem.getType() + "</Type>\r\n");
                        if (!ObjectUtils.isEmpty(recordItem.getFileSize())) {
                            recordXml.append("<FileSize>" + recordItem.getFileSize() + "</FileSize>\r\n");
                        }
                        if (!ObjectUtils.isEmpty(recordItem.getFilePath())) {
                            recordXml.append("<FilePath>" + recordItem.getFilePath() + "</FilePath>\r\n");
                        }
                    }
                    recordXml.append("</Item>\r\n");
                }
            }
        }

        recordXml.append("</RecordList>\r\n")
                .append("</Response>\r\n");
        logger.info("[国标级联] 发送录像数据通道：{}, 内容： {}", recordInfo.getChannelId(), recordXml);
        // callid
        CallIdHeader callIdHeader = sipSender.getNewCallIdHeader(parentPlatform.getDeviceIp(),parentPlatform.getTransport());

        Request request = headerProviderPlatformProvider.createMessageRequest(parentPlatform, recordXml.toString(), fromTag, SipUtils.getNewViaTag(), callIdHeader);
        sipSender.transmitRequest(parentPlatform.getDeviceIp(), request, null, eventResult -> {
            logger.info("[国标级联] 发送录像数据通道：{}, 发送成功", recordInfo.getChannelId());
        });

    }

    @Override
    public void sendMediaStatusNotify(ParentPlatform parentPlatform, SendRtpItem sendRtpItem) throws SipException, InvalidArgumentException, ParseException {
        if (sendRtpItem == null || parentPlatform == null) {
            return;
        }


        String characterSet = parentPlatform.getCharacterSet();
        StringBuffer mediaStatusXml = new StringBuffer(200);
        mediaStatusXml.append("<?xml version=\"1.0\" encoding=\"" + characterSet + "\"?>\r\n")
                .append("<Notify>\r\n")
                .append("<CmdType>MediaStatus</CmdType>\r\n")
                .append("<SN>" + (int)((Math.random()*9+1)*100000) + "</SN>\r\n")
                .append("<DeviceID>" + sendRtpItem.getChannelId() + "</DeviceID>\r\n")
                .append("<NotifyType>121</NotifyType>\r\n")
                .append("</Notify>\r\n");

        SIPRequest messageRequest = (SIPRequest)headerProviderPlatformProvider.createMessageRequest(parentPlatform, mediaStatusXml.toString(),
                sendRtpItem);

        sipSender.transmitRequest(parentPlatform.getDeviceIp(),messageRequest);

    }

    @Override
    public void streamByeCmd(ParentPlatform platform, String callId) throws SipException, InvalidArgumentException, ParseException {
        if (platform == null) {
            return;
        }
        SendRtpItem sendRtpItem = redisCatchStorage.querySendRTPServer(platform.getServerGBId(), null, null, callId);
        if (sendRtpItem != null) {
            streamByeCmd(platform, sendRtpItem);
        }
    }

    @Override
    public synchronized void streamByeCmd(ParentPlatform platform, SendRtpItem sendRtpItem) throws SipException, InvalidArgumentException, ParseException {
        if (sendRtpItem == null ) {
            logger.info("[向上级发送BYE]， sendRtpItem 为NULL");
            return;
        }
        if (platform == null) {
            logger.info("[向上级发送BYE]， platform 为NULL");
            return;
        }
        logger.info("[向上级发送BYE]， {}/{}", platform.getServerGBId(), sendRtpItem.getChannelId());
        String mediaServerId = sendRtpItem.getMediaServerId();
        MediaServer mediaServerItem = mediaServerService.getOne(mediaServerId);
        if (mediaServerItem != null) {
            mediaServerService.releaseSsrc(mediaServerItem.getId(), sendRtpItem.getSsrc());
            mediaServerService.closeRTPServer(mediaServerItem, sendRtpItem.getStream());
        }
        SIPRequest byeRequest = headerProviderPlatformProvider.createByeRequest(platform, sendRtpItem);
        if (byeRequest == null) {
            logger.warn("[向上级发送bye]：无法创建 byeRequest");
        }
        sipSender.transmitRequest(platform.getDeviceIp(),byeRequest);
    }

    @Override
    public void streamByeCmd(ParentPlatform platform, String channelId, String stream, String callId, SipSubscribe.Event okEvent) throws InvalidArgumentException, SipException, ParseException, SsrcTransactionNotFoundException {
        SsrcTransaction ssrcTransaction = streamSession.getSsrcTransaction(platform.getServerGBId(), channelId, callId, stream);
        if (ssrcTransaction == null) {
            throw new SsrcTransactionNotFoundException(platform.getServerGBId(), channelId, callId, stream);
        }

        mediaServerService.releaseSsrc(ssrcTransaction.getMediaServerId(), ssrcTransaction.getSsrc());
        mediaServerService.closeRTPServer(ssrcTransaction.getMediaServerId(), ssrcTransaction.getStream());
        streamSession.remove(ssrcTransaction.getDeviceId(), ssrcTransaction.getChannelId(), ssrcTransaction.getStream());

        Request byteRequest = headerProviderPlatformProvider.createByteRequest(platform, channelId, ssrcTransaction.getSipTransactionInfo());
        sipSender.transmitRequest(sipLayer.getLocalIp(platform.getDeviceIp()), byteRequest, null, okEvent);
    }

    @Override
    public void broadcastResultCmd(ParentPlatform platform, DeviceChannel deviceChannel, String sn, boolean result, SipSubscribe.Event errorEvent,  SipSubscribe.Event okEvent) throws InvalidArgumentException, SipException, ParseException {
        if (platform == null || deviceChannel == null) {
            return;
        }
        String characterSet = platform.getCharacterSet();
        StringBuffer mediaStatusXml = new StringBuffer(200);
        mediaStatusXml.append("<?xml version=\"1.0\" encoding=\"" + characterSet + "\"?>\r\n");
        mediaStatusXml.append("<Response>\r\n");
        mediaStatusXml.append("<CmdType>Broadcast</CmdType>\r\n");
        mediaStatusXml.append("<SN>" + sn + "</SN>\r\n");
        mediaStatusXml.append("<DeviceID>" + deviceChannel.getChannelId() + "</DeviceID>\r\n");
        mediaStatusXml.append("<Result>" + (result?"OK":"ERROR") + "</Result>\r\n");
        mediaStatusXml.append("</Response>\r\n");

        CallIdHeader callIdHeader = sipSender.getNewCallIdHeader(platform.getDeviceIp(), platform.getTransport());

        SIPRequest messageRequest = (SIPRequest)headerProviderPlatformProvider.createMessageRequest(platform, mediaStatusXml.toString(),
               SipUtils.getNewFromTag(), SipUtils.getNewViaTag(), callIdHeader);

        sipSender.transmitRequest(platform.getDeviceIp(),messageRequest, errorEvent, okEvent);
    }

    @Override
    public void broadcastInviteCmd(ParentPlatform platform, String channelId, MediaServer mediaServerItem,
                                   SSRCInfo ssrcInfo, HookSubscribe.Event event, SipSubscribe.Event okEvent,
                                   SipSubscribe.Event errorEvent) throws ParseException, SipException, InvalidArgumentException {
        String stream = ssrcInfo.getStream();

        if (platform == null) {
            return;
        }

        logger.info("{} 分配的ZLM为: {} [{}:{}]", stream, mediaServerItem.getId(), mediaServerItem.getIp(), ssrcInfo.getPort());
        Hook hook = Hook.getInstance(HookType.on_media_arrival, "rtp", stream, mediaServerItem.getId());
        subscribe.addSubscribe(hook, (hookData) -> {
            if (event != null) {
                event.response(hookData);
                subscribe.removeSubscribe(hook);
            }
        });
        String sdpIp = mediaServerItem.getSdpIp();

        StringBuffer content = new StringBuffer(200);
        content.append("v=0\r\n");
        content.append("o=" + channelId + " 0 0 IN IP4 " + sdpIp + "\r\n");
        content.append("s=Play\r\n");
        content.append("c=IN IP4 " + sdpIp + "\r\n");
        content.append("t=0 0\r\n");

        if ("TCP-PASSIVE".equalsIgnoreCase(userSetting.getBroadcastForPlatform())) {
            content.append("m=audio " + ssrcInfo.getPort() + " TCP/RTP/AVP 8 96\r\n");
        } else if ("TCP-ACTIVE".equalsIgnoreCase(userSetting.getBroadcastForPlatform())) {
            content.append("m=audio " + ssrcInfo.getPort() + " TCP/RTP/AVP 8 96\r\n");
        } else if ("UDP".equalsIgnoreCase(userSetting.getBroadcastForPlatform())) {
            content.append("m=audio " + ssrcInfo.getPort() + " RTP/AVP 8 96\r\n");
        }

        content.append("a=recvonly\r\n");
        content.append("a=rtpmap:8 PCMA/8000\r\n");
        content.append("a=rtpmap:96 PS/90000\r\n");
        if ("TCP-PASSIVE".equalsIgnoreCase(userSetting.getBroadcastForPlatform())) {
            content.append("a=setup:passive\r\n");
            content.append("a=connection:new\r\n");
        }else if ("TCP-ACTIVE".equalsIgnoreCase(userSetting.getBroadcastForPlatform())) {
            content.append("a=setup:active\r\n");
            content.append("a=connection:new\r\n");
        }

        content.append("y=" + ssrcInfo.getSsrc() + "\r\n");//ssrc
        CallIdHeader callIdHeader = sipSender.getNewCallIdHeader(sipLayer.getLocalIp(platform.getDeviceIp()), platform.getTransport());

        Request request = headerProviderPlatformProvider.createInviteRequest(platform, channelId,
                content.toString(), SipUtils.getNewViaTag(), SipUtils.getNewFromTag(),  ssrcInfo.getSsrc(),
                callIdHeader);
        sipSender.transmitRequest(sipLayer.getLocalIp(platform.getDeviceIp()), request, (e -> {
            streamSession.remove(platform.getServerGBId(), channelId, ssrcInfo.getStream());
            mediaServerService.releaseSsrc(mediaServerItem.getId(), ssrcInfo.getSsrc());
            subscribe.removeSubscribe(hook);
            errorEvent.response(e);
        }), e -> {
            ResponseEvent responseEvent = (ResponseEvent) e.event;
            SIPResponse response = (SIPResponse) responseEvent.getResponse();
            streamSession.put(platform.getServerGBId(), channelId, callIdHeader.getCallId(),  stream, ssrcInfo.getSsrc(), mediaServerItem.getId(), response, InviteSessionType.BROADCAST);
            okEvent.response(e);
        });
    }
}
