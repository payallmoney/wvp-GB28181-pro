package com.creallies.wvp.enums;

import lombok.Getter;

@Getter
public enum VideoTypeEnum {
    onlineStatus(1, "上下线状态"),
    ;

    final Integer value;
    final String text;

    VideoTypeEnum(Integer value, String text) {
        this.value = value;
        this.text = text;
    }

    public static String getText(String status) {
        for (VideoTypeEnum enumObj : VideoTypeEnum.values()) {
            if (enumObj.getValue().equals(status)) {
                return enumObj.getText();
            }
        }
        return null;
    }
}
