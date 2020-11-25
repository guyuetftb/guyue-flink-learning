package com.jerome.utils.enums;


/**
 * This is Description
 *
 * @author KONG BIN
 * @date 2019/12/12
 */
public enum KafkaVersionType {

    V_0102("0.10.2.0"),
    V_230("2.3.0");

    private String version;

    KafkaVersionType(String version) {
        this.version = version;
    }

    public static KafkaVersionType fromString(String type) {
        if (type == null) {
            throw new RuntimeException("null KafkaVersionType!");
        }
        String typeTmp = type.toUpperCase();

        if (!typeTmp.substring(0, 2).equals("V_")) {
            typeTmp = "V_" + typeTmp;
        }
        return valueOf(typeTmp.toUpperCase());
    }


}
