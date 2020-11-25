package com.jerome.utils.enums;

/**
 * This is Description
 *
 * @author KONG BIN
 * @date 2019/12/12
 */
public enum  KafkaClusterType {

    OFFLINE("OFFLINE"),
    OFFLINE_TEST("OFFLINE_TEST"),
    REALTIME("REALTIME"),
    REALTIME_TEST("REALTIME_TEST"),
    HOT_STANDBY("HOT_STANDBY"),
    HOT_STANDBY_TEST("HOT_STANDBY_TEST"),
    COLD_STANDBY("COLD_STANDBY"),
    COLD_STANDBY_TEST("COLD_STANDBY_TEST"),
    LOCAL("LOCAL");

    private String name;

    KafkaClusterType(String name){
        this.name = name;
    }

    public static KafkaClusterType fromString(String name){
        if (name == null){
            throw new RuntimeException("null KafkaClusterType");
        }
        return valueOf(name.toUpperCase());
    }

    public static void main(String[] args) {

        KafkaClusterType local = KafkaClusterType.fromString("local");
        String name = local.name;
        System.out.println(name);

    }

}
