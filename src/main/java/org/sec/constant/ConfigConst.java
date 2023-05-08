package org.sec.constant;

public enum ConfigConst {

    KAFKA_BROKER_URL("localhost:9092"),

    TOPICNAME("inventory_purchases");

    private String value;

    ConfigConst(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
