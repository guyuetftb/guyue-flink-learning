package com.jerome.utils.enums;

/**
 * This is Description
 *
 * @author Jerome丶子木
 * @date 2019/12/23
 */
public enum  SeparatorEnum {

    FLUME_HEAD_AND_BODY_SPLIT("#%&"),

    FLUME_HEAD_SPLIT("#@#"),

    COMMA(","),

    BLANK(" "),

    TAB("\t");

    public String separator;

    SeparatorEnum(String separator) {
        this.separator = separator;
    }

    public static SeparatorEnum getSeparatorEnum(String name) {

        if (name == null) {
            throw new RuntimeException("no separator for given name");
        }

        return valueOf(name.toUpperCase());

    }

    public static void main(String[] args) {
        SeparatorEnum local = SeparatorEnum.getSeparatorEnum("comma");
        String name = local.separator;
        System.out.println(name);

    }

}
