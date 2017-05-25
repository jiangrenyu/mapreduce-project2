package com.bonc.mr.comm;

/**
 * create by  johen(jing) on 2016-01-06:22:11
 * project_name bonc.hjpt.mr.roam
 * package_name PACKAGE_NAME
 * JDK 1.7
 */
public enum ProvName2Code {
    shanghai("831"),
    yunan("853"),
    nmg("815"),
    beijing("811"),
    jilin("822"),
    sichuan("851"),
    tianjin("812"),
    ningxia("864"),
    anhui("834"),
    shandong("837"),
    shanxi("814"),
    guangdong("844"),
    guangxi("845"),
    xinjiang("865"),
    jiangsu("832"),
    jiangxi("836"),
    hebei("813"),
    henan("841"),
    zhejiang("833"),
    hainan("846"),
    hubei("842"),
    hunan("843"),
    gansu("862"),
    fujian("835"),
    xizang("854"),
    guizhou("852"),
    liaoning("821"),
    chongqing("850"),
    shanxi1("861"),
    qinghai("863"),
    hlj("823");
    private String other;

    private ProvName2Code(String name) {
        other = name;
    }

    public static String getProvCode(String prov) {
        String result = null;
        try {
            result = ProvName2Code.valueOf(prov).getCode();
        } catch (Exception v) {
            for (ProvName2Code name2Code : ProvName2Code.values()) {
                if (prov.equals(name2Code.getCode())) {
                    result = name2Code.getCode();
                }
            }
        }
        if (result == null) {
            throw new RuntimeException("输入省份不对");
        }
        return result;
    }

    public static String getProvName(String name) {
        String result = null;
        try {
            result = ProvName2Code.valueOf(name).name();
        } catch (Exception va) {
            for (ProvName2Code name2Code : ProvName2Code.values()) {
                if (name.equals(name2Code.getCode())) {
                    result = name2Code.name();
                }
            }
        }
        return result;
    }

    public String getCode() {
        return other;
    }

    public static void main(String[] args) {

        System.out.println(ProvName2Code.getProvCode(args[0]));
        System.out.println(ProvName2Code.getProvName(args[0]));
    }
}
