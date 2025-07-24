package com.mcic.analytics.wavemetadata;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class SalesforceModel {
    private String userName, password, securityKey, key, secret, endpoint;

    public SalesforceModel(File propFile) {
        Properties props = new Properties();
        try {
            props.load(new FileReader(propFile));
        } catch (IOException e) {
            e.printStackTrace();
        }
        userName    = props.getProperty("username");
        password    = props.getProperty("password");
        securityKey = props.getProperty("securityKey");
        endpoint    = props.getProperty("endpoint");
        key         = props.getProperty("key");
        secret      = props.getProperty("secret");
    }

    public String getConsumerKey()    { return key; }
    public String getConsumerSecret() { return secret; }
    public String getUserName()       { return userName; }
    public String getPassword()       { return password; }
    public String getSecurityKey()    { return securityKey; }
    public String getEndpoint()       { return endpoint; }
}
