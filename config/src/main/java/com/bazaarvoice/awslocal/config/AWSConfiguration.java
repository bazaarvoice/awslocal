package com.bazaarvoice.awslocal.config;


import com.fasterxml.jackson.annotation.JsonProperty;
import org.hibernate.validator.constraints.NotBlank;
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.constraints.Min;

public class AWSConfiguration {

    public enum ServiceType {
        FILE_BASED,
        AMAZON;
    }

    @NotBlank
    @JsonProperty
    private String queueName;

    @NotBlank
    @JsonProperty
    private String topicArn;

    @JsonProperty
    private String baseDir;

    @JsonProperty
    private int visibilityTimeout;

    @NotEmpty
    @JsonProperty
    private ServiceType serviceType;

    public String getTopicArn() {
        return topicArn;
    }

    public int getVisibilityTimeout() {
        return visibilityTimeout;
    }

    public String getQueueName() {
        return queueName;
    }

    public String getBaseDir() {
        return baseDir;
    }

    public ServiceType getServiceType() {
        return serviceType;
    }
}