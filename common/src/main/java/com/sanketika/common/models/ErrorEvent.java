package com.sanketika.common.models;

public class ErrorEvent {
    private long ets;
    private String jobname;
    private String event;
    private ErrorValue error;

    public ErrorEvent() {
        this.ets = System.currentTimeMillis();
        this.jobname = "cdc_validator";
    }

    public ErrorEvent(String event, ErrorValue error) {
        this();
        this.event = event;
        this.error = error;
    }

    public long getEts() {
        return ets;
    }

    public void setEts(long ets) {
        this.ets = ets;
    }

    public String getJobname() {
        return jobname;
    }

    public void setJobname(String jobname) {
        this.jobname = jobname;
    }

    public String getEvent() {
        return event;
    }

    public void setEvent(String event) {
        this.event = event;
    }

    public ErrorValue getError() {
        return error;
    }

    public void setError(ErrorValue error) {
        this.error = error;
    }
}
