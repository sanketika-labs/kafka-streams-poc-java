package com.sanketika.models;

public class PipelineMeta {
    private long arrival_tstamp;
    private int preprocessor_time_ms;

    public PipelineMeta() {
    }

    public PipelineMeta(long arrival_tstamp, int preprocessor_time_ms) {
        this.arrival_tstamp = arrival_tstamp;
        this.preprocessor_time_ms = preprocessor_time_ms;
    }

    public long getArrival_tstamp() {
        return arrival_tstamp;
    }

    public void setArrival_tstamp(long arrival_tstamp) {
        this.arrival_tstamp = arrival_tstamp;
    }

    public int getPreprocessor_time_ms() {
        return preprocessor_time_ms;
    }

    public void setPreprocessor_time_ms(int preprocessor_time_ms) {
        this.preprocessor_time_ms = preprocessor_time_ms;
    }
}
