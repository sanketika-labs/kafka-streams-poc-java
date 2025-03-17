package com.sanketika.common.models;

import java.util.Map;

public class CanonicalEvent {
    private String table_name;
    private Map<String, Object> cdc_meta;
    private Map<String, Object> payload;
    private PipelineMeta pipeline_meta;

    public CanonicalEvent() {
    }

    public CanonicalEvent(String table_name, Map<String, Object> cdc_meta, Map<String, Object> payload) {
        this.table_name = table_name;
        this.cdc_meta = cdc_meta;
        this.payload = payload;
        this.pipeline_meta = null;
    }

    public CanonicalEvent(String table_name, Map<String, Object> cdc_meta, Map<String, Object> payload, PipelineMeta pipeline_meta) {
        this.table_name = table_name;
        this.cdc_meta = cdc_meta;
        this.payload = payload;
        this.pipeline_meta = pipeline_meta;
    }

    public String getTableName() {
        return table_name;
    }

    public void setTableName(String table_name) {
        this.table_name = table_name;
    }

    public Map<String, Object> getCdcMeta() {
        return cdc_meta;
    }

    public void setCdcMeta(Map<String, Object> cdc_meta) {
        this.cdc_meta = cdc_meta;
    }

    public Map<String, Object> getPayload() {
        return payload;
    }

    public void setPayload(Map<String, Object> payload) {
        this.payload = payload;
    }

    public PipelineMeta getPipelineMeta() {
        return pipeline_meta;
    }

    public void setPipelineMeta(PipelineMeta pipeline_meta) {
        this.pipeline_meta = pipeline_meta;
    }
}
