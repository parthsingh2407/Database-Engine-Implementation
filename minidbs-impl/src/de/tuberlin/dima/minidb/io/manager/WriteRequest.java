package de.tuberlin.dima.minidb.io.manager;

import de.tuberlin.dima.minidb.io.cache.CacheableData;

/**
 * Created by Half-Blood on 11/23/2016.
 */
public class WriteRequest {
    private final ResourceManager resource;
    private final int resourceId;
    private final byte[] buffer;
    private final CacheableData wrapper;

    public WriteRequest(int resourceId, ResourceManager resource, byte[] buffer, CacheableData wrapper){
        this.resource =  resource;
        this.resourceId = resourceId;
        this.buffer = buffer;
        this.wrapper = wrapper;
    }
    public ResourceManager getManager(){
        return this.resource;
    }
    public byte[] getBuffer(){
        return this.buffer;
    }
    public CacheableData getWrapper(){
        return this.wrapper;
    }
    public int getresourceId(){
        return this.resourceId;
    }

}
