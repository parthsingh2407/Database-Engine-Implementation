package de.tuberlin.dima.minidb.io.manager;

import de.tuberlin.dima.minidb.io.cache.CacheableData;

/**
 * Created by Half-Blood on 11/23/2016.
 */
public class ReadRequest {
    private final ResourceManager manager;
    private final byte[] buffer;
    private final int pageNumber;
    private final int resourceId;
    private final boolean prefetch;
    private boolean isdone;
    private CacheableData wrapper;

    public ReadRequest(ResourceManager manager, byte[] buffer, int pageNumber, int resourceId, boolean prefetch ){
        this.manager = manager;
        this.buffer = buffer;
        this.pageNumber = pageNumber;
        this.resourceId = resourceId;
        this.prefetch = prefetch;
        this.isdone = false;

    }
    public ResourceManager getManager(){
        return this.manager;
    }
    public CacheableData getwrapper(){
        return this.wrapper;
    }
    public void setwrapper(CacheableData wrapper){
        this.wrapper = wrapper;
    }
    public byte[] getBuffer(){
        return this.buffer;
    }
    public int getPageNumber(){
        return this.pageNumber;
    }
    public int getresourceId(){
        return this.resourceId;
    }
    public boolean isPrefetch(){
        return this.prefetch;
    }
    public boolean isDone(){
        return this.isdone;
    }
    public void done(){
        this.isdone = true;
    }
}
