package de.tuberlin.dima.minidb.io.manager;

import de.tuberlin.dima.minidb.io.cache.CacheableData;
import de.tuberlin.dima.minidb.io.cache.PageSize;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Created by Half-Blood on 11/23/2016.
 */
public class Writethread extends Thread{
    private final freebufferCallback callback;
    private boolean alive;

    interface freebufferCallback{
        void freebuffer(PageSize pageSize);
    }
    public ConcurrentLinkedQueue<WriteRequest> requests;
    public Writethread(freebufferCallback callback){
        this.callback = callback;
        this.requests = new ConcurrentLinkedQueue<WriteRequest>();
        this.alive = true;
    }
    public void run(){
        while(this.alive){
            while(!requests.isEmpty()){
                WriteRequest request = requests.remove();
                CacheableData wrapper;
                byte[] buffer;
                ResourceManager resource;
                synchronized (request){
                    wrapper = request.getWrapper();
                    buffer = request.getBuffer();
                    resource = request.getManager();
                }
                try{
                    synchronized (resource){
                        resource.writePageToResource(buffer, wrapper);

                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
                finally{
                    callback.freebuffer(resource.getPageSize());
                }
            }
        }
    }
    public synchronized void request (WriteRequest request){
        requests.add(request);
    }
    public synchronized CacheableData getRequest(int resourceId, int pageNumber){
        Iterator<WriteRequest> it = requests.iterator();
        while(it.hasNext()){
            WriteRequest request = it.next();
            synchronized (request){
                if(request.getresourceId()==resourceId && request.getWrapper().getPageNumber() == pageNumber)
                    return request.getWrapper();
            }
        }
        return null;
    }
    public void stopthread(){
        this.alive = false;
    }
}
