package de.tuberlin.dima.minidb.io.manager;

import de.tuberlin.dima.minidb.io.cache.CacheableData;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Created by Half-Blood on 11/23/2016.
 */
public class Readthread extends Thread {
    interface PrefetchCallback {
        void addPageInCache (int resourceId, CacheableData page, boolean pin);
    }
    public ConcurrentLinkedQueue<ReadRequest> requests;
    private PrefetchCallback callback;
    private volatile boolean alive;

    public Readthread(PrefetchCallback callback){
        this.requests = new ConcurrentLinkedQueue<ReadRequest>();
        this.callback = callback;
        this.alive = true;
    }
    public void run(){
        while(this.alive){
            if (!requests.isEmpty()){
                ReadRequest request = requests.peek();
                synchronized (request){
                    ResourceManager resource = request.getManager();
                    int pageNumber = request.getPageNumber();
                    byte[] buffer = request.getBuffer();
                    int resourceId = request.getresourceId();
                    try{
                        CacheableData page;
                        synchronized (resource){
                            page = resource.readPageFromResource(buffer, pageNumber);
                        }
                        if(request.isPrefetch())
                            callback.addPageInCache(resourceId, page, false);
                        request.setwrapper(page);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    finally {
                        request.done();
                        requests.remove();
                        request.notifyAll();
                    }
                }
            }
        }
    }
    public synchronized void request(ReadRequest request){
        requests.add(request);
    }
    public synchronized ReadRequest getRequest(int resourceId, int pageNumber){
        Iterator<ReadRequest> it = requests.iterator();
        while(it.hasNext()){
            ReadRequest request = it.next();
            if(request.getresourceId() == resourceId && request.getPageNumber()== pageNumber)
                return request;
        }
        return null;
    }
    public boolean isRunning(){
        return this.alive;
    }
    public void stopthread(){
        this.alive = false;
        while(!requests.isEmpty()){
            ReadRequest request = requests.remove();
            synchronized (request){
                request.notifyAll();
            }

        }
    }
}
