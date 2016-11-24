package de.tuberlin.dima.minidb.io.manager;

import com.sun.corba.se.pept.transport.ReaderThread;
import de.tuberlin.dima.minidb.Config;
import de.tuberlin.dima.minidb.api.AbstractExtensionFactory;
import de.tuberlin.dima.minidb.io.cache.*;

import javax.swing.*;
import java.io.IOException;
import java.util.*;
import java.util.logging.Logger;

/**
 * Created by Half-Blood on 11/13/2016.
 */
public class BufferPoolManagerImpl implements BufferPoolManager, Readthread.PrefetchCallback, Writethread.freebufferCallback {
    private Config config;
    private Readthread readthread;
    private Writethread writethread;
    private boolean allowed;
    private Hashtable<Integer, ResourceManager> resources;
    private Hashtable<PageSize, PageCache> cache;
    private int numIOBuffers;
    private Hashtable<PageSize, LinkedList<byte[]>> buffer;

    public BufferPoolManagerImpl(Config config, Logger logger) {
        this.config = config;
        this.allowed = false;
        resources = new Hashtable<Integer, ResourceManager>();
        cache = new Hashtable<PageSize, PageCache>();
        buffer = new Hashtable<PageSize, LinkedList<byte[]>>();
        numIOBuffers = config.getNumIOBuffers();
        readthread = new Readthread(this);
        writethread = new Writethread(this);
    }

    @Override
    public void startIOThreads() throws BufferPoolException {
        readthread.start();
        writethread.start();
        this.allowed = true;
    }

    @Override
    public void closeBufferPool() {
        this.allowed = false;
        readthread.stopthread();
        for (Map.Entry<Integer, ResourceManager> entryid : resources.entrySet() ){
            try {
                ResourceManager resource = entryid.getValue();
                int resourceId = entryid.getKey();
                PageCache pagecache = cache.get(resource.getPageSize());
                CacheableData[] allpages = pagecache.getAllPagesForResource(resourceId);
                for (CacheableData page : allpages){
                    if(page != null && page.hasBeenModified()){
                        byte[] writebuffer = getBuffer(resource.getPageSize());
                        System.arraycopy(page.getBuffer(), 0 , writebuffer, 0, writebuffer.length);
                        WriteRequest writerequest = new WriteRequest(resourceId, resource, writebuffer, page);
                        writethread.request(writerequest);
                    }
                }
            }
            catch (BufferPoolException e){;}
        }
        writethread.stopthread();
        try{
            writethread.join();
        }
        catch (InterruptedException e){;}
    }

    @Override
    public void registerResource(int id, ResourceManager manager) throws BufferPoolException {
        if (!this.allowed)
            throw new BufferPoolException("Closed Buffer Pool Manager");
        if(resources.containsKey(id))
            throw new BufferPoolException("Already registered resource");
        resources.put(id, manager);
        PageSize pageSize = manager.getPageSize();
        if(!cache.containsKey(pageSize)){
            PageCache pagecache = AbstractExtensionFactory.getExtensionFactory().createPageCache(pageSize, config.getCacheSize(pageSize));
            cache.put(pageSize, pagecache);
            LinkedList<byte[]> bufferlist = new LinkedList<byte[]>();
            for(int i = 0; i < numIOBuffers; i++){
                bufferlist.add(new byte[pageSize.getNumberOfBytes()]);
            }
            buffer.put(pageSize, bufferlist);
        }
    }

    @Override
    public CacheableData getPageAndPin(int resourceId, int pageNumber) throws BufferPoolException, IOException {
        if(!this.allowed){
            throw new BufferPoolException("Closed Buffer Pool Manager");
        }
        ResourceManager resource = resources.get(resourceId);
        if (resource == null)
            throw new BufferPoolException("Resource not registered");
        PageCache pagecache = cache.get(resource.getPageSize());
        ReadRequest request;
        synchronized (pagecache){
            CacheableData page = pagecache.getPageAndPin(resourceId, pageNumber);
            if(page != null){
                return page;
            }
            request = readthread.getRequest(resourceId, pageNumber);
        }
        CacheableData page = writethread.getRequest(resourceId, pageNumber);
        if (page != null) {
            addPageInCache(resourceId, page, true);
            return page;
        }
        if (request != null) {
             try {
                synchronized (request) {

                    while (!request.isDone()) {
                        request.wait();
                        if(!readthread.isRunning())
                            throw new BufferPoolException("Closing the Buffer Pool");
                    }
                }
            } catch (InterruptedException ie) {
                throw new IOException("Request interrupted");
            }
            synchronized (pagecache) {
                page = pagecache.getPageAndPin(resourceId, pageNumber);
                if (page != null)
                    return page;
            }
            return request.getwrapper();
        }
        byte[] readBuffer;
        readBuffer = getBuffer(resource.getPageSize());
        request = new ReadRequest(resource, readBuffer, pageNumber, resourceId, false);
        readthread.request(request);
        CacheableData wrapper;
        try {
            synchronized (request) {

                while (!request.isDone()) {
                    request.wait();
                    if(!readthread.isRunning())
                        throw new BufferPoolException("Closing the Buffer Pool");
                }

                wrapper = request.getwrapper();
            }
        } catch (InterruptedException ie) {
            throw new IOException("Request interrupted");
        }
        addPageInCache(resourceId, wrapper, true);
        return wrapper;
    }

    @Override
    public CacheableData unpinAndGetPageAndPin(int resourceId, int unpinPageNumber, int getPageNumber) throws BufferPoolException, IOException {
        if (!this.allowed)
            throw new BufferPoolException("Closed Buffer Pool Manager");
        ResourceManager resource = resources.get(resourceId);
        if (resource == null)
            throw new BufferPoolException("Resource not registered");
        PageCache pagecache = cache.get(resource.getPageSize());
        ReadRequest request;
        synchronized (pagecache) {
            pagecache.unpinPage(resourceId, unpinPageNumber);
            CacheableData page = pagecache.getPageAndPin(resourceId, getPageNumber);
            if (page != null)
                return page;
            request = readthread.getRequest(resourceId, getPageNumber);
        }
        byte[] buffer;
        buffer = getBuffer(resource.getPageSize());
        if (request != null) {
            synchronized (request) {
                while (!request.isDone()) {
                    try {
                        request.wait();
                    } catch (InterruptedException e) {
                        throw new IOException("Request interrupted");
                    }
                    if(!readthread.isRunning())
                        throw new BufferPoolException("Closing the Buffer Pool");
                }
            }
            return request.getwrapper();
        }
        request = new ReadRequest(resource, buffer, getPageNumber, resourceId, false);
        readthread.request(request);
        CacheableData wrapper;
        try {
            synchronized (request) {
                while (!request.isDone()) {
                    request.wait();
                    if(!readthread.isRunning())
                        throw new BufferPoolException("Closing the Buffer Pool");
                }
                wrapper = request.getwrapper();
            }
        } catch (InterruptedException ie) {
            throw new IOException("Request interrupted");
        }
        if (wrapper.getPageNumber() != getPageNumber)
            throw new BufferPoolException("The one requested is different from the read page number");
        addPageInCache(resourceId, wrapper, true);
        return wrapper;
    }

    @Override
    public void unpinPage(int resourceId, int pageNumber) {
        ResourceManager resource = resources.get(resourceId);
        if (resource != null)  {
            PageCache pagecache = cache.get(resource.getPageSize());
            synchronized (pagecache) {
                pagecache.unpinPage(resourceId, pageNumber);
            }
        }
    }

    @Override
    public void prefetchPage(int resourceId, int pageNumber) throws BufferPoolException {
        if (!this.allowed)
            throw new BufferPoolException("Closed Buffer Pool Manager");
        ResourceManager resource = resources.get(resourceId);
        if (resource == null)
            throw new BufferPoolException();
        PageCache pagecache = cache.get(resource.getPageSize());
        CacheableData page;
        synchronized(pagecache) {
            page = pagecache.getPage(resourceId, pageNumber);
        }
        if (page != null) {
            return;
        }
        byte[] readBuffer;
        ReadRequest request;
        readBuffer = getBuffer(resource.getPageSize());
        request = new ReadRequest(resource, readBuffer, pageNumber, resourceId, true);
        readthread.request(request);
    }

    @Override
    public void prefetchPages(int resourceId, int startPageNumber, int endPageNumber) throws BufferPoolException {
        if (!this.allowed)
            throw new BufferPoolException("Closed Buffer Pool Manager");
        ResourceManager resource = resources.get(resourceId);
        ArrayList<Integer> dump = new ArrayList<Integer>();
        if (resource == null)
            throw new BufferPoolException();
        PageCache pagecache = cache.get(resource.getPageSize());
        CacheableData page;
        synchronized(pagecache) {
            for (int i = startPageNumber; i <= endPageNumber; i++ ) {
                page = pagecache.getPage(resourceId, i);
                if (page == null)
                    dump.add(i);
            }
        }
        byte[] readBuffer;
        ReadRequest request;
        for (Integer pageNumber : dump) {
            readBuffer = getBuffer(resource.getPageSize());
            request = new ReadRequest(resource, readBuffer, pageNumber, resourceId, true);
            readthread.request(request);
        }
    }

    @Override
    public CacheableData createNewPageAndPin(int resourceId) throws BufferPoolException, IOException {
        if (!this.allowed)
            throw new BufferPoolException("Closed Buffer Pool Manager");
        ResourceManager resource = resources.get(resourceId);
        if (resource == null)
            throw new BufferPoolException();
        byte[] buffer;
        try {
            buffer = getBuffer(resource.getPageSize());
            CacheableData page;
            synchronized(resource) {
                page = resource.reserveNewPage(buffer);
            }
            addPageInCache(resourceId, page, true);
            return page;
        }
        catch (NoSuchElementException ns) {
            throw new BufferPoolException("No more buffers available");
        } catch (PageFormatException pf) {
            throw new BufferPoolException("Unable to reserve page");
        }
    }

    @Override
    public CacheableData createNewPageAndPin(int resourceId, Enum<?> type) throws BufferPoolException, IOException {
        if (!this.allowed)
            throw new BufferPoolException("Closed Buffer Pool Manager");
        ResourceManager resource = resources.get(resourceId);
        if (resource == null)
            throw new BufferPoolException("Resource not registered");
        byte[] buffer;
        try {
            buffer = getBuffer(resource.getPageSize());
            CacheableData page;
            synchronized(resource) {
                page = resource.reserveNewPage(buffer, type);
            }
            addPageInCache(resourceId, page, true);
            return page;
        } catch (NoSuchElementException ns) {
            throw new BufferPoolException("Non-availability of buffer");
        } catch (PageFormatException pf) {
            throw new BufferPoolException("Unable to reserve page");
        }
    }

    @Override
    public void addPageInCache(int resourceId, CacheableData page, boolean pin) {
        ResourceManager resource = resources.get(resourceId);
        PageSize pageSize = resource.getPageSize();
        PageCache pagecache = cache.get(pageSize);
        EvictedCacheEntry evicted;
        try {
            synchronized(pagecache) {
                if (pin) {
                    evicted = pagecache.addPageAndPin(page, resourceId);
                } else {
                    evicted = pagecache.addPage(page, resourceId);
                }
            }
            freebuffer(pageSize);
            CacheableData evictedPage = evicted.getWrappingPage();
            int evictedResourceId = evicted.getResourceID();
            ResourceManager evictedResource = resources.get(evictedResourceId);
            if (evictedPage != null && evictedPage.hasBeenModified()) {
                byte[] writeBuffer = getBuffer(pageSize);
                System.arraycopy(evictedPage.getBuffer(), 0, writeBuffer, 0, writeBuffer.length);
                WriteRequest writeRequest = new WriteRequest(evictedResourceId, evictedResource, writeBuffer, evictedPage);
                writethread.request(writeRequest);
            }
        }
        catch (DuplicateCacheEntryException dc) {
            dc.printStackTrace();
            System.out.println("Page : " + dc.getPageNumber());
        }
        catch (CachePinnedException cp) {
            cp.printStackTrace();
        }
        catch (NoSuchElementException ns) {
            ns.printStackTrace();
        }
        catch (BufferPoolException ie) {
            ie.printStackTrace();
        }
    }

    private byte[] getBuffer(PageSize pageSize) throws BufferPoolException {
        LinkedList<byte[]> bufferQueue = buffer.get(pageSize);
        byte[] buffer;
        try {
            synchronized (bufferQueue) {
                while (bufferQueue.isEmpty()) {
                    bufferQueue.wait();
                }
                buffer = bufferQueue.pop();
                bufferQueue.notifyAll();
            }
        } catch (InterruptedException ie) {
            throw new BufferPoolException();
        }
        return buffer;
    }

    @Override
    public void freebuffer(PageSize pageSize) {
        LinkedList<byte[]> bufferQueue = buffer.get(pageSize);
        synchronized (bufferQueue) {
            bufferQueue.add(new byte[pageSize.getNumberOfBytes()]);
            bufferQueue.notifyAll();
        }
    }
}
