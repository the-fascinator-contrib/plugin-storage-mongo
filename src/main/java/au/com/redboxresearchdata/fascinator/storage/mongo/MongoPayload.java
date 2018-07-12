/*******************************************************************************
 * Copyright (C) 2017 Queensland Cyber Infrastructure Foundation (http://www.qcif.edu.au/)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 ******************************************************************************/
package au.com.redboxresearchdata.fascinator.storage.mongo;

import java.io.InputStream;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.googlecode.fascinator.api.storage.PayloadType;
import com.googlecode.fascinator.api.storage.StorageException;
import com.googlecode.fascinator.common.storage.impl.GenericPayload;

/**
 * MongoPayload
 *
 * @author <a target='_' href='https://github.com/shilob'>Shilo Banihit</a>
 *
 */
public class MongoPayload extends GenericPayload {

    private static Logger log = LoggerFactory.getLogger(MongoPayload.class);

    protected MongoDigitalObject obj;
    protected Date lastModified;
    protected String pid;
    protected MongoDigitalObject.PayloadBackend backendType;
    protected MongoPayloadBackend backend;
    protected String fileId;
    protected boolean hasChangedBackendType;

    public MongoPayload(MongoDigitalObject obj, String pid, String fileId,
            MongoDigitalObject.PayloadBackend backend) {
        super(pid);
        this.obj = obj;
        this.pid = pid;
        backendType = backend;
        setLabel(pid);
        // call this because of the flag that's set with the setLabel call
        setMetaChanged(false);
        this.fileId = fileId;
        if (fileId != null) {
            List<Map<String, Object>> files = obj.getFileList();
            if (files != null) {
                for (Map<String, Object> fileInfo : files) {
                    if (pid.equals(fileInfo.get("pid"))) {
                        setMetadataDoc(new Document(fileInfo));
                    }
                }
            }
        }
    }

    public Map<String, Object> getMetadataDoc() {
        Document doc = new Document();
        doc.append("pid", getId());
        doc.append("linked", isLinked());
        doc.append("label", getLabel());
        doc.append("payloadType", getType().toString());
        doc.append("oid", obj.oid);
        doc.append("contentType", getContentType());
        doc.append("backend", backendType.toString());
        doc.append("backend_type", getBackend().getType());
        doc.append("payloadId", getPayloadId());
        doc.append("lastModified", lastModified);
        
        Document backendMeta = getBackend().getMetadata();
        if (backendMeta != null) {
            doc.append(getBackend().getId(), backendMeta);
        }
        System.out.println(doc.toJson());
        return doc;
    }

    private Document getMetadataDocLocal() {
        Document doc = new Document();
        doc.append("pid", getId());
        doc.append("oid", obj.oid);
        doc.append("contentType", getContentType());
        return doc;
    }

    public void setMetadataDoc(Document doc) {
        setId(doc.getString("pid"));
        setLinked(doc.getBoolean("linked", false));
        setLabel(doc.getString("label"));
        setType(PayloadType.valueOf(doc.getString("payloadType")));
        setContentType(doc.getString("contentType"));
        lastModified = doc.getDate("lastModified");
        getBackend().setId(doc.getString("payloadId"));
    }

    private Date getLastModified() {
        return lastModified;
    }

    private MongoPayloadBackend getBackend() {
        if (backend == null || hasChangedBackendType) {
            PayloadType type = getType();
            switch (type) {
            case Source:
                backend = new MongoPayloadBackendCollection(obj, pid);
                break;
            default:
                backend = new MongoPayloadBackendGridFs(obj.oid + "/" + pid,
                        fileId, obj.getMongoDb());
            }
            hasChangedBackendType = false;
        }
        return backend;
    }

    public void create(InputStream source) throws StorageException {
        lastModified = new Date();
        getBackend().create(source, getMetadataDocLocal());
        fileId = getBackend().getId();
    }

    @Override
    public InputStream open() throws StorageException {
        return getBackend().open();
    }

    @Override
    public void close() throws StorageException {
        save();
    }

    public void save() throws StorageException {
        if (hasMetaChanged()) {
            lastModified = new Date();
            obj.updatePayloadMeta(this);
        } else {
            System.out.println("Payload closed, not meta changed.");
        }
    }

    @Override
    public Long size() {
        return getBackend().size();
    }

    @Override
    public Long lastModified() {
        return getLastModified().getTime();
    }

    public String getPayloadId() {
        return getBackend().getId();
    }

    public void remove() {
        getBackend().remove();
        lastModified = null;
    }

    public void update(InputStream source) throws StorageException {
        remove();
        create(source);
    }

    @Override
    public void setType(PayloadType type) {
        if (type != getType()) {
            hasChangedBackendType = true;
        }
        super.setType(type);
    }

}
