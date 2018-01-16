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
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.googlecode.fascinator.api.storage.JsonDigitalObject;
import com.googlecode.fascinator.api.storage.PayloadType;
import com.googlecode.fascinator.api.storage.StorageException;
import com.googlecode.fascinator.common.storage.impl.GenericPayload;
import com.mongodb.client.gridfs.GridFSBucket;
import com.mongodb.client.gridfs.GridFSBuckets;
import com.mongodb.client.gridfs.GridFSDownloadStream;
import com.mongodb.client.gridfs.model.GridFSFile;
import com.mongodb.client.gridfs.model.GridFSUploadOptions;

/**
 * MongoPayload
 *
 * @author <a target='_' href='https://github.com/shilob'>Shilo Banihit</a>
 *
 */
public class MongoPayload extends GenericPayload {

    private static Logger log = LoggerFactory.getLogger(MongoPayload.class);

    protected MongoDigitalObject obj;
    protected String payloadPath;
    protected ObjectId mongoFileId;
    GridFSDownloadStream ds;
    protected Date lastModified;
    protected String pid;
    protected JsonDigitalObject.PayloadBackend backend;

    public MongoPayload(MongoDigitalObject obj, String pid, String fileId,
            JsonDigitalObject.PayloadBackend backend) {
        super(pid);
        this.obj = obj;
        this.pid = pid;
        this.backend = backend;
        payloadPath = obj.oid + "/" + pid;
        setLabel(pid);
        // call this because of the flag that's set with the setLabel call
        setMetaChanged(false);
        if (fileId != null) {
            if (backend.equals(JsonDigitalObject.PayloadBackend.GRIDFS)) {
                mongoFileId = new ObjectId(fileId);
            }
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
        doc.append("backend", backend.toString());
        doc.append("fileId", getFileIdAsString());
        doc.append("lastModified", lastModified);
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
    }

    private Date getLastModified() {
        return lastModified;
    }

    /**
     * Persists the data from 'source' into GridFS using the 'payloadPath'
     * property as filename.
     *
     * @param source
     */
    public void create(InputStream source) {
        lastModified = new Date();

        GridFSUploadOptions options = new GridFSUploadOptions()
                .metadata(getMetadataDocLocal());
        mongoFileId = getBucket().uploadFromStream(payloadPath, source,
                options);
    }

    private GridFSBucket getBucket() {
        return GridFSBuckets.create(obj.getMongoDb());
    }

    @Override
    public InputStream open() throws StorageException {
        ds = getBucket().openDownloadStream(mongoFileId);

        GridFSFile file = ds.getGridFSFile();
        mongoFileId = file.getObjectId();
        // setMetadataDoc(file.getMetadata());
        return ds;
    }

    @Override
    public void close() throws StorageException {
        if (hasMetaChanged()) {
            lastModified = new Date();
            obj.updatePayloadMeta(this);
        }
    }

    @Override
    public Long size() {
        if (ds == null) {
            ds = getBucket().openDownloadStream(mongoFileId);
        }
        return ds.getGridFSFile().getLength();
    }

    @Override
    public Long lastModified() {
        return getLastModified().getTime();
    }

    public String getFileIdAsString() {
        return mongoFileId.toString();
    }

    public void remove() {
        getBucket().delete(mongoFileId);
        ds = null;
        mongoFileId = null;
        lastModified = null;
    }

    public void update(InputStream source) {
        remove();
        create(source);
    }

}
