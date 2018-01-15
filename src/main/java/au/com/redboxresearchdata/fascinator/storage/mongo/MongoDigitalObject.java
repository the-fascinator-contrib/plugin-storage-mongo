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

import static com.mongodb.client.model.Filters.eq;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.googlecode.fascinator.api.storage.JsonDigitalObject;
import com.googlecode.fascinator.api.storage.Payload;
import com.googlecode.fascinator.api.storage.PayloadType;
import com.googlecode.fascinator.api.storage.StorageException;
import com.googlecode.fascinator.common.storage.impl.GenericDigitalObject;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

/**
 * MongoDigitalObject
 *
 * @author <a target='_' href='https://github.com/shilob'>Shilo Banihit</a>
 *
 */
public class MongoDigitalObject extends GenericDigitalObject
        implements JsonDigitalObject {

    private static String METADATA_PAYLOAD = "TF-OBJ-META";

    private static Logger log = LoggerFactory
            .getLogger(MongoDigitalObject.class);

    protected MongoDatabase mongoDb;
    protected String collectionName;
    protected String objectMetadataCollectionName;
    protected String oid;
    protected String sourceId;
    protected Document objectMetadata;
    protected Document recordMetadata;
    protected Properties metadataProp;
    protected JsonDigitalObject.PayloadBackend payloadBackend;

    public MongoDigitalObject(MongoDatabase mongoDb, String collectionName,
            String objectMetadataCollectionName, String oid,
            JsonDigitalObject.PayloadBackend payloadBackend) {

        super(oid);
        this.mongoDb = mongoDb;
        this.collectionName = collectionName;
        this.oid = oid;
        this.objectMetadataCollectionName = objectMetadataCollectionName;
        this.payloadBackend = payloadBackend;
    }

    public MongoDatabase getMongoDb() {
        return mongoDb;
    }

    @Override
    public String getId() {
        return oid;
    }

    @Override
    public void setId(String oid) {
        this.oid = oid;
    }

    @Override
    public String getSourceId() {
        return sourceId;
    }

    @Override
    public void setSourceId(String pid) {
        sourceId = pid;
    }

    /**
     * Returns metadata of this object, caches subsequent calls. THIS IS THE
     * TF-OBJ-META in a object.
     */
    @Override
    public Properties getMetadata() throws StorageException {
        getObjectMetadata();
        if (metadataProp == null) {
            metadataProp = new Properties();
            metadataProp.putAll(objectMetadata);
        }
        return metadataProp;
    }

    private Document getObjectMetadataFromDb() {
        MongoCollection<Document> metaCol = getObjectMetadataCollection();
        return metaCol.find(eq("redboxOid", oid)).first();
    }

    private MongoCollection<Document> getObjectMetadataCollection() {
        return mongoDb.getCollection(objectMetadataCollectionName);
    }

    /**
     * Get the manifest of the DigitalObject
     *
     * @return Manifest Map
     */
    @SuppressWarnings("unchecked")
    @Override
    public Map<String, Payload> getManifest() {
        if (objectMetadata == null) {
            try {
                getMetadata();
            } catch (StorageException e) {
                throw new RuntimeException("Failed to get metadata:", e);
            }
        }
        // populate from the metadata
        Map<String, Payload> manifest = super.getManifest();
        if (manifest.isEmpty()) {
            List<Map<String, String>> payloads = getFileList();
            if (payloads != null && payloads.size() > 0) {
                Iterator<Map<String, String>> iter = payloads.iterator();
                while (iter.hasNext()) {
                    Map<String, String> payloadMeta = iter.next();
                    String pid = payloadMeta.get("pid");
                    manifest.put(pid, new MongoPayload(this, pid,
                            payloadMeta.get("fileId"), payloadBackend));
                }
            }
            // append the 'pseudo' payload 'metadata.tfpackage'

        }
        return manifest;
    }

    @Override
    public Set<String> getPayloadIdList() {
        return getManifest().keySet();
    }

    private Payload createPayload(String pid, InputStream source,
            boolean linked, PayloadType payloadType) throws StorageException {

        Map<String, Payload> manifest = getManifest();
        if (manifest.containsKey(pid)) {
            throw new StorageException(
                    "ID '" + pid + "' already exists in manifest.");
        }
        // Create payload - GRID FS for now...
        MongoPayload payload = new MongoPayload(this, pid, null,
                payloadBackend);
        payload.setLinked(linked);
        payload.setType(payloadType);
        payload.create(source);
        // add to manifest
        manifest.put(pid, payload);
        addFileMeta(pid, payload.getFileIdAsString());
        save();
        return payload;
    }

    @Override
    public synchronized Payload createStoredPayload(String pid, InputStream in)
            throws StorageException {
        if (pid == null || in == null) {
            throw new StorageException("Error; Null parameter recieved");
        }
        PayloadType type = null;
        if (METADATA_PAYLOAD.equals(pid)) {
            type = PayloadType.Annotation;
        } else if (getSourceId() == null) {
            type = PayloadType.Source;
            setSourceId(pid);
        } else {
            type = PayloadType.Annotation;
        }
        Payload payload = createPayload(pid, in, false, type);
        return payload;
    }

    @Override
    public Payload createLinkedPayload(String pid, String linkPath)
            throws StorageException {
        log.warn("This storage plugin does not support linked payloads..."
                + " converting to stored.");
        try {
            FileInputStream in = new FileInputStream(linkPath);
            return createStoredPayload(pid, in);
        } catch (FileNotFoundException fnfe) {
            throw new StorageException(fnfe);
        }
    }

    @Override
    public Payload getPayload(String pid) throws StorageException {
        if (pid == null) {
            throw new StorageException("Error; Null PID recieved");
        }

        // Confirm we actually have this payload first
        Map<String, Payload> manifest = getManifest();
        if (!manifest.containsKey(pid)) {
            throw new StorageException("pID '" + pid + "': was not found");

        }
        return manifest.get(pid);
    }

    @Override
    public void removePayload(String pid) throws StorageException {
        MongoPayload payload = (MongoPayload) getPayload(pid);
        payload.remove();
        removeFileMeta(pid);
        getManifest().remove(pid);
        save();
    }

    /* (non-Javadoc)
     * @see com.googlecode.fascinator.api.storage.DigitalObject#updatePayload(java.lang.String, java.io.InputStream)
     */
    @Override
    public Payload updatePayload(String pid, InputStream in)
            throws StorageException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void close() throws StorageException {
        save();
    }

    @Override
    public Map<String, Object> getObjectMetadata() {
        if (objectMetadata == null) {
            objectMetadata = getObjectMetadataFromDb();
            if (objectMetadata == null) {
                objectMetadata = new Document();
                objectMetadata.put("redboxOid", oid);
                objectMetadata.put("collectionName", collectionName);
            }
        }
        return objectMetadata;
    }

    @Override
    public Map<String, Object> getRecordMetadata() {
        if (recordMetadata == null) {
            recordMetadata = getRecordMetadataFromDb();
            if (recordMetadata == null) {
                recordMetadata = new Document();
                recordMetadata.put("redboxOid", oid);
            }
        }
        return recordMetadata;
    }

    protected Document getRecordMetadataFromDb() {
        return getMetaCollection().find(eq("redboxOid", oid)).first();
    }

    @Override
    public boolean existsInStorage() {
        return getObjectMetadataFromDb() != null;
    }

    protected MongoCollection<Document> getMetaCollection() {
        return mongoDb.getCollection(collectionName);
    }

    public void save() throws StorageException {
        MongoCollection<Document> objectMetaCol = getObjectMetadataCollection();
        MongoCollection<Document> metaCol = getMetaCollection();
        boolean isInStorage = existsInStorage();
        if (!isInStorage) {
            getObjectMetadata();
            objectMetaCol.insertOne(objectMetadata);
            getRecordMetadata();
            metaCol.insertOne(recordMetadata);
        } else {
            // full replacement of object metadata...
            objectMetaCol.findOneAndReplace(eq("redboxOid", oid),
                    objectMetadata);
            // full replacement of record metadata...
            metaCol.findOneAndReplace(eq("redboxOid", oid), recordMetadata);
        }
    }

    public void load() throws StorageException {
        boolean isInStorage = existsInStorage();
        if (!isInStorage) {
            throw new StorageException(
                    "Object with OID: " + oid + ", doesn't exist!");
        } else {
            getObjectMetadata();
            getRecordMetadata();
        }
    }

    public void remove() throws StorageException {
        MongoCollection<Document> objectMetaCol = getObjectMetadataCollection();
        MongoCollection<Document> metaCol = getMetaCollection();

        boolean isInStorage = existsInStorage();
        if (!isInStorage) {
            throw new StorageException(
                    "Object with OID: " + oid + ", doesn't exist!");
        } else {
            objectMetaCol.findOneAndDelete(eq("redboxOid", oid));
            metaCol.findOneAndDelete(eq("redboxOid", oid));
        }
    }

    @SuppressWarnings("unchecked")
    public List<Map<String, String>> getFileList() {
        return (List<Map<String, String>>) recordMetadata.get("files");
    }

    protected void addFileMeta(String pid, String fileId) {
        List<Map<String, String>> files = getFileList();
        if (files == null) {
            files = new ArrayList<Map<String, String>>();
            recordMetadata.put("files", files);
        }
        HashMap<String, String> info = new HashMap<String, String>();
        info.put("pid", pid);
        info.put("backend", payloadBackend.toString());
        info.put("fileId", fileId);
        files.add(info);
    }

    protected void removeFileMeta(String pid) {
        List<Map<String, String>> files = getFileList();
        Map<String, String> metaToRemove = null;
        for (Map<String, String> info : files) {
            if (info.get("pid").equals(pid)) {
                metaToRemove = info;
            }
        }
        if (metaToRemove != null) {
            files.remove(metaToRemove);
        }
    }
}
