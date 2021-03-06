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
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.tika.Tika;
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

import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

/**
 * MongoDigitalObject
 *
 * @author <a target='_' href='https://github.com/shilob'>Shilo Banihit</a>
 *
 */
public class MongoDigitalObject extends GenericDigitalObject
        implements JsonDigitalObject {

    enum PayloadBackend {
        MONGO
    }

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
    protected MongoDigitalObject.PayloadBackend payloadBackend;
    protected Map<String, String> encodedChars;
    protected DateTimeFormatter df;

    public MongoDigitalObject(MongoDatabase mongoDb, String collectionName,
            String objectMetadataCollectionName, String oid,
            MongoDigitalObject.PayloadBackend payloadBackend) {

        super(oid);
        this.mongoDb = mongoDb;
        this.collectionName = collectionName;
        this.oid = oid;
        this.objectMetadataCollectionName = objectMetadataCollectionName;
        this.payloadBackend = payloadBackend;
        encodedChars = new HashMap<String, String>();
        encodedChars.put(".", "_dot_");
        encodedChars.put("$", "_dollar_");
        df = ISODateTimeFormat.dateTime();
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
        List<Map<String, Object>> files = getFileList();
        if (files != null) {
            for (Map<String, Object> fileInfo : files) {
                if (PayloadType.Source.toString()
                        .equals(fileInfo.get("payloadType"))) {
                    sourceId = (String) fileInfo.get("pid");
                }
            }
        }
        // mirror the legacy behavior of setting the first entry as the source
        // if none is specified...
        if (files != null && files.size() > 0) {
            sourceId = (String) files.get(0).get("pid");
        }
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
            List<Map<String, Object>> payloads = getFileList();
            if (payloads != null && payloads.size() > 0) {
                Iterator<Map<String, Object>> iter = payloads.iterator();
                while (iter.hasNext()) {
                    Map<String, Object> payloadMeta = iter.next();
                    String pid = (String) payloadMeta.get("pid");
                    manifest.put(pid,
                            new MongoPayload(this, pid,
                                    (String) payloadMeta.get("fileId"),
                                    payloadBackend));
                }
            }
        }
        return manifest;
    }

    private Map<String, Object> getFileInfo(String pid) {
        Map<String, Object> fileInfo = null;
        List<Map<String, Object>> payloads = getFileList();
        if (payloads != null && payloads.size() > 0) {
            Iterator<Map<String, Object>> iter = payloads.iterator();
            while (iter.hasNext()) {
                fileInfo = iter.next();
                String pidMeta = (String) fileInfo.get("pid");
                if (pidMeta.equals(pid)) {
                    return fileInfo;
                }
            }
        }
        return fileInfo;
    }

    @Override
    public Set<String> getPayloadIdList() {
        return getManifest().keySet();
    }

    private synchronized Payload createPayload(String pid, InputStream source,
            boolean linked, PayloadType payloadType)
            throws IOException, StorageException {

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
        Tika tika = new Tika();
        String mimeType = tika.detect(pid);
        payload.setContentType(mimeType);
        payload.create(source);
        // add to manifest
        manifest.put(pid, payload);
        addFileMeta(payload);
        save();
        payload.setMetaChanged(false);
        return payload;
    }

    @Override
    public synchronized Payload createStoredPayload(String pid, InputStream in)
            throws StorageException {
        if (pid == null || in == null) {
            throw new StorageException("Error; Null parameter recieved");
        }
        try {
            PayloadType type = null;
            if (METADATA_PAYLOAD.equals(pid)) {
                type = PayloadType.Annotation;
            } else if (getSourceId() == null) {
                if (pid.endsWith("json") || pid.endsWith("tfpackage")) {
                    type = PayloadType.Source;
                    setSourceId(pid);
                } else {
                    type = PayloadType.Annotation;
                }
            } else {
                type = PayloadType.Annotation;
            }
            Payload payload = createPayload(pid, in, false, type);
            return payload;
        } catch (Exception e) {
            throw new StorageException(e);
        }
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
        Map<String, Object> fileInfo = getFileInfo(pid);
        return new MongoPayload(this, pid, (String) fileInfo.get("payloadId"),
                payloadBackend);
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
    public synchronized Payload updatePayload(String pid, InputStream in)
            throws StorageException {
        MongoPayload payload = (MongoPayload) getPayload(pid);
        getManifest().remove(pid);
        removeFileMeta(pid);
        payload.update(in);
        getManifest().put(pid, payload);
        addFileMeta(payload);
        save();
        return payload;
    }

    @Override
    public void close() throws StorageException {
        save();
    }

    @Override
    public Map<String, Object> getObjectMetadata() {
        if (objectMetadata == null) {
            objectMetadata = getUnescapedDoc(getObjectMetadataFromDb());
            if (objectMetadata == null) {
                objectMetadata = new Document();
                objectMetadata.put("redboxOid", oid);
                objectMetadata.put("collectionName", collectionName);
                objectMetadata.put("date_object_created",
                        df.print(new Date().getTime()));
            }
        }
        return objectMetadata;
    }

    @Override
    public Map<String, Object> getRecordMetadata() {
        if (recordMetadata == null) {
            recordMetadata = getUnescapedDoc(getRecordMetadataFromDb());
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

    public synchronized void save() throws StorageException {
        MongoCollection<Document> objectMetaCol = getObjectMetadataCollection();
        MongoCollection<Document> metaCol = getMetaCollection();
        boolean isInStorage = existsInStorage();
        if (!isInStorage) {
            getObjectMetadata();
            mergeMetaProp();
            objectMetaCol.insertOne(getEscapedDoc(objectMetadata));
            getRecordMetadata();
            metaCol.insertOne(getEscapedDoc(recordMetadata));
        } else {
            objectMetadata.put("date_object_modified",
                    df.print(new Date().getTime()));
            mergeMetaProp();
            // full replacement of object metadata...
            objectMetaCol.findOneAndReplace(eq("redboxOid", oid),
                    getEscapedDoc(objectMetadata));
            // full replacement of record metadata...
            metaCol.findOneAndReplace(eq("redboxOid", oid),
                    getEscapedDoc(recordMetadata));
        }
    }

    private void mergeMetaProp() {
        // metadata properties overrides objectMetadata as this is the legacy
        // code's way of setting properties
        if (metadataProp != null && !metadataProp.isEmpty()) {
            log.info("merging properties...");
            for (Map.Entry<Object, Object> entry : metadataProp.entrySet()) {
                objectMetadata.put((String) entry.getKey(), entry.getValue());
            }
            log.info(objectMetadata.toString());
        } else {
            log.info(
                    "Trying to merge meta properties, but there was nothing there.");
        }
    }

    public Document getEscapedDoc(final Document source) {
        Document escapedVer = null;
        if (source != null) {
            escapedVer = new Document();
            for (String key : source.keySet()) {
                escapedVer.put(escapeKey(key), source.get(key));
                Object val = source.get(key);
                if (val instanceof Map || val instanceof List) {
                    if (val instanceof List) {
                        ArrayList list = new ArrayList();
                        for (Object entryVal : (List) val) {
                            if (entryVal instanceof Map) {
                                list.add(getEscapedDoc(
                                        new Document((Map) entryVal)));
                            } else {
                                list.add(entryVal);
                            }

                        }
                        escapedVer.put(escapeKey(key), list);
                    } else {
                        escapedVer.put(escapeKey(key),
                                getEscapedDoc(new Document((Map) val)));
                    }
                }
            }
        }
        return escapedVer;
    }

    public Document getUnescapedDoc(final Document source) {
        Document unEscapedVer = null;
        if (source != null) {
            unEscapedVer = new Document();
            for (String key : source.keySet()) {
                unEscapedVer.put(unescapeKey(key), source.get(key));
                Object val = source.get(key);
                if (val instanceof Map || val instanceof List) {
                    if (val instanceof List) {
                        ArrayList list = new ArrayList();
                        for (Object entryVal : (List) val) {
                            if (entryVal instanceof Map) {
                                list.add(getUnescapedDoc((Document) entryVal));
                            } else {
                                list.add(entryVal);
                            }
                        }
                        unEscapedVer.put(unescapeKey(key), list);
                    } else {
                        unEscapedVer.put(unescapeKey(key),
                                getUnescapedDoc((Document) source.get(key)));
                    }
                }

            }
        }
        return unEscapedVer;
    }

    private String escapeKey(String key) {
        for (String encodedKey : encodedChars.keySet()) {
            key = key.replace(encodedKey, encodedChars.get(encodedKey));
        }
        return key;
    }

    private String unescapeKey(String key) {
        for (String encodedKey : encodedChars.keySet()) {
            String decodedKey = encodedChars.get(encodedKey);
            key = key.replace(decodedKey, encodedKey);
        }
        return key;
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
    public List<Map<String, Object>> getFileList() {
        if (recordMetadata == null) {
            return null;
        }
        return (List<Map<String, Object>>) recordMetadata.get("files");
    }

    protected void addFileMeta(MongoPayload payload) {
        List<Map<String, Object>> files = getFileList();
        if (files == null) {
            files = new ArrayList<Map<String, Object>>();
            recordMetadata.put("files", files);
        }
        Map<String, Object> info = payload.getMetadataDoc();
        files.add(info);
    }

    protected void removeFileMeta(String pid) {
        List<Map<String, Object>> files = getFileList();
        Map<String, Object> metaToRemove = null;
        if (files != null) {
            for (Map<String, Object> info : files) {
                if (info.get("pid").equals(pid)) {
                    metaToRemove = info;
                }
            }
            if (metaToRemove != null) {
                files.remove(metaToRemove);
            }

        }
    }

    public void updatePayloadMeta(MongoPayload payload)
            throws StorageException {
        removeFileMeta(payload.getId());
        addFileMeta(payload);
        save();
        payload.setMetaChanged(false);
    }

    public Map<String, Object> getPayloadMeta(final String pid) {
        List<Map<String, Object>> files = getFileList();
        Map<String, Object> metaToFind = null;
        if (files != null) {
            for (Map<String, Object> info : files) {
                if (info.get("pid").equals(pid)) {
                    metaToFind = info;
                }
            }
        }
        return metaToFind;
    }

}
