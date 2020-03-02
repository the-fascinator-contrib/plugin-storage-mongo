/*******************************************************************************
 * Copyright (C) 2018 Queensland Cyber Infrastructure Foundation (http://www.qcif.edu.au/)
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

import java.io.*;

import com.google.gson.Gson;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.bson.Document;

import com.googlecode.fascinator.api.storage.StorageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MongoPayloadBackendCollection
 *
 * @author <a target='_' href='https://github.com/shilob'>Shilo Banihit</a>
 *
 */
public class MongoPayloadBackendCollection implements MongoPayloadBackend {

    private static Logger log = LoggerFactory.getLogger(MongoPayloadBackendCollection.class);
    static String SOURCE_FIELD = "source";
    static String PAYLOAD_FIELD = "payload";
    static String METADATA_FIELD = "metadata";
    static String TYPE = "COLLECTION_EMBEDDED";

    MongoDigitalObject obj;
    String pid;
    Document mainDoc;

    @SuppressWarnings("unchecked")
    public MongoPayloadBackendCollection(MongoDigitalObject obj, String pid) {
        this.obj = obj;
        this.pid = pid;
        Map<String, Object> payloadMeta = obj.getPayloadMeta(pid);
        if (payloadMeta != null) {
            mainDoc = (Document) payloadMeta.get(getId());
        }
        if (mainDoc == null) {
            mainDoc = new Document();
        }
    }

    public Document getMetadata() {
        return mainDoc;
    }

    public void setMetadata(Document doc) {
        mainDoc = doc;
    }

    @Override
    public void create(InputStream source, Document metadata)
            throws StorageException {
        log.debug("------------- Creating PID: " + pid);
        try {
            Document sourceDoc = getDocumentFromJsonStream(source);
            if (sourceDoc == null) {
                if (obj.existsInStorage()) {
                    log.debug(
                            "Swallowing error.... there was an attempt to update a payload using empty data.");
                } else {
                    throw new StorageException(
                            "Cannot create payload with empty data.");
                }
            } else {
                mainDoc.put(PAYLOAD_FIELD, sourceDoc);
                mainDoc.put(METADATA_FIELD, metadata);
            }
        } catch (Exception e) {
            log.error("Payload create failed:", e);
            throw new StorageException(e);
        }
    }

    private Document getDocumentFromJsonStream(InputStream source)
            throws Exception {
        try {
            StringWriter writer = new StringWriter();
            IOUtils.copy(source, writer, "UTF-8");
            String jsonStr = writer.toString();
            log.debug("Converint to Document:");
            log.debug(jsonStr);
            Gson gson = new Gson();
            Document document = new Document();
            document = (Document) gson.fromJson(jsonStr, document.getClass());
            return document;
        } catch (Exception e) {
            log.error("Failed to create document from JSON stream:", e);
            throw e;
        }
    }

    private Document getDoc() {
        return (Document) mainDoc.get(PAYLOAD_FIELD);
    }

    @Override
    public InputStream open() {
        return new ByteArrayInputStream(getBytes());
    }

    private byte[] getBytes() {
        String s = getAsString();
        log.debug("Get as string: " + s);
        try {
            return s.getBytes(StandardCharsets.UTF_8.name());
        } catch (UnsupportedEncodingException e) {
            return s.getBytes();
        }
    }

    public String getAsString() {
        Document document = getDoc();
        StringBuilder writer = new StringBuilder();
        Gson gson = new Gson();
        gson.toJson((Object) document, writer);
        return writer.toString();
    }

    @Override
    public Long size() {
        return (long) getBytes().length;
    }

    @Override
    public String getId() {
        return SOURCE_FIELD;
    }

    @Override
    public void remove() {
        obj.removeFileMeta(pid);
    }

    public String getType() {
        return TYPE;
    }

    public void setId(String id) {
        // ignore...
    }

}
