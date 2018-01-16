/*
 * The Fascinator - MongoDB Storage Plugin
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
 */
package au.com.redboxresearchdata.fascinator.storage.mongo;

import static com.mongodb.client.model.Projections.excludeId;
import static com.mongodb.client.model.Projections.fields;
import static com.mongodb.client.model.Projections.include;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.bson.Document;

import com.googlecode.fascinator.api.PluginDescription;
import com.googlecode.fascinator.api.PluginException;
import com.googlecode.fascinator.api.storage.DigitalObject;
import com.googlecode.fascinator.api.storage.JsonDigitalObject;
import com.googlecode.fascinator.api.storage.JsonStorage;
import com.googlecode.fascinator.api.storage.StorageException;
import com.googlecode.fascinator.common.JsonSimpleConfig;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

/**
 * <p>
 * This plugin provides MongoDB storage.
 * </p>
 *
 * <h3>Configuration</h3>
 * <table border="1">
 * <tr>
 * <th>Option</th>
 * <th>Description</th>
 * <th>Required</th>
 * <th>Default</th>
 * </tr>
 * <tr>
 * <td>url</td>
 * <td>Base URL of a Fedora Commons server</td>
 * <td><b>Yes</b></td>
 * <td>http://localhost:8080/fedora</td>
 * </tr>
 * <tr>
 * <td>username</td>
 * <td>Fedora user account with read/write access</td>
 * <td><b>Yes</b> (depending on server setup)</td>
 * <td>fedoraAdmin</td>
 * </tr>
 * <tr>
 * <td>password</td>
 * <td>Password for the above user account</td>
 * <td><b>Yes</b> (depending on server setup)</td>
 * <td>fedoraAdmin</td>
 * </tr>
 * <tr>
 * <td>namespace</td>
 * <td>Namespace to use for Fedora Object PIDs</td>
 * <td>No</td>
 * <td>uuid</td>
 * </tr>
 * </table>
 *
 * <h3>Sample configuration</h3>
 *
 * <pre>
 * {
 *     "storage": {
 *
 *     }
 * }
 * </pre>
 *
 * @author <a href="https://github.com/shilob" target="_blank">Shilo Banihit</a>
 */
public class MongoStorage implements JsonStorage {

    /** System Config */
    private JsonSimpleConfig systemConfig;

    private MongoClient mongoClient;
    private MongoDatabase mongoDb;
    private String defaultCollection;
    private String objectMetadataCollectionName;

    private JsonDigitalObject.PayloadBackend payloadBackend;

    @Override
    public String getId() {
        return "mongodb-storage";
    }

    @Override
    public String getName() {
        return "MongoDB Storage Plugin";
    }

    @Override
    public PluginDescription getPluginDetails() {
        return new PluginDescription(this);
    }

    @Override
    public void init(File jsonFile) throws PluginException {
        try {
            systemConfig = new JsonSimpleConfig(jsonFile);
            init();

        } catch (IOException ioe) {
            throw new StorageException("Failed to read file configuration!",
                    ioe);
        }
    }

    @Override
    public void init(String jsonString) throws PluginException {
        try {
            systemConfig = new JsonSimpleConfig(jsonString);
            init();
        } catch (IOException ioe) {
            throw new StorageException("Failed to read string configuration!",
                    ioe);
        }
    }

    private void init() {
        String host = systemConfig.getString("localhost", "storage", "mongo",
                "host");
        int port = systemConfig.getInteger(27017, "storage", "mongo", "port")
                .intValue();
        String db = systemConfig.getString("redbox", "storage", "mongo", "db");
        defaultCollection = systemConfig.getString("default", "storage",
                "mongo", "defaultCollection");
        objectMetadataCollectionName = systemConfig.getString("tf_obj_meta",
                "storage", "mongo", "metadataCollection");

        String payloadBackendName = systemConfig.getString("GRIDFS", "storage",
                "mongo", "payload_backend");
        payloadBackend = JsonDigitalObject.PayloadBackend
                .valueOf(payloadBackendName);
        mongoClient = new MongoClient(host, port);
        mongoDb = mongoClient.getDatabase(db);
    }

    @Override
    public void shutdown() throws PluginException {
        mongoClient.close();
    }

    @Override
    public DigitalObject createObject(String oid) throws StorageException {
        return createObject(oid, defaultCollection);
    }

    public synchronized JsonDigitalObject createObject(String oid,
            String collectionName) throws StorageException {
        if (oid == null) {
            throw new StorageException(
                    "Cannot create object in storage with NULL oid");
        }
        // start with the object...
        MongoDigitalObject obj = new MongoDigitalObject(mongoDb, collectionName,
                objectMetadataCollectionName, oid,
                JsonDigitalObject.PayloadBackend.GRIDFS);
        if (obj.existsInStorage()) {
            throw new StorageException(
                    "Error; object '" + oid + "' already exists in MongoDB");
        }
        obj.save();
        return obj;
    }

    @Override
    public DigitalObject getObject(String oid) throws StorageException {
        return getObject(oid, defaultCollection);
    }

    public JsonDigitalObject getObject(String oid, String collectionName)
            throws StorageException {
        MongoDigitalObject obj = new MongoDigitalObject(mongoDb, collectionName,
                objectMetadataCollectionName, oid,
                JsonDigitalObject.PayloadBackend.GRIDFS);
        obj.load();
        return obj;
    }

    @Override
    public void removeObject(String oid) throws StorageException {
        removeObject(oid, defaultCollection);
    }

    public void removeObject(String oid, String collectionName)
            throws StorageException {
        MongoDigitalObject obj = new MongoDigitalObject(mongoDb, collectionName,
                objectMetadataCollectionName, oid,
                JsonDigitalObject.PayloadBackend.GRIDFS);
        obj.remove();
    }

    @Override
    public Set<String> getObjectIdList() {
        Set<String> objectIdList = new HashSet<String>();
        MongoCollection<Document> objectMetaCol = mongoDb
                .getCollection(objectMetadataCollectionName);
        List<Document> objectIds = objectMetaCol.find(new Document())
                .projection(fields(include("redboxOid"), excludeId()))
                .into(new ArrayList<Document>());
        for (Document doc : objectIds) {
            objectIdList.add(doc.getString("redboxOid"));
        }
        return objectIdList;
    }

    public void dropDb() throws Exception {
        mongoDb.drop();
    }

}
