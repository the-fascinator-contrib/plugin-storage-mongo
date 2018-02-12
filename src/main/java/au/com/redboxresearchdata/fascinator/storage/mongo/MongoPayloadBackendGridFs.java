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

import java.io.InputStream;

import org.bson.Document;
import org.bson.types.ObjectId;

import com.googlecode.fascinator.api.storage.StorageException;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.gridfs.GridFSBucket;
import com.mongodb.client.gridfs.GridFSBuckets;
import com.mongodb.client.gridfs.GridFSDownloadStream;
import com.mongodb.client.gridfs.model.GridFSFile;
import com.mongodb.client.gridfs.model.GridFSUploadOptions;

/**
 * MongoPayloadBackendGridFs
 *
 * @author <a target='_' href='https://github.com/shilob'>Shilo Banihit</a>
 *
 */
public class MongoPayloadBackendGridFs implements MongoPayloadBackend {
    static String TYPE = "GRIDFS";
    protected String payloadPath;
    protected ObjectId mongoFileId;
    protected GridFSDownloadStream ds;
    protected MongoDatabase mongoDb;

    public MongoPayloadBackendGridFs(String payloadPath, String fileId,
            MongoDatabase db) {
        this.payloadPath = payloadPath;
        mongoDb = db;
        if (fileId != null) {
            mongoFileId = new ObjectId(fileId);
        }
    }

    /**
     * Persists the data from 'source' into GridFS using the 'payloadPath'
     * property as filename.
     *
     * @param source - the source input stream
     * @param metadata - the doc metadata
     */

    public void create(InputStream source, Document metadata)
            throws StorageException {
        GridFSUploadOptions options = new GridFSUploadOptions()
                .metadata(metadata);
        mongoFileId = getBucket().uploadFromStream(payloadPath, source,
                options);
    }

    public InputStream open() {
        ds = getBucket().openDownloadStream(mongoFileId);

        GridFSFile file = ds.getGridFSFile();
        mongoFileId = file.getObjectId();
        // setMetadataDoc(file.getMetadata());
        return ds;
    }

    public Long size() {
        if (ds == null) {
            ds = getBucket().openDownloadStream(mongoFileId);
        }
        return ds.getGridFSFile().getLength();
    }

    public String getId() {
        return mongoFileId.toString();
    }

    public void setId(String id) {
        mongoFileId = new ObjectId(id);
    }

    public void remove() {
        getBucket().delete(mongoFileId);
        ds = null;
        mongoFileId = null;
    }

    private GridFSBucket getBucket() {
        return GridFSBuckets.create(mongoDb);
    }

    @Override
    public Document getMetadata() {
        return new Document();
    }

    @Override
    public void setMetadata(Document doc) {

    }

    public String getType() {
        return TYPE;
    }

}
