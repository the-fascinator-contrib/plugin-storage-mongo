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
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.bson.BsonDocument;
import org.bson.Document;

import com.googlecode.fascinator.api.PluginDescription;
import com.googlecode.fascinator.api.PluginException;
import com.googlecode.fascinator.api.storage.DigitalObject;
import com.googlecode.fascinator.api.storage.JsonDigitalObject;
import com.googlecode.fascinator.api.storage.JsonStorage;
import com.googlecode.fascinator.api.storage.StorageException;
import com.googlecode.fascinator.common.JsonObject;
import com.googlecode.fascinator.common.JsonSimple;
import com.googlecode.fascinator.common.JsonSimpleConfig;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientOptions.Builder;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.IndexOptions;

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
	private String recordMetadataViewName;

	private MongoDigitalObject.PayloadBackend payloadBackend;

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
			throw new StorageException("Failed to read file configuration!", ioe);
		}
	}

	@Override
	public void init(String jsonString) throws PluginException {
		try {
			systemConfig = new JsonSimpleConfig(jsonString);
			init();
		} catch (IOException ioe) {
			throw new StorageException("Failed to read string configuration!", ioe);
		}
	}

	private void init() {
		String host = systemConfig.getString("localhost", "storage", "mongo", "host");
		int port = systemConfig.getInteger(27017, "storage", "mongo", "port").intValue();
		String db = systemConfig.getString("redbox", "storage", "mongo", "db");
		boolean sslEnable = systemConfig.getBoolean(false, "storage", "mongo", "sslEnable");
		String username = systemConfig.getString(null, "storage", "mongo", "username");
		String password = systemConfig.getString(null, "storage", "mongo", "password");
		defaultCollection = systemConfig.getString("default", "storage", "mongo", "defaultCollection");
		objectMetadataCollectionName = systemConfig.getString("tf_obj_meta", "storage", "mongo", "metadataCollection");

		recordMetadataViewName = systemConfig.getString("metadataDocuments", "storage", "mongo",
				"recordMetadataCollection");

		String payloadBackendName = systemConfig.getString("MONGO", "storage", "mongo", "payload_backend");
		payloadBackend = MongoDigitalObject.PayloadBackend.valueOf(payloadBackendName);
		Builder optionBuilder = MongoClientOptions.builder().sslEnabled(sslEnable);

		ServerAddress addr = new ServerAddress(new InetSocketAddress(host, port));
		if (username == null) {
			mongoClient = new MongoClient(addr, optionBuilder.build());
		} else {
			List<MongoCredential> credentialsList = new ArrayList<MongoCredential>();
			credentialsList.add(MongoCredential.createCredential(username, db, password.toCharArray()));
			mongoClient = new MongoClient(addr, credentialsList, optionBuilder.build());
		}

		mongoDb = mongoClient.getDatabase(db);

		createMetadataDocumentsView();
		createPackageViews();

	}

	/**
	 * Create views for each package type to aid querying
	 */
	private void createPackageViews() {

		JsonObject packageTypes = systemConfig.getObject("portal", "packageTypes");
		if (packageTypes != null) {

			for (Object keyObject : packageTypes.keySet()) {
				String key = (String) keyObject;
				List pipeline = Arrays.asList(BsonDocument.parse("{$match: { 'packageType' : '" + key + "'}}"));
				if (key.equals("default") || key.equals(this.recordMetadataViewName)) {
					key = key + "_package";
				}
				try {
					mongoDb.createView(key, this.recordMetadataViewName, pipeline);
				} catch (MongoCommandException e) {
					// Error code 48 means that the view has already been
					// created
					if (e.getCode() != 48) {
						throw e;
					}
				}
			}
		}

	}

	private void createMetadataDocumentsView() {
		List pipeline = Arrays.asList(
				BsonDocument.parse("{$match: {files: { $elemMatch:{ pid: 'metadata.tfpackage'}}}}"),
				BsonDocument
						.parse("{ $project: { files: { '$filter': { input: '$files', as: 'files', cond: {$eq: ['$$files.pid','metadata.tfpackage']}}}}}"),
				BsonDocument.parse("{ $unwind:  '$files' }"),
				BsonDocument.parse("{ $project: { 'metadata':'$files.source.payload', 'redboxOid': '$files.oid'}}"),
				BsonDocument
						.parse("{$lookup: { from: 'tf_obj_meta', localField:'redboxOid', foreignField: 'redboxOid', as: 'tfObj'}}"),
				BsonDocument
						.parse("{ $addFields: { 'metadata': { 'redboxOid':  '$redboxOid', 'packageType':'$tfObj.packageType', 'date_object_created':'$tfObj.date_object_created', 'date_object_modified':'$tfObj.date_object_modified' }}}"),
				BsonDocument.parse("{ $replaceRoot: { newRoot: '$metadata'}}"));
		try {
			mongoDb.createView(this.recordMetadataViewName, this.defaultCollection, pipeline);
			// create the required indices
			MongoCollection collection = mongoDb.getCollection(this.defaultCollection);
			IndexOptions options = new IndexOptions().sparse(true).background(true);
			collection.createIndex(Indexes.ascending("files.pid"), options);
			collection.createIndex(Indexes.compoundIndex(Indexes.ascending("files.pid", "files.source.payload.metaMetadata.brandId", "files.source.payload.metaMetadata.type", "files.source.payload.workflow.stage")), options.name("metaViewIndex"));
			options = new IndexOptions().sparse(true).background(true);
			collection = mongoDb.getCollection("tf_obj_meta");
			collection.createIndex(Indexes.ascending("redboxOid"), options);
		} catch (MongoCommandException e) {
			// Error code 48 means that the view has already been created
			if (e.getCode() != 48) {
				throw e;
			}
		}
	}

	@Override
	public void shutdown() throws PluginException {
		mongoClient.close();
	}

	@Override
	public DigitalObject createObject(String oid) throws StorageException {
		return createObject(oid, defaultCollection);
	}

	public synchronized JsonDigitalObject createObject(String oid, String collectionName) throws StorageException {
		if (oid == null) {
			throw new StorageException("Cannot create object in storage with NULL oid");
		}
		// start with the object...
		MongoDigitalObject obj = new MongoDigitalObject(mongoDb, collectionName, objectMetadataCollectionName, oid,
				payloadBackend);
		if (obj.existsInStorage()) {
			throw new StorageException("Error; object '" + oid + "' already exists in MongoDB");
		}
		obj.save();
		return obj;
	}

	@Override
	public DigitalObject getObject(String oid) throws StorageException {
		return getObject(oid, defaultCollection);
	}

	public JsonDigitalObject getObject(String oid, String collectionName) throws StorageException {
		MongoDigitalObject obj = new MongoDigitalObject(mongoDb, collectionName, objectMetadataCollectionName, oid,
				payloadBackend);
		obj.load();
		return obj;
	}

	@Override
	public void removeObject(String oid) throws StorageException {
		removeObject(oid, defaultCollection);
	}

	public void removeObject(String oid, String collectionName) throws StorageException {
		MongoDigitalObject obj = new MongoDigitalObject(mongoDb, collectionName, objectMetadataCollectionName, oid,
				payloadBackend);
		obj.remove();
	}

	@Override
	public Set<String> getObjectIdList() {
		Set<String> objectIdList = new HashSet<String>();
		MongoCollection<Document> objectMetaCol = mongoDb.getCollection(objectMetadataCollectionName);
		List<Document> objectIds = objectMetaCol.find(new Document())
				.projection(fields(include("redboxOid"), excludeId())).into(new ArrayList<Document>());
		for (Document doc : objectIds) {
			objectIdList.add(doc.getString("redboxOid"));
		}
		return objectIdList;
	}

	public void dropDb() throws Exception {
		mongoDb.drop();
	}

	public JsonSimple pagedQuery(String collection, String filterString) throws IOException {
		return pagedQuery(collection, filterString, 0, 10, null);
	}

	public JsonSimple pagedQuery(String collection, String filterString, int startIndex, int rows, String sort)
			throws IOException {
		System.out.println("Running paged query on: " + collection + " using: " + filterString + " sort:" + sort );
		long startStamp = System.currentTimeMillis();
		List<BsonDocument> pipeline = Arrays.asList(BsonDocument.parse("{$match:" + filterString + "}"),
				BsonDocument.parse("{'$group':{'_id': null, 'numFound': {'$sum': 1 }, 'docs':{ '$push':'$$ROOT' }} },"),
				BsonDocument.parse("{'$project': { 'numFound':1 , 'docs' : {'$slice': ['$docs'," + startIndex + ","
						+ rows + "] }   }}"));
		if (sort != null) {
			List<BsonDocument> newpipeline = new ArrayList<BsonDocument>(pipeline);
			newpipeline.add(1, BsonDocument.parse("{'$sort': {" + sort + "}}"));
			pipeline = newpipeline;
		}
		System.out.println("Pipeline:");
		System.out.println(pipeline.toString());
		AggregateIterable<Document> result = this.mongoDb.getCollection(collection).aggregate(pipeline).allowDiskUse(true);
		if (result.first() != null) {
			System.out.println("Query took: " + (System.currentTimeMillis() - startStamp));
			return new JsonSimple(result.first().toJson());
		} else {
			return new JsonSimple("{\"_id\": null,  \"numFound\": 0,  \"docs\": []}");
		}
	}

	public FindIterable<Document> query(String collection, String filterString) {
		BsonDocument filter = BsonDocument.parse(filterString);
		return this.mongoDb.getCollection(collection).find(filter);
	}

	/**
	 * Calls Mongo's aggregate function
	 *
	 * @param collection
	 * @param pipeline
	 * @return
	 */
	public AggregateIterable<Document> aggregate(String collection, List<BsonDocument> pipeline) {
		return this.mongoDb.getCollection(collection).aggregate(pipeline);
	}

}
