package com.spiddekauga.appengine.pipeline;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;

import java.util.HashMap;
import java.util.Map;

/**
 * Converts a datastore entity to an entity that can be saved through kryo
 */
public class EntitySaveObject {
public Key key;
public Map<String, PropertyWrapper> properties = new HashMap<>();

/**
 * Default constructor
 */
public EntitySaveObject() {

}

/**
 * Convert a datastore entity into this object
 * @param entity the datastore entity to convert into this object
 */
public EntitySaveObject(Entity entity) {
	key = entity.getKey();

	// Save indexed and unindexed value too
	for (Map.Entry<String, Object> propertyEntry : entity.getProperties().entrySet()) {
		String propertyName = propertyEntry.getKey();
		Object property = propertyEntry.getValue();
		PropertyWrapper propertyWrapper = new PropertyWrapper();
		propertyWrapper.object = property;
		propertyWrapper.indexed = !entity.isUnindexedProperty(propertyName);
		properties.put(propertyName, propertyWrapper);
	}
}

/**
 * Convert back to a datastore entity
 */
public Entity createDatastoreEntity() {
	Entity entity = new Entity(key);

	// Set properties
	for (Map.Entry<String, PropertyWrapper> propertyEntry : properties.entrySet()) {
		String propertyName = propertyEntry.getKey();
		PropertyWrapper propertyWrapper = propertyEntry.getValue();
		if (propertyWrapper.indexed) {
			entity.setProperty(propertyName, propertyWrapper.object);
		} else {
			entity.setUnindexedProperty(propertyName, propertyWrapper.object);
		}
	}

	return entity;
}

public class PropertyWrapper {
	public Object object;
	public boolean indexed;
}
}
