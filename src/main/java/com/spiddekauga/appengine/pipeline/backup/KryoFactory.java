package com.spiddekauga.appengine.pipeline.backup;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.serializers.FieldSerializer;
import com.esotericsoftware.kryo.serializers.TaggedFieldSerializer;
import com.google.appengine.api.blobstore.BlobKey;
import com.google.appengine.api.datastore.Blob;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.ShortBlob;
import com.spiddekauga.appengine.pipeline.EntitySaveObject;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;

/**
 * Pool for Kryo instances. When creating a new instance Kryo registers all necessary classes used
 * by the backup module
 */
class KryoFactory {
/**
 * Create a new kryo object
 */
static Kryo create() {
	Kryo kryo = new Kryo();
	kryo.setRegistrationRequired(true);
	kryo.setReferences(false);
	RegisterClasses.registerAll(kryo);
	return kryo;
}

/**
 * Contains all classes that should be registered. Adding new classes shall only be done at the end
 * of the enumeration. If a class isn't used any longer, don't remove it but set it as null
 * instead.
 */
private enum RegisterClasses {
	BYTE_ARRAY(byte[].class),
	DATASTORE_KEY(Key.class, new DatastoreKeySerializer()),
	BLOB_KEY(BlobKey.class, new BlobKeySerializer()),
	DATE(Date.class),
	HASH_MAP(HashMap.class),
	PROPERTY_WRAPPER(EntitySaveObject.PropertyWrapper.class),
	ENTITY_SAVE_OBJECT(EntitySaveObject.class, SerializerType.FIELD),
	BLOB(Blob.class, new BlobSerializer()),
	SHORT_BLOB(ShortBlob.class, new ShortBlobSerializer()),
	ARRAY_LIST(ArrayList.class);

	/** Offset for register id, as there exists some default registered types */
	private static final int OFFSET = 20;
	/** Class type to register, if null it is not registered */
	private Class<?> mType;
	/** Serializer to use, if null it uses the default serializer */
	private Serializer<?> mSerializer = null;
	/**
	 * If a serializer of the specified type should be created for this class. If null, no
	 * serializer will be created for this type.
	 */
	private SerializerType mSerializerType = null;

	/**
	 * Creates a new type to be registered with Kryo using {@link #registerAll(Kryo)}
	 * @param type the type to register, if null it won't register it. Setting to null is useful
	 * when the class isn't used anymore (doesn't exist) but we still need to keep the register
	 * order.
	 */
	private RegisterClasses(Class<?> type) {
		mType = type;
	}

	/**
	 * Creates a new type to be registered with Kryo using {@link #registerAll(Kryo)} and when
	 * {@link #createSerializers(Kryo)} is called will created the specified serializer type
	 * @param type the type to register, if null it won't register it. Setting to null is useful
	 * when the class isn't used anymore (doesn't exist) but we still need to keep the register
	 * order.
	 * @param createSerializerType the type of serializer to create when {@link
	 * #createSerializers(Kryo)} is called.
	 */
	private RegisterClasses(Class<?> type, SerializerType createSerializerType) {
		mType = type;
		mSerializerType = createSerializerType;
	}

	/**
	 * Creates a new type to be registered with Kryo using {@link #registerAll(Kryo)}
	 * @param type the type to register, if null it won't register it. Setting to null is useful
	 * when the class isn't used anymore (doesn't exist) but we still need to keep the register
	 * order.
	 * @param serializer the serializer to use for the specified type, if null the default
	 * serializer will be used instead.
	 */
	private RegisterClasses(Class<?> type, Serializer<?> serializer) {
		mType = type;
		mSerializer = serializer;
	}

	/**
	 * Registers all classes with serializers.
	 * @param kryo registers the serializers for this Kryo instance.
	 */
	public static void registerAll(Kryo kryo) {
		createSerializers(kryo);

		for (RegisterClasses registerClass : RegisterClasses.values()) {
			if (registerClass.mType != null) {
				if (registerClass.mSerializer == null) {
					kryo.register(registerClass.mType, registerClass.ordinal() + OFFSET);
				} else {
					kryo.register(registerClass.mType, registerClass.mSerializer, registerClass.ordinal() + OFFSET);
				}
			}
		}
	}

	/**
	 * Some classes needs a serializer that requires Kryo in the constructor. These serializers are
	 * created with this method instead.
	 * @param kryo creates the serializers for this Kryo instance.
	 */
	private static void createSerializers(Kryo kryo) {
		// Create tagged or compatible serializers
		for (RegisterClasses registerClass : RegisterClasses.values()) {
			if (registerClass.mSerializerType != null) {
				switch (registerClass.mSerializerType) {
				case FIELD:
					registerClass.mSerializer = new FieldSerializer<Object>(kryo, registerClass.mType);
					break;

				case TAGGED:
					registerClass.mSerializer = new TaggedFieldSerializer<Object>(kryo, registerClass.mType);
					break;
				}
			}
		}
	}

	/**
	 * Serializer types
	 */
	private enum SerializerType {
		/** Field serializer */
		FIELD,
		/** Creates a TaggedFieldSerializer for the type */
		TAGGED,
	}
}
}
