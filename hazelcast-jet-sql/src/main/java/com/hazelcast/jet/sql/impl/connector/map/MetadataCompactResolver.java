package com.hazelcast.jet.sql.impl.connector.map;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.compact.FieldDescriptor;
import com.hazelcast.internal.serialization.impl.compact.Schema;
import com.hazelcast.internal.serialization.impl.compact.SchemaImpl;
import com.hazelcast.internal.util.BiTuple;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadata;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadataResolver;
import com.hazelcast.jet.sql.impl.inject.CompactUpsertTargetDescriptor;
import com.hazelcast.jet.sql.impl.schema.MappingField;
import com.hazelcast.nio.serialization.FieldType;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.extract.GenericQueryTargetDescriptor;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.schema.map.MapTableField;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static com.hazelcast.jet.sql.impl.connector.SqlConnector.COMPACT_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_COMPACT_NAME;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_COMPACT_VERSION;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_COMPACT_NAME;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_COMPACT_VERSION;
import static com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadataResolvers.extractFields;
import static com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadataResolvers.maybeAddDefaultField;
import static java.lang.Integer.parseInt;

public class MetadataCompactResolver implements KvMetadataResolver {

    static final MetadataCompactResolver INSTANCE = new MetadataCompactResolver();
    private Map<BiTuple<Integer, String>, Schema> localSchemaRegistry = new ConcurrentHashMap<>();

    private MetadataCompactResolver() {
    }

    @Override
    public String supportedFormat() {
        return COMPACT_FORMAT;
    }

    @Override
    public List<MappingField> resolveAndValidateFields(
            boolean isKey,
            List<MappingField> userFields,
            Map<String, String> options,
            InternalSerializationService serializationService
    ) {
        Schema schema = resolveSchema(isKey, options);
        return resolveFields(isKey, userFields, schema);
    }

    List<MappingField> resolveFields(
            boolean isKey,
            List<MappingField> userFields,
            Schema schema
    ) {
        Set<Map.Entry<String, FieldType>> fieldsInClass = resolveCompact(schema).entrySet();

        Map<QueryPath, MappingField> userFieldsByPath = extractFields(userFields, isKey);

        //TODO sancar not sure if the following should change
        if (!userFields.isEmpty()) {
            // the user used explicit fields in the DDL, just validate them
            for (Map.Entry<String, FieldType> classField : fieldsInClass) {
                QueryPath path = new QueryPath(classField.getKey(), isKey);
                QueryDataType type = resolvePortableType(classField.getValue());

                MappingField mappingField = userFieldsByPath.get(path);
                if (mappingField != null && !type.getTypeFamily().equals(mappingField.type().getTypeFamily())) {
                    throw QueryException.error("Mismatch between declared and resolved type: " + mappingField.name());
                }
            }
            return new ArrayList<>(userFieldsByPath.values());
        } else {
            List<MappingField> fields = new ArrayList<>();
            for (Map.Entry<String, FieldType> classField : fieldsInClass) {
                QueryPath path = new QueryPath(classField.getKey(), isKey);
                QueryDataType type = resolvePortableType(classField.getValue());
                String name = classField.getKey();

                fields.add(new MappingField(name, type, path.toString()));
            }
            return fields;
        }
    }

    private static Map<String, FieldType> resolveCompact(Schema schema) {
        Map<String, FieldType> fields = new LinkedHashMap<>();
        for (FieldDescriptor fieldDescriptor : schema.getFields()) {
            fields.putIfAbsent(fieldDescriptor.getName(), fieldDescriptor.getType());
        }
        return fields;
    }

    @SuppressWarnings("checkstyle:ReturnCount")
    private static QueryDataType resolvePortableType(FieldType type) {
        switch (type) {
            case BOOLEAN:
                return QueryDataType.BOOLEAN;
            case BYTE:
                return QueryDataType.TINYINT;
            case SHORT:
                return QueryDataType.SMALLINT;
            case CHAR:
                return QueryDataType.VARCHAR_CHARACTER;
            case UTF:
                return QueryDataType.VARCHAR;
            case INT:
                return QueryDataType.INT;
            case LONG:
                return QueryDataType.BIGINT;
            case FLOAT:
                return QueryDataType.REAL;
            case DOUBLE:
                return QueryDataType.DOUBLE;
            default:
                return QueryDataType.OBJECT;
        }
    }

    @Override
    public KvMetadata resolveMetadata(
            boolean isKey,
            List<MappingField> resolvedFields,
            Map<String, String> options,
            InternalSerializationService serializationService
    ) {
        Schema schema = resolveSchema(isKey, options);
        return resolveMetadata(isKey, resolvedFields, schema);
    }

    KvMetadata resolveMetadata(
            boolean isKey,
            List<MappingField> resolvedFields,
            Schema schema
    ) {
        Map<QueryPath, MappingField> externalFieldsByPath = extractFields(resolvedFields, isKey);

        List<TableField> fields = new ArrayList<>();
        for (Map.Entry<QueryPath, MappingField> entry : externalFieldsByPath.entrySet()) {
            QueryPath path = entry.getKey();
            QueryDataType type = entry.getValue().type();
            String name = entry.getValue().name();

            fields.add(new MapTableField(name, type, false, path));
        }

        maybeAddDefaultField(isKey, externalFieldsByPath, fields);
        return new KvMetadata(
                fields,
                GenericQueryTargetDescriptor.DEFAULT,
                new CompactUpsertTargetDescriptor(
                        ((SchemaImpl)schema).getSchemaId()
                )
        );
    }

    private Schema resolveSchema(
            boolean isKey,
            Map<String, String> options
    ) {
        String nameProperty = isKey ? OPTION_KEY_COMPACT_NAME : OPTION_VALUE_COMPACT_NAME;
        String name = options.get(nameProperty);
        String versionProperty = isKey ? OPTION_KEY_COMPACT_VERSION : OPTION_VALUE_COMPACT_VERSION;
        String versionString = options.get(versionProperty);

        if (name == null) {
            throw QueryException.error(
                    "Unable to resolve table metadata. Missing ['"
                            + name
                            + "'] option(s)");
        }
        int version = versionString == null ? 0 : parseInt(versionString);

        Schema schema = localSchemaRegistry.get(BiTuple.of(version, nameProperty));
        if (schema == null) {
            throw QueryException.error(
                    "Unable to find schema for name: " + name
                            + ", version: " + version
            );
        }
        return schema;
    }
}
