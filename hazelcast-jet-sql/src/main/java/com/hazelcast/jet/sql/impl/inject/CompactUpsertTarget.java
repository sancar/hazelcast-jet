package com.hazelcast.jet.sql.impl.inject;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.compact.Compact;
import com.hazelcast.internal.serialization.impl.compact.FieldDescriptor;
import com.hazelcast.internal.serialization.impl.compact.Schema;
import com.hazelcast.nio.serialization.FieldType;
import com.hazelcast.nio.serialization.GenericRecord;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.type.QueryDataType;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static com.hazelcast.jet.sql.impl.inject.UpsertInjector.FAILING_TOP_LEVEL_INJECTOR;
import static com.hazelcast.sql.impl.type.QueryDataType.VARCHAR;

public class CompactUpsertTarget implements UpsertTarget {
    private final Schema schema;
    private final Compact compact;
    private GenericRecord.Builder recordBuilder;

    CompactUpsertTarget(InternalSerializationService serializationService, long schemaId) {
        this.compact = serializationService.getCompact();
        this.schema = compact.getSchemaRegistrar().lookupSchema(schemaId);
    }

    @Override
    public UpsertInjector createInjector(@Nullable String path, QueryDataType type) {
        if (path == null) {
            return FAILING_TOP_LEVEL_INJECTOR;
        }

        FieldDescriptor field = schema.getField(path);
        return value -> {
            if (field == null) {
                throw QueryException.error("Unable to inject a value to \"" + path + "\"");
            }

            if (value != null) {
                write(recordBuilder, field, value);
            }
        };
    }

    @Override
    public void init() {
        recordBuilder = compact.createGenericRecordBuilder(schema);
    }

    @Override
    public Object conclude() {
        GenericRecord record = recordBuilder.build();
        recordBuilder = compact.createGenericRecordBuilder(schema);
        return record;
    }

    private static void write(GenericRecord.Builder builder, FieldDescriptor fieldDescriptor, @Nonnull Object value) {
        String name = fieldDescriptor.getName();
        FieldType type = fieldDescriptor.getType();

        try {
            switch (type) {
                case BOOLEAN:
                    builder.writeBoolean(name, (boolean) value);
                    break;
                case BYTE:
                    builder.writeByte(name, (byte) value);
                    break;
                case SHORT:
                    builder.writeShort(name, (short) value);
                    break;
                case CHAR:
                    builder.writeChar(name, (char) value);
                    break;
                case INT:
                    builder.writeInt(name, (int) value);
                    break;
                case LONG:
                    builder.writeLong(name, (long) value);
                    break;
                case FLOAT:
                    builder.writeFloat(name, (float) value);
                    break;
                case DOUBLE:
                    builder.writeDouble(name, (double) value);
                    break;
                case UTF:
                    builder.writeUTF(name, (String) VARCHAR.convert(value));
                    break;
                case OBJECT:
                    builder.writeGenericRecord(name, (GenericRecord) value);
                    break;
                case BOOLEAN_ARRAY:
                    builder.writeBooleanArray(name, (boolean[]) value);
                    break;
                case BYTE_ARRAY:
                    builder.writeByteArray(name, (byte[]) value);
                    break;
                case SHORT_ARRAY:
                    builder.writeShortArray(name, (short[]) value);
                    break;
                case CHAR_ARRAY:
                    builder.writeCharArray(name, (char[]) value);
                    break;
                case INT_ARRAY:
                    builder.writeIntArray(name, (int[]) value);
                    break;
                case LONG_ARRAY:
                    builder.writeLongArray(name, (long[]) value);
                    break;
                case FLOAT_ARRAY:
                    builder.writeFloatArray(name, (float[]) value);
                    break;
                case DOUBLE_ARRAY:
                    builder.writeDoubleArray(name, (double[]) value);
                    break;
                case UTF_ARRAY:
                    builder.writeUTFArray(name, (String[]) value);
                    break;
                case OBJECT_ARRAY:
                    builder.writeGenericRecordArray(name, (GenericRecord[]) value);
                    break;
                default:
                    throw QueryException.error("Unsupported type: " + type);
            }
        } catch (Exception e) {
            throw QueryException.error("Cannot set value " +
                    (" of type " + value.getClass().getName())
                    + " to field \"" + name + "\" of type " + type + ": " + e.getMessage(), e);
        }
    }
}
