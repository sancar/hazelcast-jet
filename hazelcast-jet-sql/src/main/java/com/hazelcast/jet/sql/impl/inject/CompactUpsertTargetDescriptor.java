package com.hazelcast.jet.sql.impl.inject;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

public class CompactUpsertTargetDescriptor implements UpsertTargetDescriptor{

    private long schemaId;

    @SuppressWarnings("unused")
    private CompactUpsertTargetDescriptor() {
    }

    public CompactUpsertTargetDescriptor(long schemaId) {
        this.schemaId = schemaId;
    }

    @Override
    public UpsertTarget create(InternalSerializationService serializationService) {
        return new CompactUpsertTarget(serializationService, schemaId);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(schemaId);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        schemaId = in.readLong();
    }
}
