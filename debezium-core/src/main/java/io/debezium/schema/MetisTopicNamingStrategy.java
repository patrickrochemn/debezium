/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.schema;

import java.util.Properties;

import io.debezium.common.annotation.Incubating;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.util.Collect;

/**
 * Determine data event topic names using {@link DataCollectionId#databaseParts()}.
 *
 * @author Patrick Roche
 */
@Incubating
public class MetisTopicNamingStrategy extends AbstractTopicNamingStrategy<DataCollectionId> {

    public MetisTopicNamingStrategy(Properties props) {
        super(props);
    }

    public static MetisTopicNamingStrategy create(CommonConnectorConfig config) {
        return new MetisTopicNamingStrategy(config.getConfig().asProperties());
    }

    @Override
    public String dataChangeTopic(DataCollectionId id) {
        String topicName = mkString(Collect.arrayListOf(prefix, id.databaseParts()), delimiter);
        return topicNames.computeIfAbsent(id, t -> sanitizedTopicName(topicName));
    }
}
