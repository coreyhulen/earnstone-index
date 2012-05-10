/*
 * Eindex: Earnstone Sharded Column Index for Cassandra
 * 
 * Copyright 2011 Corey Hulen, Earnstone Corporation
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. 
 */
package com.earnstone.index;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.ddl.ComparatorType;

public class ShardedLongIndex extends ShardedIndex<Long> {

	private static final byte[] emptyValue = { 0 };

	public ShardedLongIndex(Cluster cluster, Keyspace keyspace, String columnFamily, String name) {
		super(cluster, keyspace, columnFamily, name.getBytes());
	}
	
	@Override
	public Class<Long> getIndexType() {
		return Long.class;
	}

	@Override
	public ComparatorType getComparatorType() {
		return ComparatorType.LONGTYPE;
	}

	@Override
	public byte[] getEmptyValue() {
		return emptyValue;
	}

	@Override
	public Serializer<Long> getColumnNameSerializer() {
		return LongSerializer.get();
	}

	@Override
	public byte[] getBytesForData(Long data) {
		return LongSerializer.get().toBytes(data);
	}

	@Override
	public Long getDataForBytes(byte[] data) {
		return LongSerializer.get().fromBytes(data);
	}

	@Override
	public byte[] getBytesForDataList(List<Long> data) {

		try {
			ByteArrayOutputStream bos = new ByteArrayOutputStream(4096);

			for (Long d : data) {
				bos.write(getBytesForData(d));
			}

			bos.flush();
			bos.close();

			return bos.toByteArray();
		}
		catch (IOException e) {
			throw new IllegalArgumentException("Unknown error while serializing List<Long>.");
		}
	}

	@Override
	public List<Long> getDataListForBytes(byte[] data) {

		int listSize = data.length / 8;
		ArrayList<Long> list = new ArrayList<Long>(listSize);

		ByteArrayInputStream bis = new ByteArrayInputStream(data);

		byte[] raw = new byte[8];
		for (int i = 0; i < listSize; i++) {
			int bytesRead = bis.read(raw, 0, 8);

			if (bytesRead == 8)
				list.add(getDataForBytes(raw));
			else
				throw new IllegalArgumentException("Invalid raw byte[] size should be 8 bytes per long.");
		}

		return list;
	}

	@Override
	public Long getShardColumnName() {		
		return new Long(0);
	}

	@Override
	public Long getStatisticsColumnName() {
		return new Long(1);
	}	
}