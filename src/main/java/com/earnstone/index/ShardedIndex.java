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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TreeMap;

import org.apache.commons.lang.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ColumnType;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.exceptions.HectorException;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.ColumnQuery;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.SliceQuery;

public abstract class ShardedIndex<T> {

	private static byte[] Delim = { ':' };
	private static byte[] SubDelim = { ':', ':' };
	private static final Logger log = LoggerFactory.getLogger(ShardedIndex.class);

	protected Cluster cluster;
	protected Keyspace keyspace;
	protected String columnFamily;
	protected byte[] baseIndexKey;
	protected byte[] emptyIndexKey;
	protected TreeMap<T, T> shards;

	/**
	 * Constructs a sharded index. It is expected that multiple indexes of the
	 * same indexType are stored in the same column family and will be
	 * differentiated by the index name.
	 * 
	 * @param indexType
	 *            The type of the index to create (this is needed because of
	 *            type erasure).
	 * @param comparatorType
	 *            The Cassandra columnFamily comparator type.
	 * @param cluster
	 *            The running cluster.
	 * @param keyspace
	 *            Will verify if the keyspace exists and throws a
	 *            IllegalArgumentException if the keyspace doesn't exist.
	 * @param columnFamily
	 *            Will verify if the column family exists and throws a
	 *            IllegalArgumentException if the column family doesn't exist.
	 * @param name
	 *            the name of the index.
	 */
	protected ShardedIndex(Cluster cluster, Keyspace keyspace, String columnFamily, byte[] baseIndexKey) {
		this.cluster = cluster;
		this.keyspace = keyspace;
		this.columnFamily = columnFamily;
		this.baseIndexKey = baseIndexKey;

		if (!getIndexType().equals(Long.class)) {
			String msg = "Only indexType of Long.class is currently supported.";
			log.error(msg);
			throw new IllegalArgumentException(msg);
		}

		KeyspaceDefinition kdef = cluster.describeKeyspace(keyspace.getKeyspaceName());
		ColumnFamilyDefinition cdef = null;		

		for (ColumnFamilyDefinition tempDef : kdef.getCfDefs()) {
			if (tempDef.getName().equals(columnFamily)) {
				cdef = tempDef;
				break;
			}
		}

		if (cdef == null) {
			String msg = "Missing column family '" + columnFamily + "' for keyspace '" + keyspace + "'";
			log.error(msg);
			throw new IllegalArgumentException(msg);
		}

		if (!cdef.getColumnType().equals(ColumnType.STANDARD)) {
			String msg = "Expected column family '" + columnFamily + "' to be ColumnType.STANDARD";
			log.error(msg);
			throw new IllegalArgumentException(msg);
		}

		if (!cdef.getComparatorType().equals(getComparatorType())) {
			String msg = "Expected comparator of type '" + getComparatorType().getClassName() + "', but was '" + cdef.getComparatorType().getClassName() + "' for column family '" + columnFamily + "'";
			log.error(msg);
			throw new IllegalArgumentException(msg);
		}

		if (baseIndexKey == null || baseIndexKey.length == 0) {
			String msg = "Sharded index name cannot be null or empty.";
			log.error(msg);
			throw new IllegalArgumentException(msg);
		}

		emptyIndexKey = ArrayUtils.addAll(baseIndexKey, ArrayUtils.addAll(getEmptyValue(), Delim));

		reloadShardsCache();
	}

	public boolean initializeShardBoundries(List<T> startingBoundries) {

		if (shards.size() == 0) {
			TreeMap<T, T> map = new TreeMap<T, T>();

			for (T value : startingBoundries) {
				map.put(value, value);
			}

			saveShardsCache(map);
			shards = map;
			return true;
		}
		else {
			return false;
		}
	}

	public void reloadShardsCache() {		
		ColumnQuery<byte[], T, byte[]> columnQuery = HFactory.createColumnQuery(keyspace, BytesArraySerializer.get(), getColumnNameSerializer(), BytesArraySerializer.get());
		columnQuery.setColumnFamily(columnFamily).setKey(baseIndexKey).setName(getShardColumnName());
		QueryResult<HColumn<T, byte[]>> result = columnQuery.execute();

		if (result.get() == null)
			shards = new TreeMap<T, T>();
		else {
			List<T> list = getDataListForBytes(result.get().getValue());
			TreeMap<T, T> map = new TreeMap<T, T>();
			for (T value : list) {
				map.put(value, value);
			}
			
			shards = map;
		}
	}

	protected void saveShardsCache(TreeMap<T, T> map) {
		byte[] raw = getBytesForDataList(new ArrayList<T>(map.values()));		
		Mutator<byte[]> mutator = HFactory.createMutator(keyspace, BytesArraySerializer.get());
		mutator.insert(baseIndexKey, columnFamily, HFactory.createColumn(getShardColumnName(), raw, getColumnNameSerializer(), BytesArraySerializer.get()));
	}

	public void splitAndBuildShards() {
		throw new UnsupportedOperationException("Currently you must rely on the prepopulate splits.");
	}

	public void addToIndex(T index, T valueToAdd) throws HectorException {
		byte[] shardKey = getShardKeyForIndex(index);		
		byte[] raw = getRawDataForIndex(shardKey, keyspace, index);

		if (raw == null || raw.length == 0) {
			overwriteIndex(shardKey, keyspace, index, getBytesForData(valueToAdd));
		}
		else if (Arrays.equals(getEmptyValue(), raw)) {
			byte[] subShardKey = getSubShardKeyForIndex(index);
			overwriteIndex(subShardKey, keyspace, valueToAdd, getBytesForData(valueToAdd));
		}
		else {
			byte[] subShardKey = getSubShardKeyForIndex(index);
			T previousValue = getDataForBytes(raw);
			overwriteIndex(subShardKey, keyspace, previousValue, getBytesForData(previousValue));
			overwriteIndex(subShardKey, keyspace, valueToAdd, getBytesForData(valueToAdd));
			overwriteIndex(shardKey, keyspace, index, getEmptyValue());
		}
	}

	public void removeAllValuesAtIndex(T index) {
		byte[] shardKey = getShardKeyForIndex(index);
		byte[] subShardKey = getSubShardKeyForIndex(index);		
		removeIndex(subShardKey, keyspace, null);
		removeIndex(shardKey, keyspace, index);
	}

	public void removeValueAtIndex(T index, T valueToRemove) {
		byte[] shardKey = getShardKeyForIndex(index);		
		byte[] raw = getRawDataForIndex(shardKey, keyspace, index);

		if (raw == null || raw.length == 0) {
			return;
		}
		else if (Arrays.equals(getEmptyValue(), raw)) {
			byte[] subShardKey = getSubShardKeyForIndex(index);
			removeIndex(subShardKey, keyspace, valueToRemove);
		}
		else {
			removeIndex(shardKey, keyspace, index);
		}
	}

	protected void removeIndex(byte[] shardKey, Keyspace ks, T index) {
		Mutator<byte[]> mutator = HFactory.createMutator(ks, BytesArraySerializer.get());
		mutator.delete(shardKey, columnFamily, index, getColumnNameSerializer());
	}

	public void overwriteIndex(T index, T valueToOverwriteWith) {
		byte[] shardKey = getShardKeyForIndex(index);		
		overwriteIndex(shardKey, keyspace, index, getBytesForData(valueToOverwriteWith));
	}

	protected void overwriteIndex(byte[] shardKey, Keyspace ks, T index, byte[] value) {
		Mutator<byte[]> mutator = HFactory.createMutator(ks, BytesArraySerializer.get());
		mutator.insert(shardKey, columnFamily, HFactory.createColumn(index, value, getColumnNameSerializer(), BytesArraySerializer.get()));
	}

	public T getValueForIndex(T index) {
		List<T> list = getValuesForIndex(index, 1);

		if (list.size() > 0)
			return list.get(0);
		else
			return null;
	}

	public List<IndexItem<T>> getValueRangesForIndex(T index, boolean reversed, int limit, int subIndexLimit) {
		byte[] shardKey = getShardKeyForIndex(index);		
		List<IndexItem<T>> list = getValueRangesForIndex(shardKey, keyspace, index, reversed, limit, subIndexLimit);

		if (list.size() < limit) {
			byte[] nextShardKey = getNextNearestShardKeyForIndex(index, reversed);

			if (nextShardKey != null) {
				List<IndexItem<T>> nextList = getValueRangesForIndex(nextShardKey, keyspace, index, reversed, limit, subIndexLimit);

				int countToAdd = limit - list.size();

				if (countToAdd > nextList.size())
					countToAdd = nextList.size();

				for (int i = 0; i < countToAdd; i++) {
					list.add(nextList.get(i));
				}
			}
		}

		return list;
	}

	protected List<IndexItem<T>> getValueRangesForIndex(byte[] shardKey, Keyspace ks, T index, boolean reversed, int limit, int subIndexLimit) {

		SliceQuery<byte[], T, byte[]> query = HFactory.createSliceQuery(ks, BytesArraySerializer.get(), getColumnNameSerializer(), BytesArraySerializer.get());
		query.setColumnFamily(columnFamily);
		query.setKey(shardKey);
		query.setRange(index, null, reversed, limit);
		QueryResult<ColumnSlice<T, byte[]>> result = query.execute();
		ColumnSlice<T, byte[]> cs = result.get();

		List<IndexItem<T>> list = new ArrayList<IndexItem<T>>();

		if (cs == null)
			return list;

		for (HColumn<T, byte[]> column : cs.getColumns()) {
			IndexItem<T> item = new IndexItem<T>();
			item.setIndex(column.getName());
			list.add(item);

			if (column.getValue() == null || column.getValue().length == 0) {
				item.setValues(new ArrayList<T>());
			}
			else if (Arrays.equals(getEmptyValue(), column.getValue())) {
				byte[] subShardKey = getSubShardKeyForIndex(item.getIndex());
				item.setValues(getValuesForIndex(subShardKey, ks, subIndexLimit));
			}
			else {
				item.setValues(new ArrayList<T>());
				item.getValues().add(getDataForBytes(column.getValue()));
			}
		}

		return list;
	}

	public List<T> getValuesForIndex(T index, int limit) {
		byte[] shardKey = getShardKeyForIndex(index);		
		byte[] raw = getRawDataForIndex(shardKey, keyspace, index);

		if (raw == null || raw.length == 0) {
			return new ArrayList<T>();
		}
		else if (Arrays.equals(getEmptyValue(), raw)) {
			byte[] subShardKey = getSubShardKeyForIndex(index);
			return getValuesForIndex(subShardKey, keyspace, limit);
		}
		else {
			ArrayList<T> list = new ArrayList<T>();
			list.add(getDataForBytes(raw));
			return list;
		}
	}

	protected List<T> getValuesForIndex(byte[] shardKey, Keyspace ks, int limit) {
		ArrayList<T> list = new ArrayList<T>();
		SliceQuery<byte[], T, byte[]> query = HFactory.createSliceQuery(ks, BytesArraySerializer.get(), getColumnNameSerializer(), BytesArraySerializer.get());
		query.setColumnFamily(columnFamily);
		query.setKey(shardKey);
		query.setRange(null, null, false, limit);
		QueryResult<ColumnSlice<T, byte[]>> result = query.execute();
		ColumnSlice<T, byte[]> cs = result.get();

		if (cs == null)
			return list;

		for (HColumn<T, byte[]> column : cs.getColumns()) {
			list.add(getDataForBytes(column.getValue()));
		}

		return list;
	}

	protected byte[] getRawDataForIndex(byte[] shardKey, Keyspace ks, T index) {
		ColumnQuery<byte[], T, byte[]> columnQuery = HFactory.createColumnQuery(ks, BytesArraySerializer.get(), getColumnNameSerializer(), BytesArraySerializer.get());
		columnQuery.setColumnFamily(columnFamily).setKey(shardKey).setName(index);
		QueryResult<HColumn<T, byte[]>> result = columnQuery.execute();

		if (result.get() == null)
			return null;
		else
			return result.get().getValue();
	}

	protected byte[] getShardKeyForIndex(T index) {

		if (shards.size() == 0) {
			return emptyIndexKey;
		}
		else {
			T shard = shards.ceilingKey(index);

			if (shard == null)
				return emptyIndexKey;
			else
				return ArrayUtils.addAll(baseIndexKey, ArrayUtils.addAll(Delim, getBytesForData(shard)));
		}
	}

	protected byte[] getNextNearestShardKeyForIndex(T index, boolean reversed) {

		if (shards.size() == 0) {
			return null;
		}
		else {
			T shard = shards.ceilingKey(index);

			if (reversed) {
				if (shard == null)
					shard = shards.lastKey();
				else
					shard = shards.lowerKey(shard);

				if (shard == null)
					return null;
			}
			else {
				if (shard == null)
					return null;
				else
					shard = shards.higherKey(shard);

				if (shard == null)
					return emptyIndexKey;
			}

			return ArrayUtils.addAll(baseIndexKey, ArrayUtils.addAll(Delim, getBytesForData(shard)));
		}
	}

	protected byte[] getSubShardKeyForIndex(T index) {
		return ArrayUtils.addAll(baseIndexKey, ArrayUtils.addAll(SubDelim, getBytesForData(index)));
	}

	public Cluster getCluster() {
		return cluster;
	}

	public Keyspace getKeyspace() {
		return keyspace;
	}

	public String getColumnFamily() {
		return columnFamily;
	}

	public byte[] getBaseIndexKey() {
		return baseIndexKey;
	}

	public abstract ComparatorType getComparatorType();

	public abstract Class<T> getIndexType();

	public abstract byte[] getEmptyValue();

	public abstract Serializer<T> getColumnNameSerializer();

	public abstract byte[] getBytesForData(T data);

	public abstract T getDataForBytes(byte[] data);

	public abstract byte[] getBytesForDataList(List<T> data);

	public abstract List<T> getDataListForBytes(byte[] data);

	public abstract T getShardColumnName();

	public abstract T getStatisticsColumnName();
}
