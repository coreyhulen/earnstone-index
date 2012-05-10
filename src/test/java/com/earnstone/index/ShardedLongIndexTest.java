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

import java.util.Arrays;
import java.util.List;

import junit.framework.Assert;
import me.prettyprint.cassandra.model.BasicColumnFamilyDefinition;
import me.prettyprint.cassandra.service.ThriftCfDef;
import me.prettyprint.cassandra.testutils.EmbeddedServerHelper;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class ShardedLongIndexTest {

	private static EmbeddedServerHelper embedded;
	private static Cluster cluster;
	private static Keyspace keyspace;	
	private static String COLFAM = "ShardedLongIndexTestCF";

	@BeforeClass
	public static void setup() throws Exception {
		embedded = new EmbeddedServerHelper();
		embedded.setup();

		cluster = HFactory.getOrCreateCluster("MyCluster", "localhost:9170");		

		BasicColumnFamilyDefinition columnFamilyDefinition1 = new BasicColumnFamilyDefinition();
		columnFamilyDefinition1.setKeyspaceName("ShardedLongIndexTestKS");
		columnFamilyDefinition1.setName(COLFAM);
		columnFamilyDefinition1.setComparatorType(ComparatorType.LONGTYPE);
		ColumnFamilyDefinition cfDef1 = new ThriftCfDef(columnFamilyDefinition1);

		KeyspaceDefinition keyspaceDefinition = HFactory.createKeyspaceDefinition("ShardedLongIndexTestKS", "org.apache.cassandra.locator.SimpleStrategy", 1, Arrays.asList(cfDef1));
		cluster.addKeyspace(keyspaceDefinition);
		
		keyspace = HFactory.createKeyspace("ShardedLongIndexTestKS", cluster);
	}

	@AfterClass
	public static void teardown() throws Exception {
		embedded.teardown();
	}

	@Test
	public void verifyConstructorArguments() {
		BasicColumnFamilyDefinition columnFamilyDefinition1 = new BasicColumnFamilyDefinition();
		columnFamilyDefinition1.setKeyspaceName("verifyConstructorArgumentsKeyspace");
		columnFamilyDefinition1.setName("verifyConstructorArgumentsCF");
		columnFamilyDefinition1.setComparatorType(ComparatorType.LONGTYPE);
		ColumnFamilyDefinition cfDef1 = new ThriftCfDef(columnFamilyDefinition1);

		BasicColumnFamilyDefinition columnFamilyDefinition2 = new BasicColumnFamilyDefinition();
		columnFamilyDefinition2.setKeyspaceName("verifyConstructorArgumentsKeyspace");
		columnFamilyDefinition2.setName("verifyConstructorArgumentsCFString");
		columnFamilyDefinition2.setComparatorType(ComparatorType.UTF8TYPE);
		ColumnFamilyDefinition cfDef2 = new ThriftCfDef(columnFamilyDefinition2);

		KeyspaceDefinition keyspaceDefinition = HFactory
				.createKeyspaceDefinition("verifyConstructorArgumentsKeyspace", "org.apache.cassandra.locator.SimpleStrategy", 1, Arrays.asList(cfDef1, cfDef2));
		cluster.addKeyspace(keyspaceDefinition);
		
		Keyspace testKeyspace = HFactory.createKeyspace("verifyConstructorArgumentsKeyspace", cluster);		

		try {
			new ShardedLongIndex(cluster, testKeyspace, "testMissingColumnFamily", "");
			Assert.fail();
		}
		catch (IllegalArgumentException e) {
		}

		try {
			new ShardedLongIndex(cluster, testKeyspace, "verifyConstructorArgumentsCF", "");
			Assert.fail();
		}
		catch (IllegalArgumentException e) {
		}

		try {
			new ShardedLongIndex(cluster, testKeyspace, "verifyConstructorArgumentsCF", "verifyConstructorArguments");
		}
		catch (IllegalArgumentException e) {
			Assert.fail();
		}

		try {
			new ShardedLongIndex(cluster, testKeyspace, "verifyConstructorArgumentsCFString", "verifyConstructorArguments");
			Assert.fail();
		}
		catch (IllegalArgumentException e) {
		}
	}

	@Test
	public void basicAddToIndex() {
		ShardedLongIndex index = new ShardedLongIndex(cluster, keyspace, COLFAM, "basicAddToIndex");

		for (long i = 0; i < 100; i++) {
			index.addToIndex(i, i);
		}

		for (long i = 0; i < 100; i++) {
			List<Long> values = index.getValuesForIndex(i, 32);
			Assert.assertEquals(1, values.size());
			Assert.assertEquals(i, values.get(0).longValue());
		}
	}

	@Test
	public void addValuesForTheSameIndex() {
		ShardedLongIndex index = new ShardedLongIndex(cluster, keyspace, COLFAM, "addValuesForTheSameIndex");

		for (long i = 0; i < 100; i++) {
			index.addToIndex(i, i);
			index.addToIndex(i, i + 1);
			index.addToIndex(i, i + 2);
		}

		for (long i = 0; i < 100; i++) {
			List<Long> values = index.getValuesForIndex(i, 32);
			Assert.assertEquals(3, values.size());
			Assert.assertEquals(i, values.get(0).longValue());
			Assert.assertEquals(i + 1, values.get(1).longValue());
			Assert.assertEquals(i + 2, values.get(2).longValue());
		}
	}

	@Test
	public void valueRangesForIndex() {
		ShardedLongIndex index = new ShardedLongIndex(cluster, keyspace, COLFAM, "valueRangesForIndex");

		for (long i = 0; i < 100; i++) {
			index.addToIndex(i, i);

			if (i % 2 == 0)
				index.addToIndex(i, i + 1);
		}

		List<IndexItem<Long>> list = index.getValueRangesForIndex(50l, false, 10, 10);
		Assert.assertEquals(10, list.size());

		for (long i = 50; i < 60; i++) {

			IndexItem<Long> item = list.get((int) i - 50);

			if (i % 2 == 0) {
				Assert.assertEquals(2, item.getValues().size());
				Assert.assertEquals(i, item.getValues().get(0).longValue());
				Assert.assertEquals(i + 1, item.getValues().get(1).longValue());
			}
			else {
				Assert.assertEquals(1, item.getValues().size());
				Assert.assertEquals(i, item.getValues().get(0).longValue());
			}
		}

		list = index.getValueRangesForIndex(50l, true, 10, 10);

		for (long i = 50; i > 40; i--) {

			IndexItem<Long> item = list.get(50 - (int)i);

			if (i % 2 == 0) {
				Assert.assertEquals(2, item.getValues().size());
				Assert.assertEquals(i, item.getValues().get(0).longValue());
				Assert.assertEquals(i + 1, item.getValues().get(1).longValue());
			}
			else {
				Assert.assertEquals(1, item.getValues().size());
				Assert.assertEquals(i, item.getValues().get(0).longValue());
			}
		}
	}
	
	@Test
	public void valueSkipRangesForIndex() {
		ShardedLongIndex index = new ShardedLongIndex(cluster, keyspace, COLFAM, "valueSkipRangesForIndex");

		for (long i = 0; i < 100; i++) {			
			index.addToIndex(i, i);
			i++;
		}

		List<IndexItem<Long>> list = index.getValueRangesForIndex(51l, false, 10, 10);
		Assert.assertEquals(10, list.size());

		long expectedValue = 52;
		for (long i = 0; i < 10; i++) {

			IndexItem<Long> item = list.get((int) i);

			Assert.assertEquals(1, item.getValues().size());
			Assert.assertEquals(expectedValue, item.getValues().get(0).longValue());	
			expectedValue += 2;
		}
	}

	@Test
	public void removeValuesFromIndex() {
		ShardedLongIndex index = new ShardedLongIndex(cluster, keyspace, COLFAM, "removeValuesFromIndex");

		long i = 0;

		index.addToIndex(i, i);
		index.removeValueAtIndex(i, i);
		List<Long> values = index.getValuesForIndex(i, 32);
		Assert.assertEquals(0, values.size());

		i++;
		index.addToIndex(i, i);
		index.addToIndex(i, i + 1);
		index.addToIndex(i, i + 2);

		index.removeValueAtIndex(i, i + 2);
		values = index.getValuesForIndex(i, 32);
		Assert.assertEquals(2, values.size());
		Assert.assertEquals(i, values.get(0).longValue());
		Assert.assertEquals(i + 1, values.get(1).longValue());

		index.removeValueAtIndex(i, i + 1);
		values = index.getValuesForIndex(i, 32);
		Assert.assertEquals(1, values.size());
		Assert.assertEquals(i, values.get(0).longValue());

		index.removeValueAtIndex(i, i);
		values = index.getValuesForIndex(i, 32);
		Assert.assertEquals(0, values.size());

		i++;
		index.addToIndex(i, i);
		index.addToIndex(i, i + 1);
		index.addToIndex(i, i + 2);
		index.removeAllValuesAtIndex(i);
		values = index.getValuesForIndex(i, 32);
		Assert.assertEquals(0, values.size());
	}
	
	@Test
	public void prepopulateShardIndex() {
		ShardedLongIndex index = new ShardedLongIndex(cluster, keyspace, COLFAM, "prepopulateShardIndex");		
		index.initializeShardBoundries(Arrays.asList(20l, 40l, 60l, 80l, 100l));
		
		for (long i = 0; i < 100; i++) {
			index.addToIndex(i, i);
		}

		for (long i = 0; i < 100; i++) {
			List<Long> values = index.getValuesForIndex(i, 32);
			Assert.assertEquals(1, values.size());
			Assert.assertEquals(i, values.get(0).longValue());
		}
	}
	
	@Test
	public void shardedRangeValues() {
		ShardedLongIndex index = new ShardedLongIndex(cluster, keyspace, COLFAM, "shardedRangeValues");
		index.initializeShardBoundries(Arrays.asList(20l, 40l, 60l, 80l, 100l));
		index.reloadShardsCache();

		for (long i = 0; i < 100; i++) {
			index.addToIndex(i, i);
			index.addToIndex(i, i + 1);
			index.addToIndex(i, i + 2);
		}
		
		index.addToIndex(1000l, 1000l);

		for (long i = 0; i < 100; i++) {
			List<Long> values = index.getValuesForIndex(i, 32);
			Assert.assertEquals(3, values.size());
			Assert.assertEquals(i, values.get(0).longValue());
			Assert.assertEquals(i + 1, values.get(1).longValue());
			Assert.assertEquals(i + 2, values.get(2).longValue());
		}
		
		List<Long> values1 = index.getValuesForIndex(1000l, 32);
		Assert.assertEquals(1, values1.size());
		Assert.assertEquals(1000, values1.get(0).longValue());
		
		List<Long> values2 = index.getValuesForIndex(1001l, 32);
		Assert.assertEquals(0, values2.size());
		
		List<IndexItem<Long>> list1 = index.getValueRangesForIndex(59l, false, 20, 20);
		Assert.assertEquals(20, list1.size());
		Assert.assertEquals(59l, (long)list1.get(0).getIndex());
		Assert.assertEquals(60l, (long)list1.get(1).getIndex());
		Assert.assertEquals(78l, (long)list1.get(19).getIndex());
		
		List<IndexItem<Long>> list2 = index.getValueRangesForIndex(61l, false, 20, 20);
		Assert.assertEquals(20, list2.size());
		Assert.assertEquals(61l, (long)list2.get(0).getIndex());
		Assert.assertEquals(80l, (long)list2.get(19).getIndex());		
		
		List<IndexItem<Long>> list3 = index.getValueRangesForIndex(101l, false, 20, 20);
		Assert.assertEquals(1, list3.size());
		Assert.assertEquals(1000l, (long)list3.get(0).getIndex());		
		
		List<IndexItem<Long>> list4 = index.getValueRangesForIndex(2000l, false, 20, 20);
		Assert.assertEquals(0, list4.size());
		
		List<IndexItem<Long>> list5 = index.getValueRangesForIndex(99l, false, 20, 20);
		Assert.assertEquals(2, list5.size());
		Assert.assertEquals(99l, (long)list5.get(0).getIndex());
		Assert.assertEquals(1000l, (long)list5.get(1).getIndex());		
		
		List<IndexItem<Long>> list6 = index.getValueRangesForIndex(0l, false, 20, 20);
		Assert.assertEquals(20, list6.size());
		Assert.assertEquals(0l, (long)list6.get(0).getIndex());		
		Assert.assertEquals(19l, (long)list6.get(19).getIndex());
		
		List<IndexItem<Long>> list7 = index.getValueRangesForIndex(-1000l, false, 20, 20);
		Assert.assertEquals(20, list7.size());
		Assert.assertEquals(0l, (long)list7.get(0).getIndex());		
		Assert.assertEquals(19l, (long)list7.get(19).getIndex());
	}
	
	@Test
	public void shardedRangeValuesInReverse() {
		ShardedLongIndex index = new ShardedLongIndex(cluster, keyspace, COLFAM, "shardedRangeValuesInReverse");
		index.initializeShardBoundries(Arrays.asList(20l, 40l, 60l, 80l, 100l));
		index.reloadShardsCache();

		for (long i = 0; i < 100; i++) {
			index.addToIndex(i, i);
			index.addToIndex(i, i + 1);
			index.addToIndex(i, i + 2);
		}
		
		index.addToIndex(1000l, 1000l);

		for (long i = 0; i < 100; i++) {
			List<Long> values = index.getValuesForIndex(i, 32);
			Assert.assertEquals(3, values.size());
			Assert.assertEquals(i, values.get(0).longValue());
			Assert.assertEquals(i + 1, values.get(1).longValue());
			Assert.assertEquals(i + 2, values.get(2).longValue());
		}
		
		List<Long> values1 = index.getValuesForIndex(1000l, 32);
		Assert.assertEquals(1, values1.size());
		Assert.assertEquals(1000, values1.get(0).longValue());
		
		List<Long> values2 = index.getValuesForIndex(1001l, 32);
		Assert.assertEquals(0, values2.size());
		
		List<IndexItem<Long>> list1 = index.getValueRangesForIndex(59l, true, 20, 20);
		Assert.assertEquals(20, list1.size());
		Assert.assertEquals(59l, (long)list1.get(0).getIndex());
		Assert.assertEquals(58l, (long)list1.get(1).getIndex());
		Assert.assertEquals(40l, (long)list1.get(19).getIndex());
		
		List<IndexItem<Long>> list2 = index.getValueRangesForIndex(61l, true, 20, 20);
		Assert.assertEquals(20, list2.size());
		Assert.assertEquals(61l, (long)list2.get(0).getIndex());
		Assert.assertEquals(42l, (long)list2.get(19).getIndex());		
		
		List<IndexItem<Long>> list3 = index.getValueRangesForIndex(21l, true, 20, 20);
		Assert.assertEquals(20, list3.size());
		Assert.assertEquals(21l, (long)list3.get(0).getIndex());
		Assert.assertEquals(2l, (long)list3.get(19).getIndex());
		
		List<IndexItem<Long>> list4 = index.getValueRangesForIndex(14l, true, 20, 20);
		Assert.assertEquals(15, list4.size());
		Assert.assertEquals(14l, (long)list4.get(0).getIndex());
		Assert.assertEquals(0l, (long)list4.get(14).getIndex());
		
		List<IndexItem<Long>> list5 = index.getValueRangesForIndex(-100l, true, 20, 20);
		Assert.assertEquals(0, list5.size());		
		
		List<IndexItem<Long>> list6 = index.getValueRangesForIndex(2000l, true, 20, 20);
		Assert.assertEquals(20, list6.size());
		Assert.assertEquals(1000l, (long)list6.get(0).getIndex());
		Assert.assertEquals(81l, (long)list6.get(19).getIndex());
		
		List<IndexItem<Long>> list7 = index.getValueRangesForIndex(999l, true, 20, 20);
		Assert.assertEquals(19, list7.size());
		Assert.assertEquals(99l, (long)list7.get(0).getIndex());
		Assert.assertEquals(81l, (long)list7.get(18).getIndex());
		
		List<IndexItem<Long>> list8 = index.getValueRangesForIndex(99l, true, 20, 20);
		Assert.assertEquals(20, list8.size());
		Assert.assertEquals(99l, (long)list8.get(0).getIndex());
		Assert.assertEquals(80l, (long)list8.get(19).getIndex());
	}
}
