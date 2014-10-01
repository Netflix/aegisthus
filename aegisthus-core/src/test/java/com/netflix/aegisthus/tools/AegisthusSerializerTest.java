package com.netflix.aegisthus.tools;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class AegisthusSerializerTest {

	@Test(enabled = false)
	public void agent() throws IOException {
		BufferedReader br = new BufferedReader(new InputStreamReader(AegisthusSerializerTest.class.getResourceAsStream("/agent.json")));
		String line = null;
		StringBuilder file = new StringBuilder();
		while ((line = br.readLine()) != null) {
			file.append(line);
		}

		new AegisthusSerializer().deserialize(file.toString());
	}

	@Test(dataProvider = "json")
	public void deserializeExpire(String value) throws IOException {
		Map<String, Object> map = new AegisthusSerializer().deserialize(value);

		Assert.assertEquals(AegisthusSerializer.serialize(map), value);
	}

	@DataProvider()
	public Object[][] json() {
		return new Object[][] { { "{\"556e6b6e6f776e3a3738383838\": {\"deletedAt\": -9223372036854775808, \"columns\": [[\"ticketId\",\"d115046000bd11e1b27112313925158b\",1319735105450,\"e\",604800,1320339905]]}}" },
								{ "{\"556e6b6e6f776e3a3738383838\": {\"deletedAt\": -9223372036854775808, \"columns\": [[\"\\\\N\",\"d115046000bd11e1b27112313925158b\",1319735105450,\"e\",604800,1320339905]]}}" },
								{ "{\"556e6b6e6f776e3a3738383838\": {\"deletedAt\": -9223372036854775808, \"columns\": [[\"ticketId\",\"d115046000bd11e1b27112313925158b\",1319735105450,\"c\",604800]]}}" } };
	}

	private String s(Object obj) {
		return obj.toString();
	}

	@SuppressWarnings("rawtypes")
	@Test
	public void serialize() throws IOException {
		String value = "{\"uid\": {\"deletedAt\": 10, \"columns\": [[\"cell\",\"00000002\",1312400480243000], [\"enabled\",\"59\",1312400475129004], [\"newcolumn\",\"59\",1312400533649004]]}}";
		Map<String, Object> map = new AegisthusSerializer().deserialize(value);

		Assert.assertEquals(s(map.get(AegisthusSerializer.KEY)), "uid");
		Assert.assertEquals(map.get(AegisthusSerializer.DELETEDAT), 10L);
		Assert.assertEquals(s(((List) map.get("enabled")).get(1)), "59");

		Assert.assertEquals(AegisthusSerializer.serialize(map), value);
	}

	@Test(dataProvider = "values")
	public void serializeColumns(List<Object> values, String exp) {
		Map<String, Object> map = Maps.newHashMap();
		map.put(values.get(0).toString(), values);

		StringBuilder sb = new StringBuilder();

		AegisthusSerializer.serializeColumns(sb, map);
		Assert.assertEquals(sb.toString(), exp);
	}

	@DataProvider()
	public Object[][] values() {
		Object[][] ret = new Object[2][2];

		List<Object> values = Lists.newArrayList();
		values.add("\\N");
		values.add("");
		values.add(1);

		ret[0][0] = values;
		ret[0][1] = "[\"\\\\N\",\"\",1]";

		values = Lists.newArrayList();
		values.add("\\\\N");
		values.add("");
		values.add(1);

		ret[1][0] = values;
		ret[1][1] = "[\"\\\\\\\\N\",\"\",1]";

		return ret;
	}
}
