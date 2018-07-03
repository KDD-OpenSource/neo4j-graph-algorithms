package org.neo4j.graphalgo.impl.metaPathComputationTests;

import org.junit.Test;
import org.neo4j.graphalgo.impl.metaPathComputation.MultiTypeMetaPath;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class ComputeMetaPathFromNodeIdThreadTest {

	@Test public void testComputeMetaPaths() throws Exception {

		MultiTypeMetaPath multiTypeMetaPath = new MultiTypeMetaPath(2);
		multiTypeMetaPath.add(new int[] { 1, 2 }, 0);
		multiTypeMetaPath.addLastNodeLabels(new int[] { 4, 5 });

		Set<List<Integer>> result = MultiTypeMetaPath.composeMetaPaths(multiTypeMetaPath);
		System.out.println(result);

		ArrayList<List<Integer>> expected_result = new ArrayList<>();
		expected_result.add(Arrays.asList(1, 0, 4));
		expected_result.add(Arrays.asList(1, 0, 5));
		expected_result.add(Arrays.asList(2, 0, 4));
		expected_result.add(Arrays.asList(2, 0, 5));

		assertThat(expected_result, containsInAnyOrder(result.toArray()));
	}

	@Test public void testGetStringsMetaPaths() {

		MultiTypeMetaPath multiTypeMetaPath = new MultiTypeMetaPath(2);
		multiTypeMetaPath.add(new int[] { 1, 2 }, 0);
		multiTypeMetaPath.addLastNodeLabels(new int[] { 4 });

		List<String> strings = MultiTypeMetaPath.getStrings(MultiTypeMetaPath.composeMetaPaths(multiTypeMetaPath));
		List<String> expected = new ArrayList<>();
		expected.add("1|0|4");
		expected.add("2|0|4");
		assertThat(expected, containsInAnyOrder(strings.toArray()));
	}

	@Test public void testGetStringMetaPaths() {

		MultiTypeMetaPath multiTypeMetaPath = new MultiTypeMetaPath(2);
		multiTypeMetaPath.add(new int[] { 1, 2 }, 0);
		multiTypeMetaPath.addLastNodeLabels(new int[] { 4 });

		String string = MultiTypeMetaPath.getString(MultiTypeMetaPath.composeMetaPaths(multiTypeMetaPath));
		assertTrue("1|0|4\n2|0|4".equals(string) || "2|0|4\n1|0|4".equals(string));
	}

	@Test public void testToStringMetaPaths() {

		MultiTypeMetaPath multiTypeMetaPath = new MultiTypeMetaPath(2);
		multiTypeMetaPath.add(new int[] { 1, 2 }, 0);
		multiTypeMetaPath.addLastNodeLabels(new int[] { 4 });

		String string = multiTypeMetaPath.toString();
		assertTrue("1|0|4\n2|0|4".equals(string) || "2|0|4\n1|0|4".equals(string));
	}
}