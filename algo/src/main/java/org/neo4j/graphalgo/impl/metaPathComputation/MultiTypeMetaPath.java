package org.neo4j.graphalgo.impl.metaPathComputation;

import com.google.common.collect.Sets;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class MultiTypeMetaPath {
	private int[][] labeldIds;
	private int[]   typeIds;
	private int     currentPosition = 0;
	private boolean finished        = false;

	public MultiTypeMetaPath(int length) {
		this.labeldIds = new int[length][];
		this.typeIds = new int[length - 1];
	}

	public MultiTypeMetaPath(MultiTypeMetaPath original) {
		int length = original.labeldIds.length;
		this.labeldIds = new int[length][];
		this.typeIds = new int[length - 1];
		System.arraycopy(original.labeldIds, 0, this.labeldIds, 0, length);
		System.arraycopy(original.typeIds, 0, this.typeIds, 0, length - 1);
		this.currentPosition = original.currentPosition;
	}

	public static Set<List<Integer>> composeMetaPaths(MultiTypeMetaPath multiTypeMetaPath) {
		assert multiTypeMetaPath.finished: "MetaPath is not yet finished";;
		List<Set<Integer>> interimList = new ArrayList<>();
		int i = 0;
		while (i < multiTypeMetaPath.currentPosition) {
			interimList.add(IntStream.of(multiTypeMetaPath.labeldIds[i]).boxed().collect(Collectors.toSet()));
			interimList.add(new HashSet<>(Arrays.asList(multiTypeMetaPath.typeIds[i])));
			i++;
		}
		interimList.add(IntStream.of(multiTypeMetaPath.labeldIds[i]).boxed().collect(Collectors.toSet()));

		return Sets.cartesianProduct(interimList);
	}

	public static String getString(Set<List<Integer>> allMetaPaths) {
		List<String> metaPaths = getStrings(allMetaPaths);
		return String.join("\n", metaPaths).replace("[", "").replace("]", "");
	}

	public static List<String> getStrings(Set<List<Integer>> allMetaPaths) {
		return allMetaPaths.stream().map(list -> list.stream().map(Object::toString).collect(Collectors.joining("|"))).collect(Collectors.toList());
	}

	//public Stream<MetaPath> expand() {
	//	// explode
	//	return null;
	//}

	public MultiTypeMetaPath add(int[] labels, int type) {
		assert !finished: "MetaPath is already finished";
		labeldIds[currentPosition] = labels;
		typeIds[currentPosition] = type;
		currentPosition++;

		return this;
	}

	public MultiTypeMetaPath addLastNodeLabels(int[] labels) {
		assert !finished: "MetaPath is already finished";
		labeldIds[currentPosition] = labels;
		finished = true;
		return this;
	}

	public String toString() {
		return getString(composeMetaPaths(this));
	}

	public int length() {
		return currentPosition;
	}

	//public String toString(LabelMapper mapper) {
	//	return "";
	//}
}
