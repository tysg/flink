package org.apache.flink.runtime.controlplane.abstraction;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.Public;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Preconditions;

import java.lang.reflect.Field;
import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Collectors;

/**
 * this class make sure all field is not modifiable for external class
 */
public class OperatorDescriptor {

	private final Integer operatorID;
	private final String name;

	private final Set<OperatorDescriptor> parents = new HashSet<>();
	private final Set<OperatorDescriptor> children = new HashSet<>();

	private final OperatorPayload payload;
	private boolean stateful = false;

	public OperatorDescriptor(Integer operatorID, String name, int parallelism) {
		this.operatorID = operatorID;
		this.name = name;
		this.payload = new OperatorPayload(parallelism);
	}

	public Integer getOperatorID() {
		return operatorID;
	}

	public String getName() {
		return name;
	}

	public Set<OperatorDescriptor> getParents() {
		return Collections.unmodifiableSet(parents);
	}

	public Set<OperatorDescriptor> getChildren() {
		return Collections.unmodifiableSet(children);
	}

	public int getParallelism() {
		return payload.parallelism;
	}

	public Function getUdf() {
		Object functionObject = payload.applicationLogic.attributeMap.get(ApplicationLogic.UDF);
		return (Function) Preconditions.checkNotNull(functionObject);
	}

	public List<List<Integer>> getKeyStateAllocation() {
		return Collections.unmodifiableList(payload.keyStateAllocation);
	}

	public Map<Integer, List<List<Integer>>> getKeyMapping() {
		return Collections.unmodifiableMap(payload.keyMapping);
	}

	public void setParallelism(int parallelism) {
		payload.parallelism = parallelism;
	}

	public void setUdf(Function udf) {
		setControlAttribute(ApplicationLogic.UDF, udf);
	}

	public final void setControlAttribute(String name, Object obj) {
		payload.applicationLogic.attributeMap.put(name, obj);
	}

	public Map<String, Object> getControlAttributeMap() {
		return Collections.unmodifiableMap(payload.applicationLogic.attributeMap);
	}

	@Internal
	void setKeyStateAllocation(List<List<Integer>> keyStateAllocation) {
		List<List<Integer>> unmodifiableKeys = Collections.unmodifiableList(
			keyStateAllocation.stream()
				.map(Collections::unmodifiableList)
				.collect(Collectors.toList())
		);
		payload.keyStateAllocation.clear();
		payload.keyStateAllocation.addAll(unmodifiableKeys);
		for (OperatorDescriptor parent : parents) {
			parent.payload.keyMapping.put(operatorID, unmodifiableKeys);
		}
		// stateless operator should not be allocated  key set
		stateful = !payload.keyStateAllocation.isEmpty();
	}

	@Internal
	void setKeyMapping(Map<Integer, List<List<Integer>>> keyMapping) {
		Map<Integer, List<List<Integer>>> unmodifiable = convertToUnmodifiable(keyMapping);
		payload.keyMapping.putAll(unmodifiable);
		for (OperatorDescriptor child : children) {
			if (child.stateful) {
				// todo two inputs?
				child.payload.keyStateAllocation.clear();
				child.payload.keyStateAllocation.addAll(unmodifiable.get(child.operatorID));
			}
		}
	}

	@Internal
	ApplicationLogic getApplicationLogic(){
		return payload.applicationLogic;
	}

	private Map<Integer, List<List<Integer>>> convertToUnmodifiable(Map<Integer, List<List<Integer>>> keyStateAllocation) {
		Map<Integer, List<List<Integer>>> unmodifiable = new HashMap<>();
		for (Integer inOpID : keyStateAllocation.keySet()) {
			List<List<Integer>> unmodifiableKeys = Collections.unmodifiableList(
				keyStateAllocation.get(inOpID)
					.stream()
					.map(Collections::unmodifiableList)
					.collect(Collectors.toList())
			);
			unmodifiable.put(inOpID, unmodifiableKeys);
		}
		return unmodifiable;
	}

	/**
	 * we assume that the key operator only have one upstream opeartor
	 *
	 * @param keyStateAllocation
	 */
	@PublicEvolving
	public void setKeySet(List<List<Integer>> keyStateAllocation) {
		if (parents.size() != 1 || !stateful) {
			System.out.println("not support now");
			return;
		}
		try {
			List<List<Integer>> unmodifiableKeys = Collections.unmodifiableList(
				keyStateAllocation.stream()
					.map(Collections::unmodifiableList)
					.collect(Collectors.toList())
			);
			payload.keyStateAllocation.clear();
			payload.keyStateAllocation.addAll(unmodifiableKeys);
			// sync with parent's key mapping
			for(OperatorDescriptor parent: parents) {
				parent.payload.keyMapping.put(operatorID, unmodifiableKeys);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Only use to update key mapping for case that target operator is stateless.
	 * If the target is stateful, the key mapping should be changed by
	 * setKeyStateAllocation(List<List<Integer>> keyStateAllocation);
	 *
	 * @param targetOperatorID
	 * @param keyMapping
	 */
	@PublicEvolving
	public void setKeyMappingTo(int targetOperatorID, List<List<Integer>> keyMapping) {
		try {
			OperatorDescriptor child = checkOperatorIDExistInSet(targetOperatorID, children);

			List<List<Integer>> unmodifiableKeys = Collections.unmodifiableList(
				keyMapping.stream()
					.map(Collections::unmodifiableList)
					.collect(Collectors.toList())
			);
			payload.keyMapping.put(targetOperatorID, unmodifiableKeys);
			if (child.stateful) {
				child.payload.keyStateAllocation.clear();
				child.payload.keyStateAllocation.addAll(unmodifiableKeys);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static OperatorDescriptor checkOperatorIDExistInSet(int opID, Set<OperatorDescriptor> set) throws Exception {
		for (OperatorDescriptor descriptor : set) {
			if (opID == descriptor.getOperatorID()) {
				return descriptor;
			}
		}
		throw new Exception("do not have this id in set");
	}


	/**
	 * @param childEdges       the list of pair of parent id and child id to represent the relationship between operator
	 * @param allOperatorsById
	 */
	void addChildren(List<Tuple2<Integer, Integer>> childEdges, Map<Integer, OperatorDescriptor> allOperatorsById) {
		// f0 is parent operator id, f1 is child operator id
		for (Tuple2<Integer, Integer> edge : childEdges) {
			Preconditions.checkArgument(allOperatorsById.get(edge.f0) == this, "edge source is wrong matched");
			OperatorDescriptor descriptor = allOperatorsById.get(edge.f1);
			// I think I am your father
			children.add(descriptor);
			descriptor.parents.add(this);
		}
	}

	/**
	 * @param parentEdges      the list of pair of parent id and child id to represent the relationship between operator
	 * @param allOperatorsById
	 */
	void addParent(List<Tuple2<Integer, Integer>> parentEdges, Map<Integer, OperatorDescriptor> allOperatorsById) {
		// f0 is parent operator id, f1 is child operator id
		for (Tuple2<Integer, Integer> edge : parentEdges) {
			Preconditions.checkArgument(allOperatorsById.get(edge.f1) == this, "edge source is wrong matched");
			OperatorDescriptor descriptor = allOperatorsById.get(edge.f0);
			// I think I am your father
			parents.add(descriptor);
			descriptor.children.add(this);
		}
	}

	@Override
	public String toString() {
		return "OperatorDescriptor{name='" + name + "'', parallelism=" + payload.parallelism +
			", parents:" + parents.size() + ", children:" + children.size() + '}';
	}

	private static class OperatorPayload {
		int parallelism;
		final ApplicationLogic applicationLogic;
		/* for stateful one input stream task, the key state allocation item is always one */
		final List<List<Integer>> keyStateAllocation;
		final Map<Integer, List<List<Integer>>> keyMapping;

		OperatorPayload(int parallelism) {
			this.parallelism = parallelism;
			keyStateAllocation = new ArrayList<>(parallelism);
			keyMapping = new HashMap<>();
			applicationLogic = new ApplicationLogic();
		}
	}

	void setAttributeField(Object object, List<Field> fieldList) throws IllegalAccessException {
		payload.applicationLogic.operator = object;
		for(Field field: fieldList) {
			ControlAttribute attribute = field.getAnnotation(ControlAttribute.class);
			boolean accessible = field.isAccessible();
			// temporary set true
			field.setAccessible(true);
			this.setControlAttribute(attribute.name(), field.get(object));
			payload.applicationLogic.fields.put(attribute.name(), field);
			field.setAccessible(accessible);
		}
	}

	public static class ApplicationLogic{

		public static final String UDF = "UDF";
		public static final String OPERATOR_TYPE = "OPERATOR_TYPE";

		private final Map<String, Object> attributeMap = new HashMap<>();
		private final Map<String, Field> fields = new HashMap<>();

		@VisibleForTesting
		private Object operator;

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;
			ApplicationLogic that = (ApplicationLogic) o;
			return attributeMap.equals(that.attributeMap);
		}

		@Override
		public int hashCode() {
			return Objects.hash(attributeMap);
		}

		public ApplicationLogic copyTo(ApplicationLogic that){
			that.attributeMap.clear();
			that.attributeMap.putAll(attributeMap);
			return that;
		}

		public Map<String, Object> getControlAttributeMap() {
			return Collections.unmodifiableMap(attributeMap);
		}

		public Map<String, Field> getControlAttributeFieldMap() {
			return Collections.unmodifiableMap(fields);
		}
	}

}
