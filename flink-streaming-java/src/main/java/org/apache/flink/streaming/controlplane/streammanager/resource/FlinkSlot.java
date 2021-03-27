package org.apache.flink.streaming.controlplane.streammanager.resource;

import java.util.Collection;

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.jobmaster.slotpool.SlotInfoWithUtilization;
import org.apache.flink.runtime.resourcemanager.slotmanager.TaskManagerSlot;

public class FlinkSlot implements AbstractSlot {
	private SlotID slotId;

	private State state;

	private Resource resource;

	private String location;

	public FlinkSlot(SlotID slotId, State state, Resource resource, String location) {
		this.slotId = slotId;
		this.state = state;
		this.resource = resource;
		this.location = location;
	}

	public static FlinkSlot fromTaskManagerSlot(TaskManagerSlot slot) {
		String taskManagerId = slot.getSlotId().getResourceID().getResourceIdString();
		return new FlinkSlot(slot.getSlotId(), fromSlotState(slot.getState()), fromResourceProfile(slot.getResourceProfile()),taskManagerId);
	}

	public static FlinkSlot fromTaskManagerSlot(TaskManagerSlot slot, Collection<SlotInfoWithUtilization> availableSlots) {
		String taskManagerId = slot.getSlotId().getResourceID().getResourceIdString();
		FlinkSlot newSlot = new FlinkSlot(slot.getSlotId(), fromSlotState(slot.getState()), fromResourceProfile(slot.getResourceProfile()),taskManagerId);

		if (newSlot.getState() == AbstractSlot.State.ALLOCATED &&
			availableSlots.stream()
				.filter(slotInfoWithUtilization -> newSlot.isMatchingSlotInfo(slotInfoWithUtilization))
				.findFirst().isPresent()) {
			newSlot.state = State.FREE;
		}

		return newSlot;
	}

	public static State fromSlotState(TaskManagerSlot.State state) {
		if (state == TaskManagerSlot.State.FREE) {
			return State.FREE;
		} else if (state == TaskManagerSlot.State.ALLOCATED || state == TaskManagerSlot.State.PENDING) {
			return State.ALLOCATED;
		}

		return null;
	}

	public static Resource fromResourceProfile(ResourceProfile resourceProfile) {
		double cpuCores = resourceProfile.getCpuCores().getValue().doubleValue();
		long taskHeapMemory = resourceProfile.getTaskHeapMemory().getBytes();
		long taskOffHeapMemory = resourceProfile.getTaskOffHeapMemory().getBytes();
		long managedMemory = resourceProfile.getManagedMemory().getBytes();
		long networkMemory = resourceProfile.getNetworkMemory().getBytes();
		return Resource.newBuilder()
			.setCpuCores(cpuCores)
			.setTaskHeapMemory(taskHeapMemory)
			.setTaskOffHeapMemory(taskOffHeapMemory)
			.setManagedMemory(managedMemory)
			.setNetworkMemory(networkMemory)
			.build();
	}

	@Override
	public State getState() {
		return state;
	}

	@Override
	public void setPending() {
		state = State.PENDING;
	}

	@Override
	public Resource getResource() {
		return resource;
	}

	@Override
	public String getLocation() {
		return location;
	}

	@Override
	public String getId() {
		return slotId.toString();
	}

	@Override
	public boolean isMatchingRequirement(Resource requirement) {
		return resource.isMatching(requirement);
	}

	public SlotID getSlotId() {
		return slotId;
	}

	public static SlotID toSlotId(String slotId) {
		String[] parts = slotId.split("_");
		try {
			int resourceNumber = Integer.parseInt(parts[1]);
			return new SlotID(new ResourceID(parts[0]), resourceNumber);
		} catch (NumberFormatException e) {
			return SlotID.generateDynamicSlotID(new ResourceID(parts[0]));
		}
	}

	public boolean isMatchingSlotInfo(SlotInfoWithUtilization slotInfoWithUtilization) {
		return slotInfoWithUtilization.getPhysicalSlotNumber() == slotId.getSlotNumber() &&
			slotInfoWithUtilization.getTaskManagerLocation().getResourceID() == slotId.getResourceID();
	}

	@Override
	public String toString() {
		return String.format("slot id: %s\nstate: %s\nresource: %s\n", slotId.toString(), state, resource.toString());
	}
}
