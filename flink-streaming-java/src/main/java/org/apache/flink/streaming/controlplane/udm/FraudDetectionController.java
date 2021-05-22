package org.apache.flink.streaming.controlplane.udm;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.controlplane.abstraction.resource.AbstractSlot;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.controlplane.streammanager.abstraction.ExecutionPlanWithLock;
import org.apache.flink.streaming.controlplane.streammanager.abstraction.ReconfigurationExecutor;
import org.apache.flink.util.FileUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class FraudDetectionController extends AbstractController {

	private final String USER_AGENT = "Mozilla/5.0";
	private int requestTime = 0;
	private String baseDir;

	public FraudDetectionController(ReconfigurationExecutor reconfigurationExecutor) {
		super(reconfigurationExecutor);
		baseDir = reconfigurationExecutor.getExperimentConfig().getString("trisk.exp.dir", "/data/flink");
	}

	@Override
	public void startControllers() {
		super.controlActionRunner.start();
	}

	@Override
	public void stopControllers() {
		super.controlActionRunner.interrupt();
	}


	private Map<String, Object> parseJsonString(String json) throws JsonProcessingException {
		ObjectMapper objectMapper = new ObjectMapper();
		return objectMapper.readValue(json, new TypeReference<Map<String, Object>>() {
		});
	}

	@Override
	protected void defineControlAction() throws Exception {
		super.defineControlAction();
		ExecutionPlanWithLock planWithLock;

//		planWithLock = getReconfigurationExecutor().getExecutionPlanCopy();
//		updatePreprocessingScaleParameter(planWithLock);
//		updateDecisionTreeParameter(planWithLock);
		Thread.sleep(10 * 1000);
//
//		Thread.sleep(2 * 60 * 1000);
		requestTime += 2 * 60;
		planWithLock = getReconfigurationExecutor().getExecutionPlanCopy();
		updatePreprocessingScaleParameter(planWithLock);
		Thread.sleep(10 * 1000);
		updateDecisionTreeParameter(planWithLock);
//
//		Thread.sleep(105 * 1000);
//		requestTime += 105;
//		planWithLock = getReconfigurationExecutor().getExecutionPlanCopy();
//		updatePreprocessingScaleParameter(planWithLock);
//		updateDecisionTreeParameter(planWithLock);
//
		Thread.sleep(10 * 1000);
		smartPlacement(findOperatorByName("dtree"));
		Thread.sleep(10 * 1000);
		smartPlacement(findOperatorByName("preprocess"));

//		Thread.sleep(110 * 1000);
//		requestTime += 110;
//		planWithLock = getReconfigurationExecutor().getExecutionPlanCopy();
//		updatePreprocessingScaleParameter(planWithLock);
//		updateDecisionTreeParameter(planWithLock);
	}

	private void smartPlacement(int preprocessOpID) throws Exception {
		ExecutionPlanWithLock planWithLock = getReconfigurationExecutor().getExecutionPlanCopy();

		Map<Integer, Tuple2<Integer, String>> deployment = new HashMap<>();

		Map<String, List<AbstractSlot>> resourceMap = planWithLock.getResourceDistribution();
		int p = planWithLock.getParallelism(preprocessOpID);
		List<AbstractSlot> allocatedSlots = allocateResourceUniformly(resourceMap, p);
		assert allocatedSlots != null;
		for (int i = 0; i < p; i++) {
			deployment.put(i, Tuple2.of(i + p, allocatedSlots.get(i).getId()));
		}
		placement(preprocessOpID, deployment);
	}

	private List<AbstractSlot> allocateResourceUniformly(Map<String, List<AbstractSlot>> resourceMap, int numTasks) throws Exception {
		List<AbstractSlot> res = new ArrayList<>(numTasks);
		int numNodes = resourceMap.size();
		// todo, please ensure numTask could be divided by numNodes for experiment
		if (numTasks % numNodes != 0) {
			throw new Exception("please ensure numTask could be divided by numNodes for experiment");
		}
		int numTasksInOneNode = numTasks / numNodes;
		// todo, now I will remove the allocated slot from resource map instead of change its state, thus
		//  placement should only happened once since the resource map is not consistent with real condition due to slot state change
		for (String nodeID : resourceMap.keySet()) {
			List<AbstractSlot> slotList = resourceMap.get(nodeID);
			int allocated = 0;
			for (AbstractSlot slot : slotList) {
				if (allocated >= numTasksInOneNode) {
					break;
				}
				if (slot.getState() == AbstractSlot.State.FREE) {
					System.out.println("++++++ choosing slot: " + slot);
					res.add(slot);
					allocated++;
				}
			}
		}
		if (res.size() == numTasks) {
			// remove them from source map
			for (AbstractSlot slot : res) {
				resourceMap.get(slot.getLocation()).remove(slot);
			}
			return res;
		} else {
			return null;
		}
	}


	private void updatePreprocessingScaleParameter(ExecutionPlanWithLock planWithLock) throws Exception {
		String scalePara = sendGet("http://127.0.0.1:5000/scale/", requestTime);
		Map<String, Object> res = parseJsonString(scalePara);
		ArrayList<Double> center = (ArrayList<Double>) res.get("center");
		ArrayList<Double> scale = (ArrayList<Double>) res.get("scale");

		int preprocessOpID = findOperatorByName("preprocess");
		// preprocessing
		float[] centerArray = doubleListToArray(center);
		float[] scaleArray = doubleListToArray(scale);
		Function preprocessFunc = planWithLock.getOperatorByID(preprocessOpID).getUdf();
		Function newPreprocessFunc = preprocessFunc.getClass()
			.getConstructor(float[].class, float[].class).newInstance(centerArray, scaleArray); // reflection related
		changeOfLogic(preprocessOpID, newPreprocessFunc);
	}

	private float[] doubleListToArray(ArrayList<Double> doubles) {
		float[] floats = new float[doubles.size()];
		for (int i = 0; i < doubles.size(); i++) {
			floats[i] = doubles.get(i).floatValue();
		}
		return floats;
	}

	private void updateDecisionTreeParameter(ExecutionPlanWithLock planWithLock) throws Exception {
		String treePara = sendGet("http://127.0.0.1:5000/dtree/", requestTime);
		Map<String, Object> res = parseJsonString(treePara);
		ArrayList<Integer> feature = (ArrayList<Integer>) res.get("feature");
		ArrayList<Integer> leftChildren = (ArrayList<Integer>) res.get("children_left");
		ArrayList<Integer> rightChildren = (ArrayList<Integer>) res.get("children_right");
		ArrayList<Double> threshold = (ArrayList<Double>) res.get("threshold");
		ArrayList<ArrayList<?>> value = (ArrayList<ArrayList<?>>) res.get("value");
		int[] featureArr = feature.stream().mapToInt(i -> i).toArray();
		float[] thresholdArr = doubleListToArray(threshold);
		int[] leftArr = leftChildren.stream().mapToInt(i -> i).toArray();
		int[] rightArr = rightChildren.stream().mapToInt(i -> i).toArray();
		float[][] valueArr = new float[value.size()][2];
		for (int i = 0; i < value.size(); i++) {
			ArrayList<Double> possibility = (ArrayList<Double>) value.get(i).get(0);
			valueArr[i][0] = possibility.get(0).floatValue();
			valueArr[i][1] = (possibility.get(1)).floatValue();
		}
		int processOpID = findOperatorByName("dtree");
		// processing
		Function processFunc = planWithLock.getOperatorByID(processOpID).getUdf();
		Class<?> decisionTreeRuleClass = processFunc.getClass().getClassLoader().loadClass("flinkapp.frauddetection.rule.DecisionTreeRule");
		Object newRule = decisionTreeRuleClass        // reflection related
			.getConstructor(int[].class, float[].class, int[].class, int[].class, float[][].class)
			.newInstance(featureArr, thresholdArr, leftArr, rightArr, valueArr);
		Function newProcessFunc = processFunc.getClass()
			.getConstructor(decisionTreeRuleClass.getSuperclass())
			.newInstance(newRule);
		changeOfLogic(processOpID, newProcessFunc);
	}

	private String sendGet(String url) throws Exception {
		URL obj = new URL(url);
		HttpURLConnection con = (HttpURLConnection) obj.openConnection();

		con.setRequestMethod("GET");

		con.setRequestProperty("User-Agent", USER_AGENT);

		int responseCode = con.getResponseCode();
		System.out.println("\nSending 'GET' request to URL : " + url);
		System.out.println("Response Code : " + responseCode);

		BufferedReader in = new BufferedReader(
			new InputStreamReader(con.getInputStream()));
		String inputLine;
		StringBuffer response = new StringBuffer();

		while ((inputLine = in.readLine()) != null) {
			response.append(inputLine);
		}
		in.close();

		return response.toString();
	}

	private String sendGet(String url, int requestTime) throws Exception {
		System.out.println("\naccess from file for url: " + url + requestTime);
		String fileNamePrefix = url.contains("dtree") ? "dtree" : "scale";
		Path parameterFile = Paths.get(baseDir, fileNamePrefix + requestTime);
		return FileUtils.readFileUtf8(parameterFile.toFile());
	}
}
