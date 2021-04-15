package org.apache.flink.streaming.controlplane.udm;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.controlplane.streammanager.insts.ExecutionPlanWithLock;
import org.apache.flink.streaming.controlplane.streammanager.insts.ReconfigurationExecutor;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Map;


public class FraudDetectionController extends AbstractController {

	private final String USER_AGENT = "Mozilla/5.0";

	public FraudDetectionController(ReconfigurationExecutor reconfigurationExecutor) {
		super(reconfigurationExecutor);
	}

	@Override
	public void startControllers() {
		super.controlActionRunner.start();
	}

	@Override
	public void stopControllers() {

	}


	private Map<String, Object> parseJsonString(String json) throws JsonProcessingException {
		ObjectMapper objectMapper = new ObjectMapper();
		return objectMapper.readValue(json, new TypeReference<Map<String, Object>>() {
		});
	}

	@Override
	protected void defineControlAction() throws Exception {
		super.defineControlAction();
		ExecutionPlanWithLock planWithLock = getReconfigurationExecutor().getExecutionPlanCopy();
		updatePreprocessingScaleParameter(planWithLock);
		updateDecisionTreeParameter(planWithLock);
	}

	private void updatePreprocessingScaleParameter(ExecutionPlanWithLock planWithLock) throws Exception {
		String scalePara = sendGet("http://127.0.0.1:5000/scale");
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
		String treePara = sendGet("http://127.0.0.1:5000/dtree");
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
}
