package com.yna.ecqmanager;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import com.yna.ecqmanager.log.KLogger;
import com.yna.ecqmanager.log.LogData;

public class BurrowCheck implements Runnable{
	protected final static String PROPERTY_ERR_MSG = "properties파일에 값이 없습니다.";	
	protected final static String BURROW_IP_KEY = "burrow.ip";
	protected final static String BURROW_PORT_NUM_KEY = "burrow.port.num";
	protected final static String BURROW_CALL_URL_KEY = "burrow.call.url";
	protected final static String BURROW_CHECK_TIME_KEY = "burrow.check.time";
	protected final static String LOG_PATH_KEY = "log.path";
	protected final static String TELEGRAM_TOKEN = "telegram.token";
	protected final static String TELEGRAM_CHAT_ID = "telegram.chat.id";
	
	/** 프로퍼티 파일 path */
	private String propertyPath;
	
	/** 프로퍼티 key, value를 담을 map */
	private Map<String, String> propertyMap;
	
	/** log를 저장할 path */
	private String logPath;
	
	/** burrow http api를 call할 시간 간격 */
	private int burrowCheckTime;
	
	/** consumer 이름 정보 */
	private String[] consumerName;
	
	private Consumer[] consumer;
	
	/** BurrowCheck class의 로거 */
	private KLogger logger;
	
	/**
	 * 생성자.<br>
	 * 호출 불가능.
	 */
	private BurrowCheck()
	{
		// can't declare without argument
	}
	
	/**
	 * 생성자
	 * @param environment 서버 환경
	 */
	public BurrowCheck(String propertyPath, String[] consumerName)
	{
		if(propertyPath == null || propertyPath.length() == 0 || consumerName == null)
			throw new NullPointerException("argument가 null");
		
		try
		{
			this.init(propertyPath, consumerName);
			this.setProperty();
			this.setConfig();
			this.setConsumers();
		}
		catch(Exception e)
		{
			Thread.interrupted();
		}
	}
	
	/**
	 * init 함수<br>
	 * burrowCheck 클래스에서 사용하는 map, list등을 초기화함.
	 * @param propertyPath 프로퍼티 파일의 path
	 * @param consumerName 관제할 컨슈머 그룹명
	 */
	private void init(String propertyPath, String[] consumerName) throws Exception
	{
		this.propertyPath = propertyPath;
		this.consumerName = consumerName;
		this.propertyMap = new HashMap<String, String>();
	}
	
	/**
	 * properties 파일을 읽어서 propertyMap에다 저장한다.
	 */
	private void setProperty() throws Exception
	{
		try(FileInputStream fis = new FileInputStream(this.propertyPath))
		{
			Properties p = new Properties();
			p.load(fis);
			
			String key = null;
			String value = null;
			
			for(Object o : p.keySet())
			{
				key = (String)o;
				value = p.getProperty(key);
				this.propertyMap.put(key, value);
			}
		}
		catch(Exception e)
		{
			e.printStackTrace(System.out);
			throw new Exception(e);
		}
	}
	
	/**
	 * 설정값들을 초기화하는 메소드.
	 * @throws NullPointerException burrow 설정값이 빈값인 경우 발생
	 */
	private void setConfig() throws NullPointerException
	{
		// 
		if(this.propertyMap.get(BURROW_IP_KEY) == null || this.propertyMap.get(BURROW_PORT_NUM_KEY) == null 
				|| this.propertyMap.get(BURROW_CALL_URL_KEY) == null)
		{	
			throw new NullPointerException(PROPERTY_ERR_MSG);
		}
		
		// log path
		this.logPath = this.propertyMap.getOrDefault(LOG_PATH_KEY, "./");
				
		// set logger for burrowcheck
		this.logger = new KLogger("BurrowCheck", this.logPath, KLogger.INFO);
		this.logger.setErrorLog(true, "BurrowCheck.error.log");
		
		// burrow check time
		try
		{
			this.burrowCheckTime = Integer.parseInt(this.propertyMap.get(BURROW_CHECK_TIME_KEY)) * 1000;
		}
		catch(Exception e)
		{
			this.burrowCheckTime = 30000;
		}
		
	}
	
	/**
	 * Consumer 객체를 초기화하는 메소드 <br>
	 * 입력된 객체에 에러가 있을 경우, this.Consumer, this.consumerName 에서 해당 객체를 제외한다.
	 */
	private void setConsumers()
	{
		this.consumer = new Consumer[this.consumerName.length];
		int errorCount = 0;
		
		for(int i = 0; i < this.consumerName.length; i++)
		{
			try
			{
				this.consumer[i] = new Consumer(this.consumerName[i], this.propertyMap);
			}
			catch(Exception e)
			{
				e.printStackTrace();
				this.consumer[i] = null;
				this.consumerName[i] = null;
				errorCount++;
			}
		}
		
		// 에러 발생시, Consumer[] 배열에서 에러 발생한 Consumer는 제거. consumerName[] 배엘여서 에러발생한 consumerName 제거.
		if(errorCount > 0)
		{
			Consumer[] tempConsumer = new Consumer[this.consumerName.length - errorCount];
			String[] tempConsumerName = new String[this.consumerName.length - errorCount];
			int arrNum = 0;
			
			for(int i = 0; i < this.consumerName.length; i++)
			{
				if(this.consumer[i] != null)
				{
					tempConsumer[arrNum] = this.consumer[i];
					tempConsumerName[arrNum] = this.consumerName[i];
					arrNum++;
				}
			}
			this.consumer = tempConsumer;
			this.consumerName = tempConsumerName;
		}
	}
	
	/**
	 * burrow api를 call하는 메소드
	 */
	private void callAPI()
	{
		try
		{
			for(int i = 0; i < this.consumerName.length; i++)
				this.callAPI(this.consumerName[i]);
		}
		catch(Exception e)
		{
			this.logger.log(new LogData(KLogger.ERROR, "callAPI() fail. ", e));
		}
	}
	
	/**
	 * 컨슈머별로 burrow api를 call하는 메소드
	 * @param consumerName 컨슈머명
	 */
	private void callAPI(String consumerName) throws Exception
	{
		try
		{
			HttpURLConnection connection = this.getRequest(consumerName);
			JSONObject resultJSON = this.getResponse(connection, consumerName);
			
			if(resultJSON != null)
			{
				this.getConsumerByName(consumerName).getLogger().log(new LogData(KLogger.INFO, resultJSON.toJSONString()));
				this.parseJson(resultJSON, consumerName);
			}
			else
			{
				throw new Exception();
			}
		}
		catch(Exception e)
		{
			this.logger.log(new LogData(KLogger.ERROR, "callAPI(consumerName) fail. consumerName : ", consumerName, e));
			this.getConsumerByName(consumerName).getLogger().log(new LogData(KLogger.ERROR, "callAPI(consumerName) fail. consumerName : ", consumerName, e));
		}
	}
	
	/**
	 * burrow api를 call하는 request를 리턴하는 메소드<br>
	 * 예외가 발생할 경우, null을 리턴한다.
	 * @param consumerName 컨슈머명
	 * @return HttpURLConnection 커넥션
	 */
	private HttpURLConnection getRequest(String consumerName)
	{
		try
		{
			URL url = new URL(this.getConsumerByName(consumerName).getUrl());
			HttpURLConnection connection = (HttpURLConnection) url.openConnection();
			connection.setConnectTimeout(3000);
			connection.setUseCaches(false);
			connection.setRequestMethod("GET");
			this.logger.log(new LogData(KLogger.INFO, "getRequest() success. consumerName :", consumerName));
			return connection;
		}
		catch(Exception e)
		{
			this.logger.log(new LogData(KLogger.ERROR, "getRequest() fail. consumerName : ", consumerName, e));
			this.getConsumerByName(consumerName).getLogger().log(new LogData(KLogger.ERROR, "getRequest() fail. consumerName : ", consumerName, e));
			this.getConsumerByName(consumerName).sendTelegramMsg("ERROR", "getRequest() fail.");
			return null;
		}
	}
	
	/**
	 * connection으로 부터, response를 받아와 json으로 리턴하는 메소드<br>
	 * connection이 null일 경우, null을 리턴한다.
	 * @param connection 커넥션
	 * @param consumerName 컨슈머명
	 * @return JSONObject 결과
	 */
	private JSONObject getResponse(HttpURLConnection connection, String consumerName) 
	{
		if(connection == null)
			return null;
		
		StringBuffer result = new StringBuffer();
		JSONParser parser = new JSONParser();
		
		try
		{
			BufferedReader bf = new BufferedReader(new InputStreamReader(connection.getInputStream()));
			String inputLine = null;
		
			while((inputLine = bf.readLine()) != null)
			{
				result.append(inputLine);
			}
			bf.close();
			
			this.logger.log(new LogData(KLogger.INFO, "getResponse() success. consumerName :", consumerName));
			return (JSONObject) parser.parse(result.toString());
		}
		catch(Exception e)
		{
			this.logger.log(new LogData(KLogger.ERROR, "getResponse() fail. consumerName : ", consumerName, e));
			this.getConsumerByName(consumerName).getLogger().log(new LogData(KLogger.ERROR, "getResponse() fail. consumerName : ", consumerName, e));
			this.getConsumerByName(consumerName).sendTelegramMsg("ERROR", "getResponse() fail.");
			return null;
		}
	}
	
	/**
	 * json값을 파싱하여 로그 기록, 텔레그램 전송하는 메소드
	 * @param json burrow api를 콜한 결과값
	 * @param consumerName 컨슈머명
	 */
	private void parseJson(JSONObject json, String consumerName)
	{
		StringBuilder result = null;
		try
		{
			Boolean error = (Boolean) json.get("error");
			String errmsg = null;
			
			// burrow error
			if(error)
			{
				errmsg = (String) json.get("message");
				this.logger.log(new LogData(KLogger.ERROR, "Burrow ERROR! consumerName : ", consumerName, ", errorMsg : ", errmsg, ", resultJson : ", json));
				this.getConsumerByName(consumerName).getLogger().log(new LogData(KLogger.ERROR, "Burrow ERROR! errorMsg :", errmsg, ", resultJson :", json));
				this.getConsumerByName(consumerName).sendTelegramMsg("ERROR", 
						new StringBuilder("Burrow ERROR! errorMsg : ").append(errmsg).append(", resultJson : ").append(json).toString());
			}
			
			// consumer error
			JSONObject statusJson = (JSONObject) json.get("status");
			String status = (String) statusJson.get("status");
			String cluster = (String) statusJson.get("cluster");
			String group = (String) statusJson.get("group");
			Long totalLag = (Long) statusJson.get("totallag");

			// consumer lag check
			JSONArray partitions = (JSONArray) statusJson.get("partitions");
			JSONObject tempPartition = null;
			String topic = null;
			Long partition = -9L;
			Long currentLag = -9L;
			
			for(int i = 0; i < partitions.size(); i++)
			{
				tempPartition = (JSONObject) partitions.get(i);
				topic = (String) tempPartition.get("topic");
				partition = (Long) tempPartition.get("partition");
				currentLag = (Long) tempPartition.get("current_lag");
				
				// 처음 parseJson이 호출될때 실행됨.
				if(this.getConsumerByName(consumerName).getPartionList().size() < partitions.size())
				{	
					PartitionInfo partitionInfo = new PartitionInfo(topic, partition);
					this.getConsumerByName(consumerName).getPartionList().add(partitionInfo);
				}
				
				// lag 변경
				for(PartitionInfo partitionInfo : this.getConsumerByName(consumerName).getPartionList())
				{
					if(partitionInfo.getTopicName().equals(topic) && partitionInfo.getPartition() == partition)
					{
						partitionInfo.setLag(currentLag);
						break;
					}
				}
			}
			
			result = new StringBuilder("cluster : ").append(cluster).append(", consumer : ").append(group).append(", status : ")
					.append(status).append(", totalLag : ").append(totalLag);
			
			for(int i = 0; i < this.getConsumerByName(consumerName).getPartionList().size(); i++)
			{
				result.append(", ").append(this.getConsumerByName(consumerName).getPartionList().get(i).getInfo());
			}
			
			if("OK".equals(status) || "STALL".equals(status))
			{
				this.logger.log(new LogData(KLogger.INFO, result.toString()));
				this.getConsumerByName(consumerName).getLogger().log(new LogData(KLogger.INFO, result.toString()));
			}
			else if("WARN".equals(status))
			{
				this.logger.log(new LogData(KLogger.WARN, result.toString()));
				this.getConsumerByName(consumerName).getLogger().log(new LogData(KLogger.WARN, result.toString()));
				this.getConsumerByName(consumerName).sendTelegramMsg("WARN", result.toString());
			}
			else
			{
				this.logger.log(new LogData(KLogger.ERROR, result.toString()));
				this.getConsumerByName(consumerName).getLogger().log(new LogData(KLogger.ERROR, result.toString()));
				this.getConsumerByName(consumerName).sendTelegramMsg("ERROR", result.toString());
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
			this.logger.log(new LogData(KLogger.ERROR, "parseJson() fail", e));
			this.getConsumerByName(consumerName).getLogger().log(new LogData(KLogger.ERROR, "parseJson() fail", e));
			this.getConsumerByName(consumerName).sendTelegramMsg("ERROR", "parseJSON() fail.");
		}
	}
	
	public void run()
	{
		Thread burrowLoggerThread = null;
		Thread[] consumerLoggerThread = null;

		try
		{
			// burrowCheck logger
			burrowLoggerThread = new Thread(this.logger);
			burrowLoggerThread.start();
			this.logger.log(new LogData(KLogger.INFO, "[[BurrowCheck run]]"));
		
			// consumer logger
			consumerLoggerThread = new Thread[this.consumer.length];
			for(int i = 0; i < this.consumer.length; i++)
			{
				consumerLoggerThread[i] = new Thread(this.consumer[i].getLogger());
				consumerLoggerThread[i].start();
				this.consumer[i].getLogger().log(new LogData(KLogger.INFO, this.consumerName[i], "run"));
			}
			
			// call burrow http api
			while(!Thread.currentThread().isInterrupted())
			{
				this.callAPI();
				Thread.sleep(this.burrowCheckTime);
			}
		}
		catch(Exception e)
		{
			this.logger.log(new LogData(KLogger.ERROR, "burrowCheck die. ", e));
			e.printStackTrace(System.out);
		}
		finally
		{
			for(int i = 0; i < this.consumer.length; i++)
				this.consumer[i].getLogger().terminate();

			this.logger.terminate();
		}
		
	}
	
	/**
	 * consumerName에 따라 해당하는 Consumer 객체를 리턴받는 메소드<br>
	 * null이 리턴될 경우, 해당 컨슈머가 제대로 생성되지 않았음을 나타낸다.
	 * @param consumerName 컨슈머명
	 * @return Consumer 컨슈머 객체
	 */
	private Consumer getConsumerByName(String consumerName)
	{
		for(int i = 0; i < this.consumer.length; i++)
		{
			if(this.consumer[i].getConsumerName().equals(consumerName))
				return this.consumer[i];
		}
		return null;
	}
	
	class Consumer {
		/** consumer group 이름 */
		private String consumerName;
		
		/** consumer logger */
		private KLogger consumerLogger;
		
		/** consumer log를 저장할 path */
		private String logPath;
		
		/** 호출할 burrow http api */
		private String consumerUrl;
		
		/** telegram bot use yn*/
		private boolean isUseTelegram;
		
		/** telegram token */
		private String telegramToken;
		
		/** telegram chatId */
		private String[] telegramChatId;
		
		/** partition별 lag을 기록하는 list*/
		private List<PartitionInfo> partitionList;
		
		/**
		 * Consumer 생성자<br>
		 * 컨슈머명과 설정정보를 담은 map을 인수로 입력해야 한다.
		 * 
		 * @param consumerName 컨슈머그룹명
		 * @param map 설정 정보를 담은 map
		 */
		Consumer(String consumerName, Map<String, String> map) throws Exception
		{
			this.consumerName = consumerName;
			this.partitionList = new LinkedList<PartitionInfo>();
			this.setConfig(map);
		}
		
		/**
		 * Consumer 객체에서 필요한 설정을 초기화하는 메소드
		 * @param map 설정 정보를 담은 map
		 * @throws NullPointerException map이 비어있을 때 발생
		 * @throws Exception 기타 나머지 오류로 인한 예외시 발생
		 */
		private void setConfig(Map<String, String> map) throws NullPointerException, Exception
		{
			if(map == null || map.size() == 0)
				throw new NullPointerException(BurrowCheck.PROPERTY_ERR_MSG);
			
			try
			{
				// consumer log path
				this.logPath = map.getOrDefault(BurrowCheck.LOG_PATH_KEY, "./");
				
				// set logger for consumer
				this.consumerLogger = new KLogger(this.consumerName, this.logPath, KLogger.INFO);
				this.consumerLogger.setErrorLog(true, this.consumerName + ".error.log");
				
				// burrow url
				consumerUrl = new StringBuilder("http://").append(map.get(BURROW_IP_KEY)).append(":")
						.append(map.get(BURROW_PORT_NUM_KEY)).append(map.get(BURROW_CALL_URL_KEY)).append(this.consumerName).append("/lag").toString();
				
				// telegram configs
				try
				{
					this.telegramToken = map.get(TELEGRAM_TOKEN).trim();
					this.telegramChatId = map.get(new StringBuilder(this.consumerName.toUpperCase()).append(".").append(TELEGRAM_CHAT_ID).toString()).trim().split(";");
				
					if(this.telegramToken != null && this.telegramChatId != null)
						this.isUseTelegram = true;
				}
				catch(Exception e)
				{
					this.isUseTelegram = false;
				}
			}
			catch(Exception e)
			{
				throw new Exception(e);
			}
		}
		
		/**
		 * 유저별 텔레그램 메세지를 전송하는 메소드.<br>
		 * 컨슈머에 등록된 유저들에게 텔레그램 메세지를 보냄.
		 * @param logLevel 로그 레벨
		 * @param msg 메세지
		 */
		protected void sendTelegramMsg(String logLevel, String msg)
		{
			if(this.isUseTelegram)
			{
				Telegram telegram = null;
				for(int i = 0; i < this.telegramChatId.length; i++)
				{
					telegram = new Telegram(this.telegramToken, this.telegramChatId[i], new StringBuilder("[").append(this.consumerName)
							.append("] [").append(logLevel).append("] ").append(msg).toString());
					telegram.sendMessage();
				}
			}
		}
		
		/**
		 * 컨슈머 이름을 리턴하는 메소드
		 * @return consumerName 컨슈머 이름
		 */
		protected String getConsumerName()
		{
			return this.consumerName;
		}
		/**
		 * consumer의 로그를 기록하는 로거를 리턴하는 메소드
		 * @return consumerLogger 로거
		 */
		protected KLogger getLogger()
		{
			return this.consumerLogger;
		}
		/**
		 * 호출할 burrow http api를 리턴하는 메소드
		 * @return consumerUrl call할 burrow url
		 */
		protected String getUrl()
		{
			return this.consumerUrl;
		}
		/**
		 * Telegram bot의 사용유무를 리턴하는 메소드
		 * @return isUseTelegram 텔레그램 사용여부
		 */
		protected boolean isUseTelegram()
		{
			return this.isUseTelegram;
		}
		
		/**
		 * partitionList를 리턴하는 메소드
		 * @return partitionList 구독하는 토픽들이 담긴 리스트
		 */
		protected List<PartitionInfo> getPartionList()
		{
			return this.partitionList;
		}
	}
}