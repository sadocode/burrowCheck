package com.yna.ecqmanager.log;
import java.text.SimpleDateFormat;
import java.util.Date;;

/**
 * Log 인터페이스를 구현한 LogData 클래스<br>
 * logLevel, logTime, logMsg 등 로그 정보를 담는다.
 * @author 황경진
 *
 */
public class LogData implements Log{

	/** 로그 날짜 형식*/
	private static final String DATE_FORMAT = "yyyyMMdd HH:mm:ss.SSS";
	
	/** 로그 레벨*/
	private int logLevel;
	
	/** 로그 시간*/
	private String logTime;
	
	/** 로그 메세지*/
	private StringBuilder logMsg;
	
	/**
	 * LogData 기본 생성자\n
	 * 사용 안 함
	 */
	private LogData()
	{
		throw new AssertionError();
	}
	
	/**
	 * LogData 생성자<br>
	 * logLevel 인자는 KLogger의 public static 변수를 참고한다.
	 * @param logLevel 로그 레벨 (KLogger의 변수 참고)
	 * @param logMsg 로그 메세지
	 */
	public LogData(int logLevel, String logMsg)
	{
		this(logLevel, logMsg, null);
	}
	
	/**
	 * LogData 생성자<br>
	 * logLevel 인자는 KLogger의 public static 변수를 참고한다.<br>
	 * Object 인자를 넣을 시, toString으로 logMsg에 더해지게 된다.
	 * @param logLevel 로그 레벨 (KLogger의 변수 참고)
	 * @param logMsg 로그 메세지
	 * @param objects 기타 Object
	 */
	public LogData(int logLevel, String logMsg, Object...objects)
	{
		if(logMsg == null || logMsg.length() == 0)
			throw new NullPointerException(); 
		if (logLevel < KLogger.DEBUG || logLevel > KLogger.FATAL)
			throw new IllegalArgumentException();
			
		this.logLevel = logLevel;
		this.setLogTime();
		this.setLogMsg(logMsg, objects);
	}
	
	/**
	 * logLevel을 리턴한다.
	 * @return logLevel 로그레벨
	 */
	@Override
	public int getLogLevel()
	{
		return this.logLevel;
	}
	
	/**
	 * logTime을 리턴한다.
	 * @return logTime 로그 타임
	 */
	@Override
	public String getLogTime()
	{
		return this.logTime;
	}
	
	/**
	 * logMsg를 리턴한다.
	 * @return logMsg 로그 메세지
	 */
	@Override
	public String getLogMsg()
	{
		return this.logMsg.toString();
	}
	
	/**
	 * logTime을 설정한다.
	 */
	private void setLogTime()
	{
		Date tempDate = new Date(System.currentTimeMillis());
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat(DATE_FORMAT);
		this.logTime = simpleDateFormat.format(tempDate);
	}
	
	/**
	 * LogData를 생성할 때 argument로 받은 logMsg와 objects를 String으로 바꿔 붙인 값을 리턴한다.
	 * @param logMsg 로그 메세지
	 * @param objects 오브젝트
	 */
	private void setLogMsg(String logMsg, Object...objects)
	{
		this.logMsg = new StringBuilder(logMsg);
		
		if(objects != null)
		{
			for(Object object : objects)
			{
				if(object != null)
					this.logMsg.append(" ").append(object.toString());
			}
		}
	}
}
