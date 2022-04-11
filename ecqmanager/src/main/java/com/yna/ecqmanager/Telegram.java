package com.yna.ecqmanager;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

public class Telegram {
	
	private static final String API_URL1 = "https://api.telegram.org/bot";
	private static final String API_URL2 = "/sendmessage?chat_id=";
	private static final String API_URL3 = "&text=";
	private String apiUrl;
	
	public Telegram(String token, String chatId, String errMsg)
	{
		if(token == null || token.length() == 0 || chatId == null || chatId.length() == 0 || errMsg == null || errMsg.length() == 0)
			throw new NullPointerException("입력값이 안 들어옴.");
			
		StringBuilder tempUrl = new StringBuilder(API_URL1).append(token).append(API_URL2).append(chatId).append(API_URL3).append(errMsg);
		this.apiUrl = tempUrl.toString().replaceAll(" ", "%20");
	}
	
	public void sendMessage()
	{
		BufferedReader br = null;
		
		try
		{
			URL url = new URL(this.apiUrl);
			HttpURLConnection con = (HttpURLConnection)url.openConnection();
			con.setRequestMethod("GET");
			br = new BufferedReader(new InputStreamReader(con.getInputStream()));
			
			String tempLine = null;
			while((tempLine = br.readLine()) != null)
			{
				System.out.println(tempLine);
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		finally
		{
			if(br != null)
			{
				try
				{
					br.close();
				}
				catch(Exception e)
				{
					e.printStackTrace();
				}
			}
		}
	}
}
