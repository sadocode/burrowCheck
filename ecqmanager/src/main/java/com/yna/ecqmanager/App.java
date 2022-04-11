package com.yna.ecqmanager;

import java.util.Arrays;

public class App 
{
    public static void main( String[] args )
    {
    	if(args.length == 0)
    	{
    		System.out.println("-h 로 argument를 확인하세요.");
    	}
    	else if("-h".equals(args[0]))
    	{
    		System.out.println("첫 번째 인수 : 설정 파일의 path\n두 번째 인수 : 컨슈머 이름 (예: ECR, ECR_QA, ECR_DEV) 컨슈머 이름은 설정파일과 동일하게 해야함."
    				+ "\n2번째 인수까지는 필수. 3번째부터는 관제할 또 다른 컨슈머명");
    	}
    	else if(args[0] != null && args[0].length() > 0)
    	{
    		if(args.length == 1)
    		{
    			System.out.println("-h 로 argument를 확인하세요.");
    		}
    		else
    		{
    			String[] arr = Arrays.copyOfRange(args, 1, args.length);
    			for(int i = 0; i < arr.length; i++)
    				arr[i] = arr[i].trim().toUpperCase();

    			BurrowCheck burrowCheck = new BurrowCheck(args[0].trim(), arr);
    			Thread thread = new Thread(burrowCheck);
        		thread.start();
    		}
    	}
    	else
    	{
    		System.out.println("-h 로 argument를 확인하세요.");
    	}
    }
}
