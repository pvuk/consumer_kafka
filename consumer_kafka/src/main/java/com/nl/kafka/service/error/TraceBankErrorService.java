package com.nl.kafka.service.error;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.h2.tools.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface TraceBankErrorService {
	
	final static Logger LOG = LoggerFactory.getLogger(TraceBankErrorService.class);
	
	/**
	 * 🛠 Automate in Spring Boot (Optional)
		If you want to programmatically check and kill a port in Java (not recommended for production), you’d need to use native commands via Runtime.exec():
		
	 * @param port
	 * @param tcpServer
	 * @param errMessage
	 * @return
	 */
	public static String killPortInWindows(int port, Server tcpServer, String errMessage) {
		String message = null;
		if(tcpServer != null) {
			port = tcpServer.getPort();
		} else {
			Pattern pattern = Pattern.compile("\\b\\d{4}\\b");
	        Matcher matcher = pattern.matcher(errMessage);
	        
	        if (matcher.find()) {
	            String firstFourDigit = matcher.group();
	            port = Integer.valueOf(firstFourDigit);
	        } else {
	        	message = "No 4-digit number found.";
	            System.err.println(message);
	        }
		}
		boolean success = killPortInWindows(port);
		message = success ? "Port: " + port +" killed successfully" : "Killing port failed.";
		return null;
	}
	/**
     * 🛠 Automate in Spring Boot (Optional)
		If you want to programmatically check and kill a port in Java (not recommended for production), you’d need to use native commands via Runtime.exec():
     * @param port
	 * @return 
     */
	private static boolean killPortInWindows(int port) {
		if(port != 0) {
			int pid = getPID(port+"");
			try {
				Runtime.getRuntime().exec("taskkill /PID " + pid + " /F");
				return true;
			} catch (IOException e) {
				LOG.error("Error occured while kill port "+ port +", Message: "+ e.getMessage());
				e.printStackTrace();
			}
		}
		return false;
	}
	
	private static int getPID(String port) {
		int pid = 0;
        try {
            Process process = Runtime.getRuntime().exec("netstat -ano | findstr :" + port);
            BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println("Matching line: " + line);
                String[] tokens = line.trim().split("\\s+");
                if (tokens.length >= 5) {
                    pid = Integer.valueOf(tokens[tokens.length - 1]);
                    System.out.println("PID using port " + port + ": " + pid);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
		return pid;
	}
}
