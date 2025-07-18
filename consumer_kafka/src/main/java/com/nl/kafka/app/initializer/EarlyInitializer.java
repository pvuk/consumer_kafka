package com.nl.kafka.app.initializer;

import java.sql.SQLException;

import org.h2.tools.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;

import com.nl.kafka.constants.Constants;
import com.nl.kafka.service.error.TraceBankErrorService;

/**
 * This class is initialized before configuring application.properties
 */
public class EarlyInitializer implements ApplicationContextInitializer<ConfigurableApplicationContext> {
	private final Logger LOG = LoggerFactory.getLogger(EarlyInitializer.class);

    private Server tcpServer;
    
    /**
     * H2 is light weight Java-based relational database that supports both embedded and server modes.
     * To allow multiple servers or applications to connect to the same H2 database, you must run H2 in server mode.
     */
    @Override
    public void initialize(ConfigurableApplicationContext applicationContext) {
    	/*
    	 * Code Ref:
    	 * Exception SQLException is not compatible with throws clause in ApplicationContextInitializer<ConfigurableApplicationContext>.initialize(ConfigurableApplicationContext)
    	 */
    	try {
    		System.out.println("EarlyInitializer: Bean initialized before application.properties");
            // You can register beans or do early setup here
            
            tcpServer = Server.createTcpServer("-tcp", "-tcpAllowOthers", "-tcpPort", Constants.PORT.H2_TCP_PORT).start();
        	
        	/*
        	 * Code Ref: Avoid Hardcoding Port
    			Let H2 pick a free port automatically:
        	 */
//            tcpServer = Server.createTcpServer("-tcp", "-tcpAllowOthers").start();

            LOG.info("H2 server started and listening on port "+ tcpServer.getPort());
		} catch (SQLException e) {
			String errMessage = e.getMessage();
			LOG.error("H2 Configuration Failed to run on manual port. Message: "+ errMessage);
			if(e.getMessage().contains("Address already in use")) {
				int port = 0;
				TraceBankErrorService.killPortInWindows(port, tcpServer, errMessage);
			}
		}
    }
    
}
