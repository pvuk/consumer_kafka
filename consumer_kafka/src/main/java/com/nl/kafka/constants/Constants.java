package com.nl.kafka.constants;

/**
 * Code Reference: default is keyword:
 * 
 * Reserved words (like Java keywords, such as int or boolean) cannot be used as names.
 * keywords also can not be used as names
 * 
 * Java: https://www.geekster.in/articles/identifiers-java/
 * SQL: https://docs.oracle.com/cd/E19798-01/821-1841/bnbuk/index.html
 * 
 * @author P.V. UdayKiran
 * @version 1
 * @since created on Sun 01-Jun-2025 19:36
 */
public class Constants {
	
	public static interface DATE {
		public static final String E_dd_MMM_yyyy_HH_mm_ss_S = "E dd-MMM-yyyy HH:mm:ss.S";
		public static final String E_dd_MMM_yyyy_HH_mm_ss_S_z = "E dd-MMM-yyyy HH:mm:ss.S z";
	}
	
	public static interface KAFKA {
		
		public static interface CONSUMER {
			public static interface GROUP_ID {
				public static final String KAFKA_GROUP_STATION_ID = "kafka-group-station-id";
				public static final String KAFKA_GROUP_TRACE_BANK_ID = "kafka-group-trace-bank-id";
			}
		}
		
		public static interface TOPIC_NAME {
			public static final String MY_TOPIC = "my_topic";
			public static final String NO_OF_LITERS_FILLED = "no-of-liters-filled";
			public static final String NO_OF_LITERS_REMAINING = "no-of-liters-remaining";
			public static final String TOPIC_TRACE_BANK = "topic-trace-bank";
			public static final String TOPIC_TRACE_BANK_READ_EXCEL = "topic-trace-bank-read-excel";
		}
	}
	
	public static interface Status {
		public static final String SUCCESS = "SUCCESS";
		public static final String FAILED = "FAILED";
		public static final String IN_PROGRESS = "IN_PROGRESS";
		public static final String PENDING = "PENDING";
		public static final String SENT_TO_CONSUMER = "SENT_TO_CONSUMER";
		public static final String FAILED_SENT_TO_CONSUMER = "FAILED_SENT_TO_CONSUMER";
		public static final String FAILED_TO_PROCESS = "FAILED_TO_PROCESS";
	}
	
	public interface PORT {
		public static final String H2_TCP_PORT = "9010";
	}
}
