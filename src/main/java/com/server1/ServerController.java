package com.server1;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.springframework.context.annotation.Bean;
import org.springframework.http.HttpStatus;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.AsyncResult;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;


import redis.clients.jedis.Jedis;


@RestController
@EnableAsync
@RequestMapping("/")
public class ServerController {
	
	static ArrayList<User> users;
	
	@RequestMapping(value= "request/{alberto}", method = RequestMethod.GET)
	@ResponseStatus(HttpStatus.ACCEPTED)
	public Object getRequests(@PathVariable("alberto") String stato, @RequestParam("api-version") String apiVersion) throws InterruptedException, ExecutionException {
		ExecutorService executor = Executors.newFixedThreadPool(10);
		Future<ArrayList<User>> usersRetrieved = executor.submit(retrieveRequests);
		while (!usersRetrieved.isDone()) {
		Future<Integer> a = executor.submit(sendRequests);
		}
		
		return "Richiesta presa in carico";
		
	}
	
	static Callable retrieveRequests = () -> {
		Connection conn = null;
		PreparedStatement ps = null;
		int i = 0;
		try {
			Class.forName("com.mysql.jdbc.Driver").newInstance();
			conn = DriverManager.getConnection("172.30.20.211", "sebastiano", System.getenv("MYSQL_PWD"));
			String query = "select * from dbSeba.user where stato = ?";
			ps = conn.prepareStatement(query);
			ps.setString(1, "Disabled");
			ResultSet rs = ps.executeQuery();
			while (rs.next()) {
				users.add(new User(rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4)));
				i++;
			}
			System.out.println(users.toString());
		}catch(Exception e) {
			e.printStackTrace();
		}finally {
			if(conn != null)
				conn.close();
			if(ps != null)
				ps.close();
		}
		return i;
	};
	
	static Callable sendRequests = () -> {
		try {
			Jedis jClient = new Jedis("172.30.118.49", 6379);		
			jClient.auth(System.getenv("REDIS_PWD"));

			System.out.println(System.getenv("REDIS_PWD"));
			jClient.connect();
			for(int i = 0; i < users.size(); i++) {
				System.out.println("Pusho e pubblicooooo");
				User user = users.get(i);
				jClient.lpush("Users", user.toString());
				jClient.publish("Nuovi_Utenti", "eeee");
			}
		}catch(Exception e) {
			e.printStackTrace();
		}
		return 2;
	};

	@RequestMapping(value= "prova/", method = RequestMethod.GET)
	@ResponseStatus(HttpStatus.ACCEPTED)
	public String getRequest() throws InterruptedException, ExecutionException, TimeoutException {
		ExecutorService executor = Executors.newFixedThreadPool(10);
		Future <String> ciao= executor.submit(callableTask);
		return  "Richiesta presa in carico";
	}
	
	static Callable callableTask = () -> {
	    try {
	    	Thread.sleep(3000);
	        System.out.println("ciao");
	        System.out.println(Thread.currentThread().getName());
	    } catch (Exception e) {
	        e.printStackTrace();
	    }
	    return "ca";
	};

	
}
