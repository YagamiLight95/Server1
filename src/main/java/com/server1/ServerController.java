package com.server1;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
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
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.AsyncResult;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import com.google.gson.Gson;

import redis.clients.jedis.Jedis;


@RestController
@EnableAsync
@RequestMapping("/")
public class ServerController {
	
	static ArrayList<User> users = new ArrayList<User>();
	static ArrayList<User> send2Cedacri = new ArrayList<User>();
	static String operation = "";
	
	@RequestMapping(value= "request2insert", method = RequestMethod.GET)
	public ResponseEntity<String> getRequests() throws InterruptedException, ExecutionException {
		ExecutorService executor = Executors.newFixedThreadPool(10);
		System.out.println("Avvio Retriever");
		Future<Integer> usersRetrieved = executor.submit(retrieveRequests);
		while(!usersRetrieved.isDone()) {
			// Wait....
		}
		if(usersRetrieved.get()==2) {
			System.out.println("Avvio Sender");
			operation = "memorizza";
			Future<Integer> a = executor.submit(sendRequests);
			while(!a.isDone()) {
				// Wait...
			}
			executor.shutdownNow();
		}
		
		return new ResponseEntity<String>("Richiesta presa in carico", HttpStatus.ACCEPTED);
		
	}
	
	@RequestMapping(value= "send2cedacri/", method = RequestMethod.POST)
	public ResponseEntity<Boolean> send2Cedacri(@RequestBody String gson){
		System.out.println(gson);
		System.out.println("Richiesta ben formata");
		return new ResponseEntity<Boolean>(true,HttpStatus.OK);
		
	}

	@RequestMapping(value= "updaterequest/", method = RequestMethod.PUT)
	public ResponseEntity<Boolean> updateRequest(@RequestBody String gson) throws SQLException{
		System.out.println(gson);
		Gson g = new Gson();
		User user = g.fromJson(gson, User.class);
		Connection cn = null;
		PreparedStatement ps = null;
		int rs = 0;
		try {
			Class.forName("org.postgresql.Driver");
			String url = "jdbc:postgresql://172.17.0.9:5432/db";
			cn = DriverManager.getConnection(url, "sebastiano", System.getenv("PWD_PSQL"));
			String sql = "update users set sent2Cedacri = 1 where login = ?";
			ps = cn.prepareStatement(sql);
			ps.setString(1, user.getLogin());
			rs = ps.executeUpdate();
		}catch(Exception e) {
			e.printStackTrace();
		}finally {
			if(cn != null) {
				cn.close();
			}
			if(ps != null) {
				ps.close();
			}
		}
		
		if(rs == 1) {
			System.out.println("Utente aggiornato correttamente");
			return new ResponseEntity<Boolean>(true,HttpStatus.OK);
		}
		return new ResponseEntity<Boolean>(false,HttpStatus.INTERNAL_SERVER_ERROR);
		
	}
	
	
	@RequestMapping(value= "insertrequest/", method = RequestMethod.POST)
	public ResponseEntity<Boolean> insertRequest(@RequestBody String gson) throws SQLException {
		System.out.println(gson);
		Gson g = new Gson();
		User user = g.fromJson(gson, User.class);
		Connection cn = null;
		PreparedStatement ps = null;
		int rs = 0;
		try {
			Class.forName("org.postgresql.Driver");
			String url = "jdbc:postgresql://172.17.0.9:5432/db";
			cn = DriverManager.getConnection(url, "sebastiano", System.getenv("PWD_PSQL"));
			String sql = "insert into users  (nome, cognome, login, stato, sent2cedacri) values (?,?,?,?,?)";
			ps = cn.prepareStatement(sql);
			ps.setString(1, user.getNome());
			ps.setString(2, user.getCognome());
			ps.setString(3, user.getLogin());
			ps.setString(4, user.getStatus());
			ps.setString(5, "0");
			rs = ps.executeUpdate();
		}catch(Exception e) {
			e.printStackTrace();
		}finally {
			if(cn != null) {
				cn.close();
			}
			if(ps != null) {
				ps.close();
			}
		}
		
		if(rs == 1) {
			System.out.println("Utente inserito correttamente");
			return new ResponseEntity<Boolean>(true,HttpStatus.CREATED);
		}
		return new ResponseEntity<Boolean>(false,HttpStatus.INTERNAL_SERVER_ERROR);
		
	}
	
	@RequestMapping(value= "request2send/", method = RequestMethod.GET)
	public ResponseEntity<String> request2send(){
		ExecutorService es = Executors.newFixedThreadPool(10);
		Future<Object> ft = es.submit(retrieveRequests4Cedacri);
		while(!ft.isDone()) {
			// Wait...
		}
		operation = "forward";
		Future<Object> f = es.submit(sendRequests);
		while(!f.isDone()) {
			//Wait...
		}
		es.shutdownNow();

		return new ResponseEntity<String>("Partito recupero delle richieste per invio a Cedacri",HttpStatus.ACCEPTED);
	}

	static Callable retrieveRequests4Cedacri = () -> {
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			Class.forName("org.postgresql.Driver");
			String url = "jdbc:postgresql://172.17.0.9:5432/db";
			conn = DriverManager.getConnection(url, "sebastiano", System.getenv("PWD_PSQL"));
			String query = "select nome,cognome,login,status from users where sent2cedacri = ?";
			ps = conn.prepareStatement(query);
			ps.setString(1, "0");
			ps.setFetchSize(500);
			rs = ps.executeQuery();
			while(rs.next()) {
				send2Cedacri.add(new User(rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4)));				
			}
			send2Cedacri.trimToSize();
			System.out.println("Dimensione send2Cedacri: " +send2Cedacri.size() + " contenuto: " +send2Cedacri.toString());
		}catch(Exception e) {
			e.printStackTrace();
		}finally {
			if(conn != null) { conn.close(); } 
			if(ps != null)   { ps.close(); }
			if(rs != null)   { rs.close(); }
		}
		
		return null;
	};
	
	
	static Callable retrieveRequests = () -> {
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			Class.forName("com.mysql.cj.jdbc.Driver");
			conn = DriverManager.getConnection("jdbc:mysql://172.30.20.211:3306/dbSeba", "sebastiano", System.getenv("MYSQL_PWD"));
			String query = "select * from user where stato = ?";
			ps = conn.prepareStatement(query);
			ps.setString(1, System.getenv("Query_Param"));
			ps.setFetchSize(500);
			rs = ps.executeQuery();
			while (rs.next()) {
				System.out.println(rs.getString(1) + " " + rs.getString(2) + " " +  rs.getString(3) + " " + rs.getString(4));
				users.add(new User(rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4)));
			}
			System.out.println(users.toString());
		}catch(Exception e) {
			e.printStackTrace();
		}finally {
			if(conn != null)
				conn.close();
			if(ps != null)
				ps.close();
			if(rs != null) 
				rs.close();
		}
		return 2;
	};
	
	static Callable sendRequests = () -> {
		try {
			Jedis jClient = new Jedis("172.30.118.49", 6379);		
			String verifica = jClient.auth(System.getenv("REDIS_PWD"));
			jClient.connect();
			System.out.println(verifica);
			System.out.println(System.getenv("REDIS_PWD"));
			
			System.out.println("Dimensione: " + users.size());
			System.out.println("Pusho e pubblicooooo");
			String channel = "";
			String queue = "";
			ArrayList<User> useIt = new ArrayList<User>();
			if(operation.equalsIgnoreCase("memorizza")) {
				queue = "Users";
				channel = "Nuovi_Utenti";
				useIt.addAll(users);
				users = new ArrayList<User>();
				useIt.trimToSize();
			}else if(operation.equalsIgnoreCase("forward")) {
				queue = "Cedacri";
				channel = "Forward2Cedacri";
				useIt.addAll(send2Cedacri);
				send2Cedacri = new ArrayList<User>();
				useIt.trimToSize();
			}
			for(int i = 0; i < useIt.size(); i++) {	
				User user = useIt.get(i);
				Gson g = new Gson();
				String push = g.toJson(user);
				jClient.lpush(queue, push);
				jClient.publish(channel, "Nuove Operazioni");
			}
			jClient.disconnect();
		}catch(Exception e) {
			e.printStackTrace();
		}
		return 5;
	};

	@RequestMapping(value= "prova/", method = RequestMethod.GET)
	public ResponseEntity<User> getRequest() throws InterruptedException, ExecutionException, TimeoutException {
		ExecutorService executor = Executors.newFixedThreadPool(10);
		Future <String> ciao= executor.submit(callableTask);
		return  new ResponseEntity<User>(new User("ss","asda","ss","adfa"), HttpStatus.ACCEPTED);
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
