package com.server1;

public class User {
	
	private final String nome;
	private final String cognome;
	private final String login;
	private final String status;
	
	
	
	public User(String nome, String cognome, String login, String status) {
		super();
		this.nome = nome;
		this.cognome = cognome;
		this.login = login;
		this.status = status;
	}


	@Override
	public String toString() {
		return getNome()+","+getCognome()+","+getLogin()+","+getStatus();
	}


	public String getNome() {
		return nome;
	}


	public String getCognome() {
		return cognome;
	}


	public String getLogin() {
		return login;
	}


	public String getStatus() {
		return status;
	}
	
	
}
