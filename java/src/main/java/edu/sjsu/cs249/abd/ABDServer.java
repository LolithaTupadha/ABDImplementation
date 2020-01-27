package edu.sjsu.cs249.abd;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Timestamp;
import java.util.HashMap;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import edu.sjsu.cs249.adb.*;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

public class ABDServer extends ABDServiceGrpc.ABDServiceImplBase {
	static class reg {
		String value;
		long timestamp;

		reg(String value, long l) {
			this.value = value;
			this.timestamp = l;
		}
	}

	private Server server;
	// In this program we use both hashmaps and SQLite
	// We access HashMaps because it is faster compared to db
	// SQLITE db is used for persistence
	static HashMap<String, reg> registers = new HashMap<>();

	public ABDServer(int port) {
		System.out.println("Starting the server on port " + port);
		this.server = ServerBuilder.forPort(port).addService(this).build();
	}

	@Override
	public void read1(Read1Request request, StreamObserver<Read1Response> responseObserver) {
		System.out.println("trying to read " + request.getRegister());
		// System.out.println("Register
		// vale"+registers.get(request.getRegister()).value);//null pointer
		String value = fetch(request.getRegister());
		value = (value == null) ? " " : value;
		Read1Response rsp = Read1Response.newBuilder().setValue(value).build();
		responseObserver.onNext(rsp);
		responseObserver.onCompleted();
	}

	@Override
	public void read2(Read2Request request, StreamObserver<AckResponse> responseObserver) {
		System.out.println("trying to read2 " + request.getRegister());
		reg reg1 = registers.get(request.getRegister());
		if (registers.containsKey(request.getRegister())) {
			if (request.getTimestamp() > reg1.timestamp) {
				registers.put(request.getRegister(), new reg(request.getValue(), request.getTimestamp()));
				update(request.getRegister(), request.getValue(), request.getTimestamp());
			}
		} else {
			registers.put(request.getRegister(), new reg(request.getValue(), request.getTimestamp()));
			insert(request.getRegister(), request.getValue(), request.getTimestamp());
		}
		AckResponse ackresponse = AckResponse.newBuilder().build();
		responseObserver.onNext(ackresponse);
		responseObserver.onCompleted();

	}

	@Override
	public void write(WriteRequest request, StreamObserver<AckResponse> responseObserver) {
		System.out.println("trying to write " + request.getRegister());
		// registers.put("1", new reg("5", 1000));
		if (!registers.containsKey(request.getRegister())) {
			registers.put(request.getRegister(), new reg(request.getValue(), request.getTimestampe()));
			insert(request.getRegister(), request.getValue(), request.getTimestampe());
		} else {
			registers.put(request.getRegister(), new reg(request.getValue(), request.getTimestampe()));
			update(request.getRegister(), request.getValue(), request.getTimestampe());

		}
		AckResponse ackresponse1 = AckResponse.newBuilder().build();
		responseObserver.onNext(ackresponse1);
		responseObserver.onCompleted();
	}

	public static void createNewTable() {
		String url = "jdbc:sqlite:DC.db";
		try {
			Connection connection = DriverManager.getConnection(url);
			if (connection != null) {
				DatabaseMetaData meta = connection.getMetaData();
				System.out.println("A new database has been created.");
			}

		} catch (SQLException e) {
			System.out.println(e.getMessage());
		}

		String sql = "CREATE TABLE IF NOT EXISTS register (\n" + "	id VARCHAR(20) PRIMARY KEY NOT NULL,\n"
				+ "	value VARCHAR2 ,\n" + "	timestmp double \n" + ");";

		try {
			Connection connection = DriverManager.getConnection(url);
			Statement statement = connection.createStatement();
			statement.execute(sql);
			System.out.println("Database table has been created.");
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		}
	}

	public void insert(String id, String value, double timestmp) {
		String sql = "INSERT INTO register VALUES(?,?, ?)";
		System.out.println("In INSERT");
		try {
			String url = "jdbc:sqlite:DC.db";
			Connection connection = null;
			try {
				connection = DriverManager.getConnection(url);
			} catch (SQLException e) {
				System.out.println(e.getMessage());
			}
			PreparedStatement statement = connection.prepareStatement(sql);
			statement.setString(1, id);
			statement.setString(2, value);
			statement.setDouble(3, timestmp);
			statement.executeUpdate();
			System.out.println("INSERT FINISHED");

		} catch (SQLException e) {
			System.out.println(e.getMessage());
		}
	}

	public void update(String id, String value, double timestmp) {
		String sql = "UPDATE register set value = ?, timestmp = ? where id = ?";
		System.out.println("IN UPDATE");

		try {
			String url = "jdbc:sqlite:DC.db";
			Connection connection = null;
			try {
				connection = DriverManager.getConnection(url);
			} catch (SQLException e) {
				System.out.println(e.getMessage());
			}
			PreparedStatement statement = connection.prepareStatement(sql);
			statement.setString(1, value);
			statement.setDouble(2, timestmp);
			statement.setString(3, id);
			statement.executeUpdate();
			System.out.println("UPDATE FINISHED");
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		}
	}

	public String fetch(String id) {
		String sql = "SELECT value from register where id = ?";
		String value = " ";
		System.out.println("IN FETCH");
		try {
			String url = "jdbc:sqlite:DC.db";
			Connection connection = null;
			try {
				connection = DriverManager.getConnection(url);
			} catch (SQLException e) {
				System.out.println(e.getMessage());
			}
			PreparedStatement statement = connection.prepareStatement(sql);
			statement.setString(1, id);
			ResultSet rs = statement.executeQuery();
			while (rs.next()) {
				value = rs.getString("value");
			}
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		}
		System.out.println("FETCH FINISHED");

		return value;
	}

	public static void main(String args[]) throws Exception {
		ABDServer server = new ABDServer(2222);
		createNewTable();
		server.server.start();
		server.server.awaitTermination();
	}
}
