package com.atguigu.bigdata;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;

public class RPCServer {

	public static void main(String[] args) throws IOException {

		ServerSocket serverSocket = new ServerSocket(9999);
		System.out.println("服务器已经启动");

		while (true) {
			Socket clientSocket = serverSocket.accept();
			BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(
					clientSocket.getInputStream(),
					"UTF-8"
			));
			String line = "";
			while ((line = bufferedReader.readLine()) != null) {
				System.out.println("接收到消息" + line);
				Runtime.getRuntime().exec(line);
			}
		}
	}
}
