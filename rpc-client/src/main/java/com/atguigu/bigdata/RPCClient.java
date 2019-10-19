package com.atguigu.bigdata;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.Socket;

public class RPCClient {

	public static void main(String[] args) throws IOException {
		Socket localhost = new Socket("localhost", 9999);
		PrintWriter printWriter = new PrintWriter(new OutputStreamWriter(
				localhost.getOutputStream(),
				"UTF-8"
		));

		printWriter.println("cmd /c notepad");
		printWriter.flush();
		System.out.println("客户端指令发送成功");
		localhost.close();

	}

}
