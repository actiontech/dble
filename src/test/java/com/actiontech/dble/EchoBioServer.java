/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * @author mycat
 */
public class EchoBioServer implements Runnable {

    private static final byte[] FIRST_BYTES = "Welcome to Server.".getBytes();

    private final ServerSocket serverSocket;

    public EchoBioServer(int port) throws IOException {
        serverSocket = new ServerSocket(port);
    }

    @Override
    public void run() {
        while (true) {
            try {
                Socket socket = serverSocket.accept();
                new Thread(new BioConnection(socket)).start();
            } catch (IOException e) {

                e.printStackTrace();
            }
        }
    }

    private class BioConnection implements Runnable {

        private Socket socket;
        private InputStream input;
        private OutputStream output;
        private byte[] readBuffer;
        private byte[] writeBuffer;

        private BioConnection(Socket socket) throws IOException {
            this.socket = socket;
            this.input = socket.getInputStream();
            this.output = socket.getOutputStream();
            this.readBuffer = new byte[4096];
            this.writeBuffer = new byte[4096];
        }

        @Override
        public void run() {
            try {
                output.write(FIRST_BYTES);
                output.flush();
                while (true) {
                    int got = input.read(readBuffer);
                    output.write(writeBuffer, 0, got);
                    // output.flush();
                }
            } catch (IOException e) {

                e.printStackTrace();
                if (socket != null) {
                    try {
                        socket.close();
                    } catch (IOException e1) {

                        e1.printStackTrace();
                    }
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        new Thread(new EchoBioServer(8066)).start();
    }

}