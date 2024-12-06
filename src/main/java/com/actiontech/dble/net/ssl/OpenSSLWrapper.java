/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.net.ssl;

import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.*;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;

public class OpenSSLWrapper implements IOpenSSLWrapper {
    private static final Logger LOGGER = LoggerFactory.getLogger(OpenSSLWrapper.class);

    private static final String PROTO = "TLS";

    private SSLContext clientContext;
    private SSLContext serverContext;

    public static final Integer PROTOCOL = 1;
    protected static final String NAME = "OpenSSL";

    @Override
    public boolean initContext() {
        final boolean a = initClientContext();
        final boolean b = initServerContext();
        return a || b;
    }

    private boolean initServerContext() {
        String serverCertificateKeyStoreUrl = SystemConfig.getInstance().getServerCertificateKeyStoreUrl();
        String serverCertificateKeyStorePwd = SystemConfig.getInstance().getServerCertificateKeyStorePwd();
        String trustCertificateKeyStoreUrl = SystemConfig.getInstance().getTrustCertificateKeyStoreUrl();
        String trustCertificateKeyStorePwd = SystemConfig.getInstance().getTrustCertificateKeyStorePwd();
        try {
            if (serverCertificateKeyStoreUrl == null) {
                return false;
            }

            if (StringUtil.isBlank(serverCertificateKeyStorePwd)) {
                LOGGER.warn("Please set the correct [serverCertificateKeyStoreUrl] value.");
                return false;
            }

            if (!StringUtil.isBlank(trustCertificateKeyStoreUrl) && StringUtil.isBlank(trustCertificateKeyStorePwd)) {
                LOGGER.warn("Please set the correct [trustCertificateKeyStoreUrl] value.");
                return false;
            }

            serverContext = SSLContext.getInstance(PROTO);

            KeyManager[] keyM = createKeyManagers(serverCertificateKeyStoreUrl, serverCertificateKeyStorePwd);
            TrustManager[] trustM = StringUtil.isBlank(trustCertificateKeyStoreUrl) ? null : createTrustManagers(trustCertificateKeyStoreUrl, trustCertificateKeyStorePwd);

            serverContext.init(keyM, trustM, null);
            return true;
        } catch (Exception e) {
            LOGGER.error("OpenSSL initialization exception: ", e);
        }
        return false;
    }

    private boolean initClientContext() {
        final String clientCertificateKeyStoreUrl = SystemConfig.getInstance().getClientCertificateKeyStoreUrl();
        final String clientCertificateKeyStorePwd = SystemConfig.getInstance().getClientCertificateKeyStorePwd();
        String trustCertificateKeyStoreUrl = SystemConfig.getInstance().getTrustCertificateKeyStoreUrl();
        String trustCertificateKeyStorePwd = SystemConfig.getInstance().getTrustCertificateKeyStorePwd();
        try {

            if (!StringUtil.isBlank(trustCertificateKeyStoreUrl) && StringUtil.isBlank(trustCertificateKeyStorePwd)) {
                LOGGER.warn("Please set the correct [trustCertificateKeyStoreUrl] value.");
                return false;
            }

            clientContext = SSLContext.getInstance(PROTO);


            TrustManager[] trustM = StringUtil.isBlank(trustCertificateKeyStoreUrl) ? null : createTrustManagers(trustCertificateKeyStoreUrl, trustCertificateKeyStorePwd);

            if (StringUtil.isBlank(clientCertificateKeyStorePwd) && StringUtil.isBlank(clientCertificateKeyStoreUrl)) {
                LOGGER.warn("doesn't detect client Certificate for server ssl, use One-way Authentication instead.");
                clientContext.init(null, trustM, null);
            } else {
                KeyManager[] keyM = createKeyManagers(clientCertificateKeyStoreUrl, clientCertificateKeyStorePwd);
                clientContext.init(keyM, trustM, null);
            }

            return true;
        } catch (Exception e) {
            LOGGER.error("OpenSSL initialization exception: ", e);
        }
        return false;
    }



    private static KeyManager[] createKeyManagers(String filepath, String keystorePassword) throws KeyStoreException, IOException, CertificateException, NoSuchAlgorithmException, UnrecoverableKeyException {
        KeyStore keyStore = KeyStore.getInstance("JKS");
        InputStream keyStoreIS = new FileInputStream(filepath);
        try {
            keyStore.load(keyStoreIS, keystorePassword.toCharArray());
        } finally {
            keyStoreIS.close();
        }
        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(keyStore, keystorePassword.toCharArray());
        return kmf.getKeyManagers();
    }

    private static TrustManager[] createTrustManagers(String filepath, String keystorePassword) throws KeyStoreException, IOException, CertificateException, NoSuchAlgorithmException {
        KeyStore trustStore = KeyStore.getInstance("JKS");
        InputStream trustStoreIS = new FileInputStream(filepath);
        try {
            trustStore.load(trustStoreIS, keystorePassword.toCharArray());
        } finally {
            trustStoreIS.close();
        }
        TrustManagerFactory trustFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trustFactory.init(trustStore);
        return trustFactory.getTrustManagers();
    }

    @Override
    public SSLEngine createServerSSLEngine(boolean isAuthClient) {
        SSLEngine engine = serverContext.createSSLEngine();
        engine.setUseClientMode(false);

        //        engine.setEnabledCipherSuites(serverContext.getServerSocketFactory().getSupportedCipherSuites());

        engine.setEnabledProtocols(new String[]{"TLSv1.1", "TLSv1.2"});
        if (isAuthClient) {
            engine.setWantClientAuth(true); // request the client authentication.
            // engine.setNeedClientAuth(true);  // require client authentication.
        }
        return engine;
    }

    @Override
    public SSLEngine createClientSSLEngine() {
        SSLEngine engine = clientContext.createSSLEngine();
        engine.setUseClientMode(true);
        engine.setEnabledProtocols(new String[]{"TLSv1.1", "TLSv1.2"});

        /*engine.setEnabledCipherSuites(context.getServerSocketFactory().getSupportedCipherSuites());
        engine.setEnabledProtocols(new String[]{"TLSv1.1","TLSv1.2"});*/

        return engine;
    }
}
