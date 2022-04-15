package com.actiontech.dble.services.factorys;

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

public final class SSLEngineFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(SSLEngineFactory.class);

    private static final SSLEngineFactory INSTANCE = new SSLEngineFactory();
    private static final String PROTO = "TLS";

    private boolean init = false;

    private SSLContext context;
    private String sslCertificate;
    private String sslCertificatePwd;
    private String sslTrust;
    private String sslTrustPwd;

    public static SSLEngineFactory getInstance() {
        return INSTANCE;
    }

    private SSLEngineFactory() {
    }

    public boolean isInit() {
        return init;
    }

    public static void init() {
        initSSLContext();
    }

    private static void initSSLContext() {
        INSTANCE.sslCertificate = SystemConfig.getInstance().getSslCertificate();
        INSTANCE.sslCertificatePwd = SystemConfig.getInstance().getSslCertificatePwd();
        INSTANCE.sslTrust = SystemConfig.getInstance().getSslTrust();
        INSTANCE.sslTrustPwd = SystemConfig.getInstance().getSslTrustPwd();
        try {
            if (INSTANCE.sslCertificate == null) {
                return;
            }

            if (StringUtil.isBlank(INSTANCE.sslCertificatePwd)) {
                LOGGER.warn("Please set the correct [sslCertificatePwd] value.");
                return;
            }

            if (!StringUtil.isBlank(INSTANCE.sslTrust) && StringUtil.isBlank(INSTANCE.sslTrustPwd)) {
                LOGGER.warn("Please set the correct [sslTrustPwd] value.");
                return;
            }

            INSTANCE.context = SSLContext.getInstance(PROTO);

            KeyManager[] keyM = createKeyManagers(INSTANCE.sslCertificate, INSTANCE.sslCertificatePwd);
            TrustManager[] trustM = StringUtil.isBlank(INSTANCE.sslTrust) ? null : createTrustManagers(INSTANCE.sslTrust, INSTANCE.sslTrustPwd);

            INSTANCE.context.init(keyM, trustM, null);

            INSTANCE.init = true;
        } catch (Exception e) {
            LOGGER.error("SSLEngineFactory initialization exception: {}", e);
        } finally {
            if (INSTANCE.init) {
                LOGGER.info("===========================================init SSLEngineFactory finish=================================");
            }
        }
    }

    public static SSLEngine appleSSLEngine(boolean isAuthClient) {
        SSLEngine engine = INSTANCE.context.createSSLEngine();
        engine.setUseClientMode(false);

        /*engine.setEnabledCipherSuites(INSTANCE.context.getServerSocketFactory().getSupportedCipherSuites());
        engine.setEnabledProtocols(new String[]{"TLSv1.1","TLSv1.2"});*/

        if (isAuthClient) {
            engine.setWantClientAuth(true); // request the client authentication.
            // engine.setNeedClientAuth(true);  // require client authentication.
        }
        return engine;
    }

    private static KeyManager[] createKeyManagers(String filepath, String keystorePassword) throws KeyStoreException, IOException, CertificateException, NoSuchAlgorithmException, UnrecoverableKeyException {
        KeyStore keyStore = KeyStore.getInstance("JKS");
        InputStream keyStoreIS = new FileInputStream(filepath);
        try {
            keyStore.load(keyStoreIS, keystorePassword.toCharArray());
        } finally {
            if (keyStoreIS != null) {
                keyStoreIS.close();
            }
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
            if (trustStoreIS != null) {
                trustStoreIS.close();
            }
        }
        TrustManagerFactory trustFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trustFactory.init(trustStore);
        return trustFactory.getTrustManagers();
    }
}
