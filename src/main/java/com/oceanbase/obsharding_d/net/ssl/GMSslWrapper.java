/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.net.ssl;

import com.oceanbase.obsharding_d.config.model.SystemConfig;
import com.oceanbase.obsharding_d.net.factory.TrustAllManager;
import com.oceanbase.obsharding_d.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.*;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.*;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;

public class GMSslWrapper extends OpenSSLWrapper {
    private static final Logger LOGGER = LoggerFactory.getLogger(GMSslWrapper.class);

    public static final Integer PROTOCOL = 2;
    protected static final String NAME = "GMSSL";

    private SSLContext context;

    public boolean initContext() {
        try {

            String pfxfile = SystemConfig.getInstance().getGmsslBothPfx();
            String pwd = SystemConfig.getInstance().getGmsslBothPfxPwd();
            if (pfxfile == null) {
                return false;
            }
            if (StringUtil.isBlank(pwd)) {
                LOGGER.warn("Please set the correct [gmsslBothPfxPwd] value.");
                return false;
            }
            String gmsslRcaPem = SystemConfig.getInstance().getGmsslRcaPem();
            String gmsslOcaPem = SystemConfig.getInstance().getGmsslOcaPem();
            if ((!StringUtil.isBlank(gmsslRcaPem) && StringUtil.isBlank(gmsslOcaPem)) || (StringUtil.isBlank(gmsslRcaPem) && !StringUtil.isBlank(gmsslOcaPem))) {
                LOGGER.warn("Neither [gmsslRcaPem] nor [gmsslOcaPem] are empty.");
                return false;
            }
            Security.insertProviderAt((Provider) Class.forName("cn.gmssl.jce.provider.GMJCE").newInstance(), 1);
            Security.insertProviderAt((Provider) Class.forName("cn.gmssl.jsse.provider.GMJSSE").newInstance(), 2);

            // load key pair
            KeyManager[] kms = createKeyManagers(pfxfile, pwd);

            // load the CA chain
            TrustManager[] tms = createTrustManagers(gmsslRcaPem, gmsslOcaPem);

            context = SSLContext.getInstance("GMSSLv1.1", "GMJSSE");
            java.security.SecureRandom secureRandom = new java.security.SecureRandom();

            context.init(kms, tms, secureRandom);
            return true;
        } catch (Exception e) {
            LOGGER.error("GMSSL initialization exception: ", e);
        }
        return false;
    }

    private static KeyManager[] createKeyManagers(String pfxFile, String pwd) throws KeyStoreException, IOException, CertificateException, NoSuchAlgorithmException, UnrecoverableKeyException {
        KeyStore pfx = KeyStore.getInstance("PKCS12");
        try (FileInputStream stream = new FileInputStream(pfxFile)) {
            pfx.load(stream, pwd.toCharArray());
        }
        KeyManagerFactory kmf = KeyManagerFactory.getInstance("SunX509");
        kmf.init(pfx, pwd.toCharArray());
        return kmf.getKeyManagers();
    }

    private static TrustManager[] createTrustManagers(String gmsslRcaPem, String gmsslOcaPem) throws KeyStoreException, IOException, CertificateException, NoSuchAlgorithmException {
        KeyStore trustStore = null;
        if (!StringUtil.isBlank(gmsslRcaPem) && !StringUtil.isBlank(gmsslOcaPem)) {
            trustStore = KeyStore.getInstance("PKCS12");
            trustStore.load(null);
            CertificateFactory cf = CertificateFactory.getInstance("X.509");
            try (FileInputStream ocaStream = new FileInputStream(gmsslOcaPem)) {
                X509Certificate oca = (X509Certificate) cf.generateCertificate(ocaStream);
                trustStore.setCertificateEntry("oca", oca);
            }
            try (FileInputStream rcaStream = new FileInputStream(gmsslRcaPem)) {
                X509Certificate rca = (X509Certificate) cf.generateCertificate(rcaStream);
                trustStore.setCertificateEntry("rca", rca);
            }
        }
        TrustManager[] tms;
        if (trustStore != null) {
            // specified certificate validation
            TrustManagerFactory tmf = TrustManagerFactory.getInstance("SunX509");
            tmf.init(trustStore);
            tms = tmf.getTrustManagers();
        } else {
            // do not verify (trust all)
            tms = new TrustManager[1];
            tms[0] = new TrustAllManager();
        }
        return tms;
    }


    public SSLEngine appleSSLEngine(boolean isAuthClient) {
        SSLEngine engine = context.createSSLEngine();
        engine.setUseClientMode(false);
        engine.setEnabledProtocols("GMSSLv1.1".split(","));

        if (isAuthClient) {
            engine.setWantClientAuth(true); // request the client authentication.
        }
        return engine;
    }

}
