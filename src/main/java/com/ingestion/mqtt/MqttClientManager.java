package com.ingestion.mqtt;

import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.openssl.PEMKeyPair;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.FileReader;
import java.security.KeyPair;
import java.security.KeyStore;
import java.security.cert.X509Certificate;

/**
 * MqttClientManager
 * ─────────────────
 * Owns the single shared MqttClient connection to the broker.
 *
 * Connects using mutual TLS (mTLS) as required by AWS IoT Core:
 *  - TrustStore : Amazon Root CA  (ca-cert-path)
 *  - KeyStore   : device cert + private key  (client-cert-path / client-key-path)
 *
 * Exposes publish() and getClient() for other components.
 */
@Slf4j
@Component
public class MqttClientManager {

    @Value("${mqtt.broker-url}")
    private String brokerUrl;

    @Value("${mqtt.client-id}")
    private String clientId;

    @Value("${mqtt.ca-cert-path}")
    private String caCertPath;

    @Value("${mqtt.client-cert-path}")
    private String clientCertPath;

    @Value("${mqtt.client-key-path}")
    private String clientKeyPath;

    private MqttClient mqttClient;

    @PostConstruct
    public void connect() throws Exception {
        mqttClient = new MqttClient(brokerUrl, clientId, new MemoryPersistence());

        MqttConnectOptions options = new MqttConnectOptions();
        options.setCleanSession(true);
        options.setAutomaticReconnect(true);   // reconnect automatically on drop
        options.setKeepAliveInterval(60);       // send PINGREQ every 60s to keep connection alive
        options.setSocketFactory(buildMtlsSocketFactory());

        mqttClient.connect(options);
        log.info("MQTT client connected to broker: {}", brokerUrl);
    }

    public void publish(String topic, String payload) throws MqttException {
        MqttMessage message = new MqttMessage(payload.getBytes());
        message.setQos(1);
        mqttClient.publish(topic, message);
        log.info("MQTT message published | topic={} | payload={}", topic, payload);
    }

    public MqttClient getClient() {
        return mqttClient;
    }

    // ─── mTLS helper ─────────────────────────────────────────────────────────

    /**
     * Builds an SSLSocketFactory for mutual TLS using PEM files.
     *
     * TrustStore: loaded with the Amazon Root CA so we trust the broker.
     * KeyStore  : loaded with the device certificate + private key so the
     *             broker can authenticate this client.
     */
    private javax.net.ssl.SSLSocketFactory buildMtlsSocketFactory() throws Exception {

        // ── 1. Load CA cert into TrustStore ──────────────────────────────────
        X509Certificate caCert = loadCertificate(caCertPath);

        KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
        trustStore.load(null, null);
        trustStore.setCertificateEntry("ca-cert", caCert);

        TrustManagerFactory tmf = TrustManagerFactory.getInstance(
                TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(trustStore);

        // ── 2. Load client cert + private key into KeyStore ──────────────────
        X509Certificate clientCert = loadCertificate(clientCertPath);
        KeyPair keyPair = loadKeyPair(clientKeyPath);

        KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
        keyStore.load(null, null);
        keyStore.setCertificateEntry("client-cert", clientCert);
        keyStore.setKeyEntry("private-key",
                keyPair.getPrivate(),
                new char[0],                          // no password on the entry
                new java.security.cert.Certificate[]{clientCert});

        KeyManagerFactory kmf = KeyManagerFactory.getInstance(
                KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(keyStore, new char[0]);

        // ── 3. Build SSLContext with both trust + key managers ────────────────
        SSLContext sslContext = SSLContext.getInstance("TLSv1.2");
        sslContext.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);

        return sslContext.getSocketFactory();
    }

    /** Reads a PEM file and returns the first X.509 certificate found. */
    private X509Certificate loadCertificate(String path) throws Exception {
        try (PEMParser parser = new PEMParser(new FileReader(path))) {
            Object obj = parser.readObject();
            return new JcaX509CertificateConverter().getCertificate(
                    (X509CertificateHolder) obj);
        }
    }

    /** Reads a PEM file and returns the RSA/EC key pair. */
    private KeyPair loadKeyPair(String path) throws Exception {
        try (PEMParser parser = new PEMParser(new FileReader(path))) {
            Object obj = parser.readObject();
            return new JcaPEMKeyConverter().getKeyPair((PEMKeyPair) obj);
        }
    }
}
