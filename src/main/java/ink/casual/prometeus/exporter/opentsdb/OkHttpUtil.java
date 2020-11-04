package ink.casual.prometeus.exporter.opentsdb;


import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.IOException;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.concurrent.TimeUnit;

public class OkHttpUtil {

    /**
     * 连接超时时间
     */
    public static final int CONNECT_TIMEOUT = 20;

    private static OkHttpClient client;

    static {
        OkHttpClient.Builder builder = new OkHttpClient.Builder();
        builder.connectTimeout(CONNECT_TIMEOUT, TimeUnit.SECONDS);
        builder.sslSocketFactory(createSSLSocketFactory(), new TrustAllCerts());
        builder.hostnameVerifier((hostname, session) -> true);
        client = builder.build();
    }

    /**
     * Get请求
     *
     * @param url
     * @return
     */
    public static String get(String url) throws IOException {
        if (!url.startsWith("http://")) {
            url = "http://".concat(url);
        }
        Response response = client.newCall(new Request.Builder().url(url).build()).execute();
        ResponseBody body = response.body();
        if (body != null) {
            return body.string();
        } else {
            return null;
        }
    }

    public static class TrustAllCerts implements X509TrustManager {
        @Override
        public void checkClientTrusted(X509Certificate[] chain, String authType) {
        }

        @Override
        public void checkServerTrusted(X509Certificate[] chain, String authType) {
        }

        @Override
        public X509Certificate[] getAcceptedIssuers() {
            return new X509Certificate[0];
        }
    }

    private static SSLSocketFactory createSSLSocketFactory() {
        SSLSocketFactory ssfFactory = null;
        try {
            SSLContext sc = SSLContext.getInstance("TLS");
            sc.init(null, new TrustManager[]{new TrustAllCerts()}, new SecureRandom());

            ssfFactory = sc.getSocketFactory();
        } catch (Exception e) {
        }
        return ssfFactory;
    }

}
