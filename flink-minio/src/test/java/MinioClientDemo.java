//import cn.hutool.core.io.FileUtil;
//import io.minio.GetObjectArgs;
//import io.minio.MinioClient;
//
//import java.io.File;
//import java.io.FileOutputStream;
//import java.io.InputStream;
//
//public class MinioClientDemo {
//
//    public static void main(String[] args) throws Exception {
//
//        String endpoint = "http://192.168.2.103:9000";
//        String accesskey = "minioadmin";
//        String secretkey = "minioadmin";
//
//        MinioClient minioClient = MinioClient.builder().endpoint(endpoint)
//                .credentials(accesskey, secretkey)
//                .build();
//
//        InputStream inputStream = minioClient.getObject(
//                GetObjectArgs.builder().bucket("flink").object("/checkpoint/4 个字节 137.txt").build()
//        );
//
//        File file = FileUtil.file(new File("jars"), "aa.txt");
//
//        FileOutputStream outputStream = new FileOutputStream(
//                file
//        );
//
//        byte[] buf = new byte[1024];
//        int length = 0;
//
//        while ((length = inputStream.read(buf)) > 0) {
//            outputStream.write(buf, 0, length);
//        }
//
//        inputStream.close();
//        outputStream.close();
//
//    }
//
//}
