package art.limitium.kafe.kscore;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = {"art.limitium.kafe"})
public class KStreamApplication {
    public static void main(String[] args) {
        SpringApplication.run(KStreamApplication.class, args);
    }

}