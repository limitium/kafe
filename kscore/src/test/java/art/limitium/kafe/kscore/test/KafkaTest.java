package art.limitium.kafe.kscore.test;

import art.limitium.kafe.kscore.KStreamApplication;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.annotation.AliasFor;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.support.DependencyInjectionTestExecutionListener;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;

@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE, ElementType.METHOD, ElementType.ANNOTATION_TYPE})
@SpringBootTest
@EmbeddedKafka(
        bootstrapServersProperty = "spring.kafka.bootstrap-servers",
        brokerProperties = {
        "offsets.topic.replication.factor=1",
        "transaction.state.log.replication.factor=1",
        "transaction.state.log.min.isr=1"
})
@TestPropertySource(properties = {"kafka.bootstrap.servers=${spring.kafka.bootstrap-servers}"})
@TestExecutionListeners(
        listeners = {
                DependencyInjectionTestExecutionListener.class,
                BaseKStreamApplicationTests.CustomExecutionListener.class,
                DirtiesContextTestExecutionListener.class
        })
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
public @interface KafkaTest {
    @AliasFor(annotation = EmbeddedKafka.class, attribute = "topics")
    String[] topics() default "";

    @AliasFor(annotation = EmbeddedKafka.class, attribute = "partitions")
    int partitions() default 2;

    String[] consumers() default "";

    @AliasFor(annotation = SpringBootTest.class, attribute = "classes")
    Class<?>[] configs() default {
            KStreamApplication.class, BaseKStreamApplicationTests.BaseKafkaTestConfig.class
    };

    @AliasFor(annotation = EmbeddedKafka.class, attribute = "adminTimeout")
    int adminTimeout() default EmbeddedKafkaBroker.DEFAULT_ADMIN_TIMEOUT * 3;
}
