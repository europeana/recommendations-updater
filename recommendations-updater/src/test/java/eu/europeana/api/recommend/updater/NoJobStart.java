package eu.europeana.api.recommend.updater;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.test.context.SpringBootContextLoader;
import org.springframework.boot.test.context.SpringBootTestContextBootstrapper;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.test.context.ContextLoader;

/**
 * Bootstrapper to set command line parameters and prevent the Spring batch job from starting automatically
 */
public class NoJobStart extends SpringBootTestContextBootstrapper {

    @Override
    protected Class<? extends ContextLoader> getDefaultContextLoaderClass(Class<?> testClass) {
        return ArgumentSupplyingContextLoader.class;
    }

    static class ArgumentSupplyingContextLoader extends SpringBootContextLoader {
        @Override
        protected SpringApplication getSpringApplication() {
            return new SpringApplication() {
                @Override
                public ConfigurableApplicationContext run(String... args) {
                    // we also provide "test" parameter so no jobs are started
                    return super.run("--FULL", "test");
                }
            };
        }
    }

}
