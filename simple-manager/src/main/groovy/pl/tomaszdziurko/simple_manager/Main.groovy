package pl.tomaszdziurko.simple_manager

import groovy.transform.TypeChecked
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.RetryNTimes
import org.apache.curator.x.discovery.ServiceDiscovery
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder
import org.apache.curator.x.discovery.ServiceProvider
import org.jboss.resteasy.plugins.server.undertow.UndertowJaxrsServer

import javax.ws.rs.ApplicationPath
import javax.ws.rs.GET
import javax.ws.rs.Path
import javax.ws.rs.core.Application

@TypeChecked
@Path("/")
class Main {

    private static UndertowJaxrsServer server
    static private int managerPort

    static ServiceProvider serviceProvider

    static final void main(String[] args) {

        if (args.size() != 1) {
            throw new IllegalArgumentException("Invalid arguments")
        }

        managerPort = args[0].toInteger()

        startRestServer(managerPort)

        CuratorFramework curatorFramework = CuratorFrameworkFactory.newClient("localhost:2181", new RetryNTimes(5, 1000))
        curatorFramework.start()
        ServiceDiscovery<Void> serviceDiscovery = ServiceDiscoveryBuilder
                .builder(Void)
                .basePath("load-balancing-example")
                .client(curatorFramework).build()
        serviceDiscovery.start()
        serviceProvider = serviceDiscovery
                .serviceProviderBuilder()
                .serviceName("worker")
                .build()
        serviceProvider.start()

        println ("Manager started on port $managerPort")
    }

    private static void startRestServer(int port) {
        System.setProperty("org.jboss.resteasy.port", "$port")
        server = new UndertowJaxrsServer().start()
        server.deploy(RestApp)
    }

    @GET
    @Path("/delegate")
    public String delegate() {
        def instance = serviceProvider.getInstance()
        String address = instance.buildUriSpec()
        String response = (address + "/work").toURL().getText()
        println response
        return response
    }

    @ApplicationPath("/")
    static class RestApp extends Application {
        @Override
        public Set<Class<?>> getClasses() {
            return [Main] as Set
        }
    }

}
