package pl.tomaszdziurko.simple_worker

import groovy.transform.TypeChecked
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.RetryNTimes
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder
import org.apache.curator.x.discovery.ServiceInstance
import org.apache.curator.x.discovery.UriSpec
import org.jboss.resteasy.plugins.server.undertow.UndertowJaxrsServer

import javax.ws.rs.ApplicationPath
import javax.ws.rs.GET
import javax.ws.rs.Path
import javax.ws.rs.core.Application


@TypeChecked
@Path("/")
class Main {

    private static UndertowJaxrsServer server
    static private String workerName
    static private int workerPort

    static final void main(String[] args) {

        if (args.size() != 2) {
            throw new IllegalArgumentException("Invalid arguments")
        }

        workerName = args[0]
        workerPort = args[1].toInteger()

        startRestServer(workerPort)
        registerInZookeeper(workerPort)

        println ("Worker ${workerName} started on port $workerPort")
    }

    private static void startRestServer(int port) {
        System.setProperty("org.jboss.resteasy.port", "$port")
        server = new UndertowJaxrsServer().start()
        server.deploy(RestApp)
    }


    private static void registerInZookeeper(int port) {
        CuratorFramework curatorFramework = CuratorFrameworkFactory.newClient("localhost:2181", new RetryNTimes(5, 1000))
        curatorFramework.start()
        ServiceInstance<Void> serviceInstance = ServiceInstance.builder()
                .uriSpec(new UriSpec("{scheme}://{address}:{port}"))
                .address('localhost')
                .port(port)
                .name("worker")
                .build()

        ServiceDiscoveryBuilder.builder(Void)
                .basePath("load-balancing-example")
                .client(curatorFramework)
                .thisInstance(serviceInstance)
                .build()
                .start()
    }

    @GET
    @Path("/work")
    public String work() {
        String response = "Work done by $workerName"
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

    static class CustomPayload {
        String name;
        String speed;
        String location

        CustomPayload(String name, String speed, String location) {
            this.name = name
            this.speed = speed
            this.location = location
        }
    }

}
