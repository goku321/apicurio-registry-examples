package io.apicurio.registry.examples.simple.json;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.apicurio.registry.resolver.SchemaResolverConfig;
import io.apicurio.registry.rest.client.RegistryClient;
import io.apicurio.registry.rest.client.RegistryClientFactory;
import io.apicurio.registry.rest.v2.beans.IfExists;
import io.apicurio.registry.serde.SchemaResolverConfigurer;
import io.apicurio.registry.serde.SerdeConfig;
import io.apicurio.registry.serde.jsonschema.JSONSchemaNATSDeserializer;
import io.apicurio.registry.serde.jsonschema.JsonSchema;
import io.apicurio.registry.serde.strategy.SimpleTopicIdStrategy;
import io.apicurio.registry.serde.strategy.SubjectNameStrategy;
import io.apicurio.registry.types.ArtifactType;
import io.nats.client.AuthHandler;
import io.nats.client.Connection;
import io.nats.client.JSONSchema;
import io.nats.client.JetStream;
import io.nats.client.JetStreamManagement;
import io.nats.client.Message;
import io.nats.client.PublishOptions;
import io.nats.client.Subscription;
import io.nats.client.api.PublishAck;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.impl.NatsMessage;
import java.io.ByteArrayInputStream;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import io.nats.client.Nats;
import io.apicurio.registry.serde.jsonschema.JSONSchemaNATSSerializer;
import java.util.HashMap;
import java.util.Map;

class User implements Serializable {

    public String firstName;

    public String lastName;

    public int age;

    public String getFirstName() {
        return firstName;
    }

    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public User() {}

    public User(String firstName, String lastName, int age) {
        this(firstName, lastName, age, null);
    }

    public User(String firstName, String lastName, int age, Object o) {
        this.firstName = firstName;
        this.lastName = lastName;
        this.age = age;
    }
}

public class NATSExample {
    private static final String REGISTRY_URL = "http://localhost:8080/apis/registry/v2";
//    private static final String SERVERS = "localhost:9092";
    private static final String TOPIC_NAME = NATSExample.class.getSimpleName();
    private static final String SUBJECT_NAME = "subject-2";
    private static Connection nc;
    private static JetStream js;
    public static final String SCHEMA = "{" +
            "    \"$id\": \"https://example.com/message.schema.json\"," +
            "    \"$schema\": \"http://json-schema.org/draft-07/schema#\"," +
            "    \"required\": [" +
            "        \"firstName\"," +
            "        \"age\"" +
            "    ]," +
            "    \"type\": \"object\"," +
            "    \"properties\": {" +
            "        \"firstName\": {" +
            "            \"description\": \"\"," +
            "            \"type\": \"string\"" +
            "        }," +
            "        \"age\": {" +
            "            \"description\": \"\"," +
            "            \"type\": \"number\"" +
            "        }" +
            "    }" +
            "}";

    public static void main(String []args) throws Exception {
        System.out.println("Starting example " + NATSExample.class.getSimpleName());
//        String topicName = TOPIC_NAME;
        String subjectName = SUBJECT_NAME;

        // Register the schema with the registry (only if it is not already registered)
        String artifactId = SUBJECT_NAME; // use the subject name as the artifactId because we're going to map subject name to artifactId later on.
        RegistryClient client = RegistryClientFactory.create(REGISTRY_URL);
        client.createArtifact("default", artifactId, ArtifactType.JSON, IfExists.RETURN_OR_UPDATE, new ByteArrayInputStream(SCHEMA.getBytes(
                StandardCharsets.UTF_8)));

        // Connect to NATS
        try {
            AuthHandler authHandler = Nats.credentials("/Users/deepak.sah/Code/go/src/github.com/goku321/nsc-wrapper/src/user.creds");
            nc = Nats.connect("nats://localhost:4222", authHandler);
        } catch(Exception e) {
            e.printStackTrace();
        }

        try {
            js = nc.jetStream();
        } catch(Exception e) {
            e.printStackTrace();
        }

        StreamConfiguration sc = StreamConfiguration.builder()
                .name("schema-registry-2")
                .storageType(StorageType.File)
                .subjects(SUBJECT_NAME).build();

        JetStreamManagement jsm = nc.jetStreamManagement();
        jsm.addStream(sc);

        User user = new User("first", "last", 27);
        SchemaResolverConfigurer<JsonSchema, Object> resolver = new SchemaResolverConfigurer<JsonSchema, Object>(client);
        JSONSchemaNATSSerializer<?> jsonSchema = new JSONSchemaNATSSerializer<>(resolver.getSchemaResolver());
        Map<String, Object> config = new HashMap<>();
        config.put(SerdeConfig.EXPLICIT_ARTIFACT_GROUP_ID, "default");
        config.put(SerdeConfig.ARTIFACT_RESOLVER_STRATEGY, SubjectNameStrategy.class.getName());
        config.put(SerdeConfig.REGISTRY_URL, REGISTRY_URL);
        config.put(SerdeConfig.FALLBACK_ARTIFACT_GROUP_ID, "default");
        config.put(SerdeConfig.FALLBACK_ARTIFACT_ID, "subject");
        config.put(SerdeConfig.FALLBACK_ARTIFACT_VERSION, "1");
        config.put(SerdeConfig.DESERIALIZER_SPECIFIC_KEY_RETURN_CLASS, User.class.getName());
//        config.put(SchemaResolverConfig.REGISTRY_URL, REGISTRY_URL);
        jsonSchema.configure(config, false);
        Message msg = NatsMessage.builder().subject(SUBJECT_NAME).schema(jsonSchema).value(user).build();
        PublishAck pa = js.publish(msg);
        System.out.println(pa);

        // Subscribe and Deserialize
        JSONSchemaNATSDeserializer<User> jsonDeser = new JSONSchemaNATSDeserializer(resolver.getSchemaResolver());
        jsonDeser.configure(config, false);
        Subscription sub = js.subscribe(SUBJECT_NAME);
        Message<User> receivedMsg = sub.nextMessage(1000);
        receivedMsg.setSchema(jsonDeser);
        User receivedUser = receivedMsg.getValue();
        System.out.println(receivedUser);

    }
}
