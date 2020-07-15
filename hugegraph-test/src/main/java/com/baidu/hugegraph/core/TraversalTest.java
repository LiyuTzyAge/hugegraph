package com.baidu.hugegraph.core;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.schema.SchemaManager;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.List;

import static com.baidu.hugegraph.core.CoreTestSuite.graph;

/**
 *  @author: liyu04
 *  @date: 2020/7/7
 *  @version: V1.0
 *
 * @Description:
 */
public class TraversalTest
{
    public static void init()
    {
        CoreTestSuite.initEnv();
        CoreTestSuite.init();
//        CoreTestSuite.clear();
        initSchema();
        writeData();
    }

    public TraversalTest()
    {
        init();
    }

    public static void test1()
    {
        TraversalTest test = new TraversalTest();
        HugeGraph graph = graph();
//        List<Edge> edges = graph.traversal().E().toList();
        System.out.println("===============================================");
//        System.out.println(edges);
        GraphTraversalSource traversal = graph.traversal();
        GraphTraversal<Vertex, Vertex> v = traversal.V();
        GraphTraversal<Vertex, Vertex> out = v.out();
        List<Vertex> next = out.next(10);
        System.out.println(next);
        System.out.println("===============================================");
        graph.close();

    }

    public static void main(String[] args)
    {
        test1();
    }

    public static void initSchema() {
        SchemaManager schema = graph().schema();

//        LOG.debug("===============  propertyKey  ================");

        schema.propertyKey("id").asInt().create();
        schema.propertyKey("name").asText().create();
        schema.propertyKey("dynamic").asBoolean().create();
        schema.propertyKey("time").asText().create();
        schema.propertyKey("timestamp").asLong().create();
        schema.propertyKey("age").asInt().valueSingle().create();
        schema.propertyKey("comment").asText().valueSet().create();
        schema.propertyKey("contribution").asText().create();
        schema.propertyKey("score").asInt().create();
        schema.propertyKey("lived").asText().create();
        schema.propertyKey("city").asText().create();
        schema.propertyKey("amount").asFloat().create();
        schema.propertyKey("message").asText().create();
        schema.propertyKey("place").asText().create();
        schema.propertyKey("tool").asText().create();
        schema.propertyKey("reason").asText().create();
        schema.propertyKey("hurt").asBoolean().create();
        schema.propertyKey("arrested").asBoolean().create();
        schema.propertyKey("date").asDate().create();

//        LOG.debug("===============  vertexLabel  ================");

        schema.vertexLabel("person")
                .properties("name", "age", "city")
                .primaryKeys("name")
                .enableLabelIndex(false)
                .create();
        schema.vertexLabel("author")
                .properties("id", "name", "age", "lived")
                .primaryKeys("id")
                .enableLabelIndex(false)
                .create();
        schema.vertexLabel("language")
                .properties("name", "dynamic")
                .primaryKeys("name")
                .nullableKeys("dynamic")
                .enableLabelIndex(false)
                .create();
        schema.vertexLabel("book")
                .properties("name")
                .primaryKeys("name")
                .enableLabelIndex(false)
                .create();

//        LOG.debug("===============  edgeLabel  ================");

        schema.edgeLabel("transfer")
                .properties("id", "amount", "timestamp", "message")
                .nullableKeys("message")
                .multiTimes().sortKeys("id")
                .link("person", "person")
                .enableLabelIndex(false)
                .create();
        schema.edgeLabel("authored").singleTime()
                .properties("contribution", "comment", "score")
                .nullableKeys("score", "contribution", "comment")
                .link("author", "book")
                .enableLabelIndex(true)
                .create();
        schema.edgeLabel("write").properties("time")
                .multiTimes().sortKeys("time")
                .link("author", "book")
                .enableLabelIndex(false)
                .create();
        schema.edgeLabel("look").properties("time", "score")
                .nullableKeys("score")
                .multiTimes().sortKeys("time")
                .link("person", "book")
                .enableLabelIndex(true)
                .create();
        schema.edgeLabel("know").singleTime()
                .link("author", "author")
                .enableLabelIndex(true)
                .create();
        schema.edgeLabel("followedBy").singleTime()
                .link("author", "person")
                .enableLabelIndex(false)
                .create();
        schema.edgeLabel("friend").singleTime()
                .link("person", "person")
                .enableLabelIndex(true)
                .create();
        schema.edgeLabel("follow").singleTime()
                .link("person", "author")
                .enableLabelIndex(true)
                .create();
        schema.edgeLabel("created").singleTime()
                .link("author", "language")
                .enableLabelIndex(true)
                .create();
        schema.edgeLabel("strike").link("person", "person")
                .properties("id", "timestamp", "place", "tool", "reason",
                        "hurt", "arrested")
                .multiTimes().sortKeys("id")
                .nullableKeys("tool", "reason", "hurt")
                .enableLabelIndex(false)
                .ifNotExist().create();
    }

    public static void writeData()
    {
        HugeGraph graph = graph();

        Vertex james = graph.addVertex(T.label, "author", "id", 1,
                "name", "James Gosling", "age", 62,
                "lived", "Canadian");
        Vertex guido = graph.addVertex(T.label, "author", "id", 2,
                "name", "Guido van Rossum", "age", 61,
                "lived", "California");

        Vertex java = graph.addVertex(T.label, "language", "name", "java");
        Vertex python = graph.addVertex(T.label, "language", "name", "python",
                "dynamic", true);

        Vertex java1 = graph.addVertex(T.label, "book", "name", "java-1");
        Vertex java2 = graph.addVertex(T.label, "book", "name", "java-2");
        Vertex java3 = graph.addVertex(T.label, "book", "name", "java-3");

        james.addEdge("created", java);
        guido.addEdge("created", python);

        james.addEdge("authored", java1);
        james.addEdge("authored", java2);
        james.addEdge("authored", java3);
        graph.tx().commit();
    }
}
