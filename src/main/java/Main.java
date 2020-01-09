import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.TransactionWork;

import static org.neo4j.driver.v1.Values.parameters;


public class Main implements AutoCloseable {
    private final Driver driver;

    public Main(String uri, String user, String password) {
        driver = GraphDatabase.driver(uri, AuthTokens.basic(user, password));
    }

    @Override
    public void close() throws Exception {
        driver.close();
    }

    public void getData(final String[] taxIds) {

        try (Session session = driver.session()) {
            String greeting = session.writeTransaction(new TransactionWork<String>() {
                @Override
                public String execute(Transaction tx) {
                    StatementResult result = tx.run(
                            "    MATCH (n:Company {is_head_office:true}) where n.tax_id IN $tax_ids\n" +
                                    "    WITH collect(n) as nodes\n" +
                                    "    UNWIND nodes as n\n" +
                                    "    UNWIND nodes as m\n" +
                                    "    WITH * WHERE id(n) < id(m) AND n.tax_id <> m.tax_id\n" +
                                    "    MATCH path = allShortestPaths((n)" +
                                    "-[:NONPROFIT|SAME_TAX_ID|HAS_ADDRESS|HAS_SURNAME|HAS_CONTACT|DAD|MANAGER|BRANCH" +
                                    "|HAS_DOMAIN|HAS_SURNAME_AND_PATRONYMIC|CHILD|SAME_FULLNAME|SHAREHOLDER" +
                                    "|MANAGEMENT_COMPANY\n" +
                                    "*..4]-(m))\n" +
                                    "    WITH path, nodes(path) as ns\n" +
                                    "    WHERE ALL(node IN ns WHERE (node.is_white_listed=False OR node" +
                                    ".is_white_listed IS NULL))\n" +
                                    "    RETURN\n" +
                                    "        path,\n" +
                                    "        [x in relationships(path) | type(x)] as types,\n" +
                                    "        [n in nodes(path) | labels(n)] as labels,\n" +
                                    "        [r in relationships(path) | properties(r)] as props;",
                            parameters("tax_ids", taxIds));

                    Object object = result.list();

                    return null;
                }
            });


            System.out.println(greeting);
        }
    }

    public static void main(String[] args) throws Exception {
        try (Main main = new Main("bolt://server_name:8072", "neo4j", "password")) {
            main.getData(new String[]{
                    "7730104767",
                    "5042022397",
                    "7736051896",
            });

        } catch (Throwable t) {
            System.out.println(t);
        }

    }
}
