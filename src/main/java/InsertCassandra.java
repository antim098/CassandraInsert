import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.utils.UUIDs;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.sql.Timestamp;
import java.util.UUID;

public class InsertCassandra {
    public static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) {
        String tablename = args[0];
        long count = 0;
        count = Long.parseLong(args[1]);
        CassandraConnector client = new CassandraConnector();
        final int port = 9042;
        client.connect(port);
        Session session = client.getSession();
        System.out.println("============Starting ingestion in table: " + tablename + "===============");

        String json = "{\"archived\":\"0\",\"create_dts\":\"2014-12-17 15:14:40.247\",\"create_uid\":\"ASIA-PACIFIC\\\\Soumyasingh_Shraddha\",\"item_long_name\":\"5Yr ProSupport Flex for Data Center and 4hr Mission Critical, Incident Volume Medium\",\"item_short_name\":\"Restricted-FOR ONBOARDED PS4DC CUSTOMERS ONLY~5Yr PS Flex for Data Center , 4hr MC, IncVol Medium\",\"jobid\":\"1607602101804\",\"language_code\":\"EN\",\"region\":\"GLOBAL\",\"sku_num\":\"TI-1000443202\",\"update_dts\":\"2014-12-17 15:14:40.247\",\"update_uid\":\"ASIA-PACIFIC\\\\Soumyasingh_Shraddha\",\"ds\":\"20200928\",\"ts\":\"16f665d2-3af4-11eb-85d9-91d4c3908712\"}";
        ObjectNode jsonRecord = null;
        try {
            jsonRecord = (ObjectNode) MAPPER.readTree(json);
        } catch (JsonProcessingException e) {
            System.out.println("Error in parsing json");
        }
        long i = 0;
        try {
            Timestamp start_time = new Timestamp(System.currentTimeMillis());
            System.out.println("Started at:" + start_time);
            for (; i < count; i++) {
                UUID timeBasedUuid = UUIDs.timeBased();
                jsonRecord.put("ts", String.valueOf(timeBasedUuid));
                Insert insert_stmt = QueryBuilder.insertInto("test", tablename).json(jsonRecord.toString()).defaultUnset();
                session.execute(insert_stmt);
            }
            Timestamp end_time = new Timestamp(System.currentTimeMillis());
            System.out.println("Inserted records : " + i);
            System.out.println("Ended at:" + end_time);
            System.exit(0);
        } catch (Exception e) {
            System.out.println("Inserted records :" + i);
            System.exit(0);
        }
    }
}
