import ballerina/io;
import ballerinax/kafka;
import ballerina/lang.value;
import ballerinax/mysql.driver as _;
import ballerinax/mysql;
//import ballerina/sql;
import ballerina/random;
//import ballerina/lang.array;


final mysql:Client dbClient = check new(
    host="172.25.0.6", user="root", password="root", port=3306, database="package_delivery_system"
);

type Package readonly & record {
    string customer_name;
    string contact_number;
    string pickup_location;
    string delivery_location;
    string delivery_type;
    string preferred_times;
};

type Delivery record {
    string delivery_type;
    string delivery_time;
    string delivery_day;
};


configurable string groupId = "standard";
configurable string standard_delivery_requests = "standard-delivery-requests";
configurable string delivery_schedule_response = "delivery-schedule-response";
configurable decimal pollingInterval = 1;
configurable string kafkaEndpoint = "172.25.0.11:9092";

final kafka:ConsumerConfiguration consumerConfigs = {
    groupId: groupId,
    topics: [standard_delivery_requests],
    offsetReset: kafka:OFFSET_RESET_EARLIEST,
    pollingInterval
};

service on new kafka:Listener(kafkaEndpoint, consumerConfigs) {
    private final kafka:Producer packageProducer;
    private final string[] morning_times =  ["9h00", "10h00", "11h00"];
    private final string[] afternoon_times =  ["13h00", "14h00", "15h00"];
    private final string[] evening_times =  ["18h00", "19h00", "20h00"];
    private final string[] days_of_the_week =  ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"];
    private final string[] locations =  ["Katutura", "", "Khomasdal", "Windhoek West"];
    public function init() returns error? {
        self.packageProducer = check new (kafkaEndpoint);
    }

    remote function onConsumerRecord(kafka:AnydataConsumerRecord[] records) returns error? {
         foreach var item in records {
            byte[] byteArray = <byte[]> item.value;
            string jsonString = check string:fromBytes(byteArray);
            json Json = check value:fromJsonString(jsonString);
            Package package = check Json.cloneWithType(Package);

            boolean validation = false;
            self.locations.forEach(function (string location){
                if (location == package.delivery_location){
                    validation = true;
                }
            });
            
            if (validation){
                Delivery delivery = {delivery_day: "", delivery_time: "", delivery_type: package.delivery_type};
                io:println("Creating schedule...");
                int random;
                match package.preferred_times{
                    "Morning" => {
                        random = check random:createIntInRange(0, 3);
                        delivery.delivery_time = self.morning_times[random];
                    }
                }
            }
         }
    }

    isolated function addDelivery(Package pkg) returns error?{
    }
}